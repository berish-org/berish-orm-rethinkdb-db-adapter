import Emitter from '@berish/emitter';
import { BaseDBAdapter, IBaseDBItem, IQueryData, Query } from '@berish/orm';
import tryCall from '@berish/try-call';
import * as r from 'rethinkdb';

export interface IRethinkDBAdapterParams {
  host?: string;
  port?: number;
  dbName?: string;
}

export default class RethinkDBAdapter extends BaseDBAdapter<IRethinkDBAdapterParams> {
  private connection: r.Connection = null;
  private emitter: Emitter<any> = null;

  private tables: string[] = [];
  private tablesInWait: { [tableName: string]: Promise<void> } = {};
  private indexNames: { [tableName: string]: string[] } = {};

  public async initialize(params: IRethinkDBAdapterParams) {
    this.params = params;
    this.emitter = new Emitter();
    this.connection = await r.connect({ db: params.dbName, host: params.host, port: params.port });
    const dbList = await r.dbList().run(this.connection);
    if (!dbList.includes(params.dbName)) {
      await r.dbCreate(params.dbName).run(this.connection);
    }
  }

  public emptyFieldLiteral() {
    return null;
  }

  public async get<T>(query: IQueryData) {
    const items = await this.find<T>(query);
    return items && items[0];
  }

  public async create(tableName: string, items: IBaseDBItem[]) {
    return this.update(tableName, items);
  }

  public async update(tableName: string, items: IBaseDBItem[]) {
    const table = await this.table(tableName);
    await table.insert(items, { conflict: 'update' }).run(this.connection);
  }

  public async index(tableName: string, indexName: string, keys?: string[]) {
    const table = await this.table(tableName);
    const indexTable = this.indexNames[tableName];
    if (indexTable.includes(indexName)) return void 0;
    const indexList = await table.indexList().run(this.connection);
    if (!indexList.includes(indexName)) {
      if (keys && keys.length)
        await table
          .indexCreate(
            indexName,
            keys.map(key => r.row(key)),
          )
          .run(this.connection);
      else await table.indexCreate(indexName).run(this.connection);
      await table.indexWait(indexName).run(this.connection);
    }
    indexTable.push(indexName);
  }

  public async find<T>(query: IQueryData) {
    const table = await this.table(query.className);
    const seq = await this.filter(table, query);
    const cursor = await seq.run(this.connection);
    return cursor.toArray<T>();
  }

  public async delete(query: IQueryData) {
    const table = await this.table(query.className);
    const seq = await this.filter(table, query);
    await seq.delete().run(this.connection);
  }

  public subscribe<T>(query: IQueryData, callback: (oldValue: T, newValue: T) => void) {
    const hash = Query.getHash(query);
    const methodName = 'subscribe';
    const eventName = `${hash}_${methodName}`;
    const eventHash = this.emitter.cacheSubscribe<{ oldValue: T; newValue: T }>(
      eventName,
      async callback => {
        const table = await this.table(query.className);
        const seq = await this.filter(table, query);
        const cursor = await seq.changes({ squash: 1 } as any).run(this.connection);
        cursor.each((err, { old_val: oldValue, new_val: newValue }) => {
          if (!err) callback({ oldValue, newValue });
        });
        return () => {
          return tryCall(() => cursor.close(), { maxAttempts: 5, timeout: 1000, canThrow: true }).catch(() => {
            this.connection.close();
            this.initialize(this.params);
          });
        };
      },
      ({ oldValue, newValue }) => callback(oldValue, newValue),
    );
    return () => this.emitter.off(eventHash);
  }

  private async table(tableName: string): Promise<r.Table> {
    const db = r.db(this.params.dbName);
    if (this.tables.includes(tableName)) return db.table(tableName);
    if (this.tablesInWait[tableName]) {
      await this.tablesInWait[tableName];
      return this.table(tableName);
    }
    let resolvePromise: () => void = null;
    this.tablesInWait[tableName] = new Promise<void>(resolve => (resolvePromise = resolve));
    const tableList = await db.tableList().run(this.connection);
    if (!tableList.includes(tableName)) {
      await db.tableCreate(tableName).run(this.connection);
      await db
        .table(tableName)
        .wait()
        .run(this.connection);
    }
    this.indexNames = Object.assign(this.indexNames, { [tableName]: [] });
    this.tables.push(tableName);

    resolvePromise();
    delete this.tablesInWait[tableName];

    return db.table(tableName);
  }

  private async filter(table: r.Table, query: IQueryData) {
    let seq: r.Sequence = table;
    if (query.ids) seq = this.ids(table, query.ids);
    if (query.limit > 0) seq = this.limit(seq, query.limit);
    if (query.skip > 0) seq = this.skip(seq, query.skip);
    if (query.less) {
      const [key, value] = Object.entries(query.less)[0];
      seq = this.less(seq, key, value);
    }
    if (query.lessOrEqual) {
      const [key, value] = Object.entries(query.lessOrEqual)[0];
      seq = this.lessOrEqual(seq, key, value);
    }
    if (query.greater) {
      const [key, value] = Object.entries(query.greater)[0];
      seq = this.greater(seq, key, value);
    }
    if (query.greaterOrEqual) {
      const [key, value] = Object.entries(query.greaterOrEqual)[0];
      seq = this.greaterOrEqual(seq, key, value);
    }
    if (query.where)
      seq = Object.entries(query.where).reduce((seq, [key, value]) => this.equalTo(seq, key, value), seq);
    if (query.subQueries) {
      seq = await this.subQuery(seq, query.subQueries);
    }
    if (query.contains) {
      seq = Object.entries(query.contains).reduce((seq, [key, value]) => this.contains(seq, key, value), seq);
    }
    return seq;
  }

  private async subQuery(mainSeq: r.Sequence, subQueries: IQueryData['subQueries']) {
    const entries = Object.entries(subQueries);
    const subSeqs = await Promise.all(
      entries.map(async ([key, info]) => {
        const { query, key: keyInQuery } = info;
        const subSeq = await this.table(query.className);
        const subFilter = await this.filter(subSeq, query);
        return {
          className: query.className,
          keys: key && key.split('.'),
          keysInQuery: keyInQuery && keyInQuery.split('.'),
          seq: subFilter,
        };
      }),
    );
    for (const { keys, keysInQuery, className, seq } of subSeqs) {
      if (keysInQuery && keysInQuery.length > 0) {
        mainSeq = mainSeq.filter(row => {
          const plucked = keysInQuery.reduceRight<any>((plucked, key) => {
            if (!plucked) return { [key]: true };
            return { [key]: plucked };
          }, null);
          return seq
            .pluck(plucked)
            .map(subRow => keysInQuery.reduce((subRow, key) => subRow(key), subRow))
            .contains(keys.reduce((row, key) => row(key), row) as any);
        });
      } else {
        mainSeq = mainSeq.filter(row => {
          return seq
            .pluck('id')
            .map(subRow => r.expr(subRow('id'))['add' as any](`:${className}`))
            .contains(keys.reduce((row, key) => row(key), row)('link') as any);
        });
      }
    }
    return mainSeq;
  }

  private equalTo(seq: r.Sequence, key: string, value: any) {
    return seq.filter(row =>
      key
        .split('.')
        .reduce((row, key) => row(key), row)
        .eq(value),
    );
  }

  private contains(seq: r.Sequence, key: string, values: any[]) {
    return seq.filter(row => r.expr(values).contains(key.split('.').reduce((row, key) => row(key), row) as any));
  }

  private limit(seq: r.Sequence, limit: number) {
    return seq.limit(limit);
  }

  private skip(seq: r.Sequence, skip: number) {
    return seq.skip(skip);
  }

  private ids(table: r.Table, ids: string[]) {
    return table.getAll(...ids);
  }

  private less(seq: r.Sequence, key: string, value: any) {
    return seq.filter(row =>
      key
        .split('.')
        .reduce((row, key) => row(key), row)
        .lt(value),
    );
  }

  private lessOrEqual(seq: r.Sequence, key: string, value: any) {
    return seq.filter(row =>
      key
        .split('.')
        .reduce((row, key) => row(key), row)
        .le(value),
    );
  }

  private greater(seq: r.Sequence, key: string, value: any) {
    return seq.filter(row =>
      key
        .split('.')
        .reduce((row, key) => row(key), row)
        .gt(value),
    );
  }

  private greaterOrEqual(seq: r.Sequence, key: string, value: any) {
    return seq.filter(row =>
      key
        .split('.')
        .reduce((row, key) => row(key), row)
        .ge(value),
    );
  }
}
