import { CacheEmitter } from '@berish/emitter';
import LINQ from '@berish/linq';

import * as r from 'rethinkdb';

import { BaseDBAdapter, IBaseDBItem, Query, QueryData, QueryDataSchema } from '@berish/orm';

export interface IRethinkDBAdapterParams {
  host?: string;
  port?: number;
  dbName?: string;
}

export default class RethinkDBAdapter extends BaseDBAdapter<IRethinkDBAdapterParams> {
  private connection: r.Connection = null;
  private _cacheEmitter = new CacheEmitter();

  private tables: string[] = [];
  private indexNames: { [tableName: string]: string[] } = {};
  private subscribeCursors: r.Cursor[] = [];
  private _subscribeEventHashes: string[] = [];

  public async initialize(params: IRethinkDBAdapterParams) {
    this.params = params;
    this.connection = await r.connect({ db: params.dbName, host: params.host, port: params.port });
    const dbList = await r.dbList().run(this.connection);
    if (!dbList.includes(params.dbName)) {
      await r.dbCreate(params.dbName).run(this.connection);
    }
  }

  public async close() {
    for (const hash of this._subscribeEventHashes) {
      this._cacheEmitter.unsubscribe(hash);
    }

    if (this.connection && this.connection.open) {
      await this.connection.close();
    }
    this.params = null;
    this.connection = null;

    this.tables = [];
    this.indexNames = {};
    this.subscribeCursors = [];
  }

  public async reconnect() {
    const params = this.params;

    await this.close();
    await this.initialize(params);
  }

  public async count(queryData: QueryData<QueryDataSchema>) {
    const { value: className } = LINQ.from(queryData).last(m => m.key === 'className');

    const table = await this.table(className);
    const seq = await this.filter(table, queryData);
    const result = await seq.count().run(this.connection);
    return result;
  }

  public async get<T>(query: QueryData<QueryDataSchema>) {
    const items = await this.find<T>(query);
    return items && items[0];
  }

  public async create(tableName: string, items: IBaseDBItem[]) {
    return this.update(tableName, items);
  }

  public async update(tableName: string, items: IBaseDBItem[]) {
    const table = await this.table(tableName);

    items.forEach(item => (item.createdAt = item.updatedAt = +new Date()));

    await table
      .insert(items, {
        conflict: (id, oldDoc, newDoc) => oldDoc.merge(newDoc).merge({ createdAt: oldDoc('createdAt') }),
      })
      .run(this.connection);
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

  public async find<T>(queryData: QueryData<QueryDataSchema>) {
    const { value: className } = LINQ.from(queryData).last(m => m.key === 'className');

    const table = await this.table(className);
    const seq = await this.filter(table, queryData);
    const cursor = await seq.run(this.connection);
    return cursor.toArray<T>();
  }

  public async delete(queryData: QueryData<QueryDataSchema>) {
    const { value: className } = LINQ.from(queryData).last(m => m.key === 'className');

    const table = await this.table(className);
    const seq = await this.filter(table, queryData);
    await seq.delete().run(this.connection);
  }

  public subscribe<T>(
    query: QueryData<QueryDataSchema>,
    callback: (oldValue: T, newValue: T) => void,
    onError: (reason: any) => any,
  ) {
    const queryHash = Query.getHash(query);

    const eventHash = this._cacheEmitter.subscribe<{ oldValue: T; newValue: T }>(
      `subscribe_${queryHash}`,
      async callback => {
        const { value: className } = LINQ.from(query).last(m => m.key === 'className');

        const table = await this.table(className);
        const seq = await this.filter(table, query);
        const cursor = await seq.changes({ squash: 1 } as any).run(this.connection);

        cursor.each((err, data) => {
          if (err) return onError && onError(err);
          const { old_val: oldValue, new_val: newValue } = data;
          callback({ oldValue, newValue });
        });

        this.subscribeCursors.push(cursor);

        // return cursor;
        return () => {
          this.closeCursor(cursor, () => this.reconnect());
        };
      },
      ({ oldValue, newValue }) => callback(oldValue, newValue),
    );

    this._subscribeEventHashes.push(eventHash);

    return () => {
      this._subscribeEventHashes = this._subscribeEventHashes.filter(m => m !== eventHash);
      this._cacheEmitter.unsubscribe(eventHash);
    };
  }

  private closeCursor(cursor: r.Cursor, onError?: (err: any) => any) {
    if (cursor) {
      cursor.close(onError);
      this.subscribeCursors = this.subscribeCursors.filter(m => m !== cursor);
    }
  }

  private table(tableName: string): Promise<r.Table> {
    const _table = async (tableName: string) => {
      const db = r.db(this.params.dbName);

      if (this.tables.includes(tableName)) {
        const tableList = await db.tableList().run(this.connection);

        if (tableList.includes(tableName)) return db.table(tableName);
        this.tables.splice(this.tables.indexOf(tableName), 1);

        return _table(tableName);
      }

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

      return db.table(tableName);
    };

    return this._cacheEmitter.call(tableName, () => {
      return _table(tableName);
    });
  }

  private async filter(table: r.Table, queryData: QueryData<QueryDataSchema>) {
    return queryData.reduce<Promise<r.Sequence>>(async (seqPromise, { key, value }) => {
      const seq = await seqPromise;
      if (key === 'ids') return this.ids(table, value);
      if (key === 'limit') return this.limit(seq, value);
      if (key === 'skip') return this.skip(seq, value);
      if (key === 'less') return this.less(seq, value);
      if (key === 'lessOrEqual') return this.lessOrEqual(seq, value);
      if (key === 'greater') return this.greater(seq, value);
      if (key === 'greaterOrEqual') return this.greaterOrEqual(seq, value);
      if (key === 'where') return this.where(seq, value);
      if (key === 'subQueries') return this.subQuery(seq, value);
      if (key === 'contains') return this.contains(seq, value);
      if (key === 'pluck') return this.pluck(seq, value);

      return seq;
    }, Promise.resolve(table as r.Table));
  }

  private async subQuery(mainSeq: r.Sequence, subQueries: QueryDataSchema['subQueries']) {
    const entries = Object.entries(subQueries);
    const subSeqs = await Promise.all(
      entries.map(async ([key, info]) => {
        const { query, key: keyInQuery } = info;

        const { value: className } = LINQ.from(query).last(m => m.key === 'className');
        const subSeq = await this.table(className);
        const subFilter = await this.filter(subSeq, query);

        return {
          className,
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

  private where(seq: r.Sequence, value: QueryDataSchema['where']) {
    return Object.entries(value).reduce(
      (seq, [key, value]) =>
        seq.filter(row =>
          key
            .split('.')
            .reduce((row, key) => row(key), row)
            .eq(value),
        ),
      seq,
    );
  }

  private contains(seq: r.Sequence, value: QueryDataSchema['contains']) {
    return Object.entries(value).reduce(
      (seq, [key, values]) =>
        seq.filter(row => r.expr(values).contains(key.split('.').reduce((row, key) => row(key), row) as any)),
      seq,
    );
  }

  private limit(seq: r.Sequence, limit: QueryDataSchema['limit']) {
    return seq.limit(limit);
  }

  private skip(seq: r.Sequence, skip: QueryDataSchema['skip']) {
    return seq.skip(skip);
  }

  private ids(table: r.Table, ids: QueryDataSchema['ids']) {
    return table.getAll(...ids);
  }

  private less(seq: r.Sequence, key: QueryDataSchema['less']) {
    return Object.entries(key).reduce(
      (seq, [key, value]) =>
        seq.filter(row =>
          key
            .split('.')
            .reduce((row, key) => row(key), row)
            .lt(value),
        ),
      seq,
    );
  }

  private lessOrEqual(seq: r.Sequence, key: QueryDataSchema['lessOrEqual']) {
    return Object.entries(key).reduce(
      (seq, [key, value]) =>
        seq.filter(row =>
          key
            .split('.')
            .reduce((row, key) => row(key), row)
            .le(value),
        ),
      seq,
    );
  }

  private greater(seq: r.Sequence, key: QueryDataSchema['greater']) {
    return Object.entries(key).reduce(
      (seq, [key, value]) =>
        seq.filter(row =>
          key
            .split('.')
            .reduce((row, key) => row(key), row)
            .gt(value),
        ),
      seq,
    );
  }

  private greaterOrEqual(seq: r.Sequence, key: QueryDataSchema['greaterOrEqual']) {
    return Object.entries(key).reduce(
      (seq, [key, value]) =>
        seq.filter(row =>
          key
            .split('.')
            .reduce((row, key) => row(key), row)
            .ge(value),
        ),
      seq,
    );
  }

  private pluck(seq: r.Sequence, keys: QueryDataSchema['pluck']) {
    const pluckedObj = keys.reduce((pluckedObj, key) => {
      const words = key.split('.');
      const wordsCount = words.length;
      words.reduce((out, word, index) => {
        if (index === wordsCount - 1) {
          out[word] = true;
          return out;
        } else {
          out[word] = out[word] || {};
          return out[word];
        }
      }, pluckedObj);
      return pluckedObj;
    }, {} as any);

    return seq.pluck(pluckedObj);
  }
}
