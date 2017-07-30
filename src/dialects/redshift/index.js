
// PostgreSQL
// -------
import { assign, map, extend } from 'lodash'
import inherits from 'inherits';
import Client from '../../client';
import Promise from 'bluebird';
import { Readable } from 'stream';

import * as path from 'path';

import QueryCompiler from './query/compiler';
import ColumnCompiler from './schema/columncompiler';
import TableCompiler from './schema/tablecompiler';
import SchemaCompiler from './schema/compiler';
import {makeEscape} from '../../query/string'

function Client_Redshift(config) {
  Client.apply(this, arguments);
  if (config.returning) {
    this.defaultReturning = config.returning;
  }

  if (config.searchPath) {
    this.searchPath = config.searchPath;
  }

  if (config.version) {
    this.version = config.version;
  }
}
inherits(Client_Redshift, Client);

assign(Client_Redshift.prototype, {

  queryCompiler() {
    return new QueryCompiler(this, ...arguments)
  },

  columnCompiler() {
    return new ColumnCompiler(this, ...arguments)
  },

  schemaCompiler() {
    return new SchemaCompiler(this, ...arguments)
  },

  tableCompiler() {
    return new TableCompiler(this, ...arguments)
  },

  dialect: 'redshift',

  driverName: 'rs',

  _driver() {
    let jinst = require('jdbc/lib/jinst');
    if (!jinst.isJvmCreated()) {
      jinst.addOption("-Xrs");
      jinst.setupClasspath([
        path.join(__dirname, 'drivers/RedshiftJDBC42-1.2.1.1001.jar')
      ]);
    }
    let JDBC = require('jdbc');
    let settings = this.connectionSettings;
    let config = {
      url: `jdbc:redshift://${settings.endpoint}:${settings.port}/${settings.database}`,
      drivername: 'com.amazon.redshift.jdbc42.Driver',
      minpoolsize: settings.minpoolsize || 10,
      maxpoolsize: settings.maxpoolsize || 100,
      properties: {
        user: settings.user,
        password: settings.password,
        currentSchema: this.config.searchPath
      }
    };
    return new JDBC(config);
  },

  _escapeBinding: makeEscape({
    escapeArray(val, esc) {
      return esc(arrayString(val, esc))
    },
    escapeString(str) {
      let hasBackslash = false
      let escaped = '\''
      for (let i = 0; i < str.length; i++) {
        const c = str[i]
        if (c === '\'') {
          escaped += c + c
        } else if (c === '\\') {
          escaped += c + c
          hasBackslash = true
        } else {
          escaped += c
        }
      }
      escaped += '\''
      if (hasBackslash === true) {
        escaped = 'E' + escaped
      }
      return escaped
    },
    escapeObject(val, prepareValue, timezone, seen = []) {
      if (val && typeof val.toPostgres === 'function') {
        seen = seen || [];
        if (seen.indexOf(val) !== -1) {
          throw new Error(`circular reference detected while preparing "${val}" for query`);
        }
        seen.push(val);
        return prepareValue(val.toPostgres(prepareValue), seen);
      }
      return JSON.stringify(val);
    }
  }),

  wrapIdentifier(value) {
    if (value === '*') return value;
    const matched = value.match(/(.*?)(\[[0-9]\])/);
    if (matched) return this.wrapIdentifier(matched[1]) + matched[2];
    return `"${value.replace(/"/g, '""')}"`;
  },

  // Get a raw connection, called by the `pool` whenever a new
  // connection needs to be added to the pool.
  acquireRawConnection() {
    const client = this;
    return new Promise(function(resolver, rejecter) {
      client.driver.initialize(function(err) {
        if (err) {
          return rejecter(err);
        }
        client.driver.reserve(function(err, connObj) {
          if (err) {
            return rejecter(err);
          } else if (connObj) {
            var connection = connObj.conn;

            if (!client.version) {
              return client.checkVersion(connObj).then(function(version) {
                client.version = version;
                resolver(connObj);
              });
            }
            resolver(connObj);
          }
        });
      });
    })
    .tap(function setSearchPath(connObj) {
      return client.setSchemaSearchPath(connObj);
    });
  },

  // Used to explicitly close a connection, called internally by the pool
  // when a connection times out or the pool is shutdown.
  destroyRawConnection(connObj) {
    const client = this;
    client.driver.release(connObj, function(err) {});
  },

  // In PostgreSQL, we need to do a version check to do some feature
  // checking on the database.
  checkVersion(connObj) {
    var connection = connObj.conn;
    return new Promise(function(resolver, rejecter) {
      connection.createStatement(function(err, statement) {
        if (err) {
          rejecter(err);
        } else {
          let query = "SELECT VERSION();";
          statement.executeQuery(query, function(err, resultset) {
            if (err) {
              rejecter(err);
            } else {
              resultset.toObjArray(function(err, results) {
                if (err) {
                  rejecter(err);
                } else {
                  resolver(/^PostgreSQL (.*?)( |$)/.exec(results[0].version)[1]);
                }
              });
            }
          });
        }
      });
    });
  },

  // Position the bindings for the query. The escape sequence for question mark
  // is \? (e.g. knex.raw("\\?") since javascript requires '\' to be escaped too...)
  positionBindings(sql) {
    let questionCount = 0;
    return sql.replace(/(\\*)(\?)/g, function (match, escapes) {
      if (escapes.length % 2) {
        return '?';
      } else {
        questionCount++;
        return `$${questionCount}`;
      }
    });
  },

  setSchemaSearchPath(connObj, searchPath) {
    const path = (searchPath || this.searchPath);

    if (!path) return Promise.resolve(true);

    var connection = connObj.conn;
    return new Promise(function(resolver, rejecter) {
      connection.createStatement(function(err, statement) {
        if (err) {
          rejecter(err);
        } else {
          let query = `set search_path to ${path};`
          statement.executeUpdate(query, function(err) {
            if (err) {
              rejecter(err);
            } else resolver(true);
          });
        }
      });
    });
  },

  _stream(connObj, obj, stream, options) {
    let self = this;
    let sql = obj.sql = this.positionBindings(obj.sql);
    let batchSize = options.batchSize || 1000;
    var connection = connObj.conn;
    return new Promise(function(resolver, rejecter) {
      connection.prepareStatement(sql, function(err, statement) {
        if (err) {
          rejecter(err);
        } else {
          statement.setFetchSize(batchSize, function(err) {
            if (err) {
              rejecter(err);
            } else {
              self.createBindings(obj, statement, function(err) {
                if (err) {
                  rejecter(err);
                } else {
                  statement.executeQuery(function(err, resultset) {
                    if (err) {
                      rejecter(err);
                    } else {
                      resultset.toObjectIter(function(err, iterator) {
                        if (err) {
                          rejecter(err);
                        } else {
                          let rows = iterator.rows;
                          let rs = new Readable({
                            objectMode: true,
                            highWaterMark: options.highWaterMark || 16,
                            read( size ) {
                              if ( this._busy ) return;
                              this._busy = true;
                              let readNext = function () {
                                  let next = rows.next();
                                  setImmediate(() => {
                                    let writable = rs.push(next.value);
                                    if ( writable && ! next.done ) setImmediate(readNext);
                                    else this._busy = false;
                                  })
                                  if ( next.done ) setImmediate(() => { rs.push(null);  })
                              }
                              readNext();
                            }
                          });
                          rs._busy = false;
                          rs.pipe(stream);
                          resolver(rs);
                        }
                      });
                    }
                  });
                }
              });
            }
          });
        }
      });
    });
  },

  // Runs the query on the specified connection, providing the bindings
  // and any other necessary prep work.
  _query(connObj, obj) {
    let self = this;
    let sql = obj.sql = this.positionBindings(obj.sql);
    var connection = connObj.conn;
    return new Promise(function(resolver, rejecter) {
      connection.prepareStatement(sql, function(err, statement) {
        if (err) {
          rejecter(err);
        } else {
          self.createBindings(obj, statement, function(err) {
            if (err) {
              rejecter(err);
            } else {
              statement.executeQuery(function(err, resultset) {
                if (err) {
                  rejecter(err);
                } else {
                  resultset.toObjArray(function(err, results) {
                    if (err) {
                      rejecter(err);
                    } else {
                      obj.response = results;
                      resolver(obj);
                    }
                  });
                }
              });
            }
          });
        }
      });
    });
  },

  createBindings(obj, statement, callback) {
    if ( obj && obj.bindings && obj.bindings.length > 0 ) {
      let pending = 0, done = false;
      obj.bindings.forEach((value, index) => {
        let method;
        switch(typeof value) {
          case 'string':
            method = statement.setString.bind(statement);
          break;

          case 'number':
            if ( value % 1 === 0 ) method = statement.setLong.bind(statement);
            if ( value % 1 !== 0 ) method = statement.setDecimal.bind(statement);
          break;

         case 'boolean':
           method = statement.setBoolean.bind(statement);
         break;

         case 'object':
           if ( value instanceof Date ) method = statement.setTimestamp.bind(statement);
           if ( value instanceof Date ) method = statement.setTimestamp.bind(statement);
         break;
        }
        if ( method ) {
          pending++;
          method(index + 1, value, function(err) {
            pending--;
            if ( err && ! done ) { callback(err); }
            else if ( ! pending && ! done ) callback();
          });
        } else {
          throw new Error('Binding cannot be matched to an allowed type (string, number, boolean, or Date)');
        }
      });
    } else callback();
  },

  // Ensures the response is returned in the same format as other clients.
  processResponse(obj, runner) {
    const resp = obj.response;
    if (obj.output) return obj.output.call(runner, resp);
    if (obj.method === 'raw') return resp;
    const { returning } = obj;
    if (resp.command === 'SELECT') {
      if (obj.method === 'first') return resp.rows[0];
      if (obj.method === 'pluck') return map(resp.rows, obj.pluck);
      return resp.rows;
    }
    if (returning) {
      const returns = [];
      for (let i = 0, l = resp.rows.length; i < l; i++) {
        const row = resp.rows[i];
        if (returning === '*' || Array.isArray(returning)) {
          returns[i] = row;
        } else {
          returns[i] = row[returning];
        }
      }
      return returns;
    }
    if (resp.command === 'UPDATE' || resp.command === 'DELETE') {
      return resp.rowCount;
    }
    return resp;
  }

})

function arrayString(arr, esc) {
  let result = '{'
  for (let i = 0 ; i < arr.length; i++) {
    if (i > 0) result += ','
    const val = arr[i]
    if (val === null || typeof val === 'undefined') {
      result += 'NULL'
    } else if (Array.isArray(val)) {
      result += arrayString(val, esc)
    } else if (typeof val === 'number') {
      result += val
    } else {
      result += JSON.stringify(typeof val === 'string' ? val : esc(val))
    }
  }
  return result + '}'
}

export default Client_Redshift
