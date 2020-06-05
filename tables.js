'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var aws = _interopDefault(require('aws-sdk'));
var https = _interopDefault(require('https'));

var runWaterfall_1 = runWaterfall;

function runWaterfall (tasks, cb) {
  var current = 0;
  var isSync = true;

  function done (err, args) {
    function end () {
      args = args ? [].concat(err, args) : [ err ];
      if (cb) cb.apply(undefined, args);
    }
    if (isSync) process.nextTick(end);
    else end();
  }

  function each (err) {
    var args = Array.prototype.slice.call(arguments, 1);
    if (++current >= tasks.length || err) {
      done(err, args);
    } else {
      tasks[current].apply(undefined, [].concat(args, each));
    }
  }

  if (tasks.length) {
    tasks[0](each);
  } else {
    done(null);
  }

  isSync = false;
}

/**
 * @param {string} type - events, queues, or tables
 * @returns {object} {name: value}
 */
function lookup(type, callback) {

  let Path = `/${process.env.ARC_CLOUDFORMATION}`;
  let Recursive = true;
  let values = [];

  function getParams(params) {
    let isType = p=> p.Name.split('/')[2] === type;
    let ssm = new aws.SSM;
    ssm.getParametersByPath(params, function done(err, result) {
      if (err) {
        callback(err);
      }
      else if (result.NextToken) {
        values = values.concat(result.Parameters.filter(isType));
        getParams({Path, Recursive, NextToken: result.NextToken});
      }
      else {
        values = values.concat(result.Parameters.filter(isType));
        callback(null, values.reduce((a, b)=> {
          a[b.Name.split('/')[3]] = b.Value;
          return a
        }, {}));
      }
    });
  }

  getParams({Path, Recursive});
}

var discovery = {
  events: lookup.bind({}, 'events'),
  queues: lookup.bind({}, 'queues'),
  tables: lookup.bind({}, 'tables')
};

/**
 * Instantiates Dynamo service interfaces
 * - Internal APIs should use `db` + `doc` to instantiate DynamoDB interfaces
 * - Avoid using `direct.db` + `direct.doc`: as it's an issue vector for using Functions in certain test harnesses!
 */
function getDynamo(type, callback) {
  if (!type) throw ReferenceError('Must supply Dynamo service interface type')

  let testing = process.env.NODE_ENV === 'testing';
  let arcLocal = process.env.ARC_LOCAL;
  let port = process.env.ARC_TABLES_PORT || 5000;
  let local = {
    endpoint: new aws.Endpoint(`http://localhost:${port}`),
    region: process.env.AWS_REGION || 'us-west-2', // Do not assume region is set!
  };
  let DB = aws.DynamoDB;
  let Doc = aws.DynamoDB.DocumentClient;
  let dynamo; // Assigned below

  /**
   * This module may be loaded by @arc/arc via repl
   * - The `direct` interfaces will instantiate before NODE_ENV is set
   * - Thus, unlike most other scenarios, don't assume the presence of NODE_ENV
   * - Also: some test harnesses (ahem) will automatically populate NODE_ENV with their own values, unbidden
   * - *Why this matters*: using https.Agent (and not http.Agent) will stall the Sandbox
   */
  if (!testing && !arcLocal) {
    let agent = new https.Agent({
      keepAlive: true,
      maxSockets: 50, // Node can set to Infinity; AWS maxes at 50; check back on this every once in a while
      rejectUnauthorized: true,
    });
    aws.config.update({
      httpOptions: {agent},
    });
    // TODO? migrate to using `AWS_NODEJS_CONNECTION_REUSE_ENABLED`?
  }

  if (type === 'db') {
    dynamo = testing ? new DB(local) : new DB();
  }

  if (type === 'doc') {
    dynamo = testing ? new Doc(local) : new Doc();
  }

  if (type === 'session') {
    // if SESSION_TABLE_NAME isn't defined we mock the client and just pass session thru
    let passthru = !process.env.SESSION_TABLE_NAME;
    let mock = {
      get(params, callback) {
        callback();
      },
      put(params, callback) {
        callback();
      },
    };
    dynamo = testing ? new Doc(local) : passthru ? mock : new Doc();
  }

  if (!callback) return dynamo
  else callback(null, dynamo);
}

const db = getDynamo.bind({}, 'db');
const doc = getDynamo.bind({}, 'doc');
const session = getDynamo.bind({}, 'session');
const directDb = getDynamo('db');
const directDoc = getDynamo('doc');

// accepts an object and promisifies all keys
function pfy(obj) {
  var copy = {};
  Object.keys(obj).forEach((k) => {
    copy[k] = promised(obj[k]);
  });
  return copy
}

// accepts an errback style fn and returns a promisified fn
function promised(fn) {
  return function _promisified(params, callback) {
    if (!callback) {
      return new Promise(function (res, rej) {
        fn(params, function (err, result) {
          err ? rej(err) : res(result);
        });
      })
    } else {
      fn(params, callback);
    }
  }
}

var runParallel_1 = runParallel;

function runParallel (tasks, cb) {
  var results, pending, keys;
  var isSync = true;

  if (Array.isArray(tasks)) {
    results = [];
    pending = tasks.length;
  } else {
    keys = Object.keys(tasks);
    results = {};
    pending = keys.length;
  }

  function done (err) {
    function end () {
      if (cb) cb(err, results);
      cb = null;
    }
    if (isSync) process.nextTick(end);
    else end();
  }

  function each (i, err, result) {
    results[i] = result;
    if (--pending === 0 || err) {
      done(err);
    }
  }

  if (!pending) {
    // empty
    done(null);
  } else if (keys) {
    // object
    keys.forEach(function (key) {
      tasks[key](function (err, result) { each(key, err, result); });
    });
  } else {
    // array
    tasks.forEach(function (task, i) {
      task(function (err, result) { each(i, err, result); });
    });
  }

  isSync = false;
}

/**
 * returns a data client
 */
function reflectFactory(tables, callback) {
  runParallel_1({db, doc}, function done(err, {db, doc}) {
    if (err) throw err
    else {
      let data = Object.keys(tables).reduce((client, tablename) => {
        client[tablename] = factory(tables[tablename]);
        return client
      }, {});

      Object.defineProperty(data, '_db', {
        enumerable: false,
        value: db,
      });

      Object.defineProperty(data, '_doc', {
        enumerable: false,
        value: doc,
      });

      data.reflect = async function reflect() {
        return tables
      };

      data._name = function _name(name) {
        return tables[name]
      };

      function factory(TableName) {
        return pfy({
          delete(key, callback) {
            let params = {};
            params.TableName = TableName;
            params.Key = key;
            doc.delete(params, callback);
          },
          get(key, callback) {
            let params = {};
            params.TableName = TableName;
            params.Key = key;
            doc.get(params, function _get(err, result) {
              if (err) callback(err);
              else callback(null, result.Item);
            });
          },
          put(item, callback) {
            let params = {};
            params.TableName = TableName;
            params.Item = item;
            doc.put(params, function _put(err) {
              if (err) callback(err);
              else callback(null, item);
            });
          },
          query(params, callback) {
            params.TableName = TableName;
            doc.query(params, callback);
          },
          scan(params, callback) {
            params.TableName = TableName;
            doc.scan(params, callback);
          },
          update(params, callback) {
            params.TableName = TableName;
            doc.update(params, callback);
          },
        })
      }

      callback(null, data);
    }
  });
}

/**
 * returns a data client
 */
function sandbox(callback) {
  runParallel_1([db, doc], function _done(err, results) {
    if (err) callback(err);
    else {
      let db = results[0];
      let doc = results[1];
      db.listTables({}, function listed(err, result) {
        if (err) callback(err);
        else {
          let reduce = (a, b) => Object.assign({}, a, b);
          let dontcare = (tbl) => tbl != 'arc-sessions' && tbl.includes('production') === false;
          let tables = result.TableNames.filter(dontcare);
          let data = tables
            .map(function fmt(tbl) {
              let parts = tbl.split('-staging-');
              let app = parts.shift();
              let name = parts.join('');
              return client(app)(name)
            })
            .reduce(reduce, {});

          Object.defineProperty(data, '_db', {
            enumerable: false,
            value: db,
          });

          Object.defineProperty(data, '_doc', {
            enumerable: false,
            value: doc,
          });

          data.reflect = async function reflect() {
            return tables.reduce(function visit(result, tbl) {
              let parts = tbl.split('-staging-');
              let app = parts.shift();
              let name = parts.join('');
              result[name] = `${app}-staging-${name}`;
              return result
            }, {})
          };

          data._name = function _name(name) {
            return tables.filter((t) => RegExp(`^.*${name}$`).test(t))
          };

          callback(null, data);
        }
      });

      function client(appname) {
        return function (tablename) {
          let name = (nom) => `${appname}-staging-${nom}`;
          let TableName = name(tablename);
          let client = {
            delete(key, callback) {
              let params = {};
              params.TableName = TableName;
              params.Key = key;
              doc.delete(params, callback);
            },
            get(key, callback) {
              let params = {};
              params.TableName = TableName;
              params.Key = key;
              doc.get(params, function _get(err, result) {
                if (err) callback(err);
                else callback(null, result.Item);
              });
            },
            put(item, callback) {
              let params = {};
              params.TableName = TableName;
              params.Item = item;
              doc.put(params, function _put(err) {
                if (err) callback(err);
                else callback(null, item);
              });
            },
            query(params, callback) {
              params.TableName = TableName;
              doc.query(params, callback);
            },
            scan(params, callback) {
              params.TableName = TableName;
              doc.scan(params, callback);
            },
            update(params, callback) {
              params.TableName = TableName;
              doc.update(params, callback);
            },
          };
          let result = {};
          result[tablename] = pfy(client);
          return result
        }
      }
    }
  });
}

/**
 * var trigger = require('aws-dynamodb-lambda-trigger/lambda')
 *
 * function onInsert(record, callback) {
 *   console.log(record)
 *   callback(null, record) // errback style; results passed to context.succeed
 * }
 *
 * module.exports = trigger.insert(onInsert)
 */

function __trigger(types, handler) {
  return function __lambdaSignature(evt, ctx) {
    // dynamo triggers send batches of records so we're going to create a handler for each one
    var handlers = evt.Records.map(function (record) {
      // for each record we construct a handler function
      return function __actualHandler(callback) {
        // if isInvoking we invoke the handler with the record
        var isInvoking = types.indexOf(record.eventName) > -1;
        if (isInvoking) {
          handler(record, callback);
        } else {
          callback(); // if not we just call the continuation (callback)
        }
      }
    });
    // executes the handlers in parallel
    runParallel_1(handlers, function __processedRecords(err, results) {
      if (err) {
        ctx.fail(err);
      } else {
        ctx.succeed(results);
      }
    });
  }
}

const insert = __trigger.bind({}, ['INSERT']);
const modify = __trigger.bind({}, ['MODIFY']);
const update = __trigger.bind({}, ['MODIFY']);
const remove = __trigger.bind({}, ['REMOVE']);
const destroy = __trigger.bind({}, ['REMOVE']);
const all = __trigger.bind({}, ['INSERT', 'MODIFY', 'REMOVE']);
const save = __trigger.bind({}, ['INSERT', 'MODIFY']);
const change = __trigger.bind({}, ['INSERT', 'REMOVE']);

// cheap client cache
let client = false;

/**
 * // example usage:
 * let arc = require('architect/functions')
 *
 * exports.handler = async function http(req) {
 *  let data = await arc.tables()
 *  await data.tacos.put({taco: 'pollo'})
 *  return {statusCode: 200}
 * }
 */
function tables(callback) {
  let promise;
  if (!callback) {
    promise = new Promise(function ugh(res, rej) {
      callback = function errback(err, result) {
        if (err) rej(err);
        else res(result);
      };
    });
  }
  /**
   * Read Architect manifest if local / sandbox, otherwise use service reflection
   */
  let runningLocally = process.env.NODE_ENV === 'testing';
  if (runningLocally) {
    sandbox(callback);
  } else if (client) {
    callback(null, client);
  } else {
    runWaterfall_1(
      [
        discovery.tables,
        reflectFactory,
        function (created, callback) {
          client = created;
          callback(null, client);
        },
      ],
      callback
    );
  }
  return promise
}

exports.all = all;
exports.change = change;
exports.db = directDb;
exports.destroy = destroy;
exports.doc = directDoc;
exports.insert = insert;
exports.modify = modify;
exports.remove = remove;
exports.save = save;
exports.tables = tables;
exports.update = update;
