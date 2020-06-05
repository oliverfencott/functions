import waterfall from 'run-waterfall'
import {tables as lookupTables} from '../discovery'
import factory from './factory'
import sandbox from './sandbox'

// cheap client cache
let client = false

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
  let promise
  if (!callback) {
    promise = new Promise(function ugh(res, rej) {
      callback = function errback(err, result) {
        if (err) rej(err)
        else res(result)
      }
    })
  }
  /**
   * Read Architect manifest if local / sandbox, otherwise use service reflection
   */
  let runningLocally = process.env.NODE_ENV === 'testing'
  if (runningLocally) {
    sandbox(callback)
  } else if (client) {
    callback(null, client)
  } else {
    waterfall(
      [
        lookupTables,
        factory,
        function (created, callback) {
          client = created
          callback(null, client)
        },
      ],
      callback
    )
  }
  return promise
}

// Legacy compat methods
export {insert, modify, update, remove, destroy, all, save, change} from './old'

// Export directly for fast use
export {directDoc as doc, directDb as db} from './dynamo'

export {tables}
