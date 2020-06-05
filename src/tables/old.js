import parallel from 'run-parallel'
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
        var isInvoking = types.indexOf(record.eventName) > -1
        if (isInvoking) {
          handler(record, callback)
        } else {
          callback() // if not we just call the continuation (callback)
        }
      }
    })
    // executes the handlers in parallel
    parallel(handlers, function __processedRecords(err, results) {
      if (err) {
        ctx.fail(err)
      } else {
        ctx.succeed(results)
      }
    })
  }
}

const insert = __trigger.bind({}, ['INSERT'])
const modify = __trigger.bind({}, ['MODIFY'])
const update = __trigger.bind({}, ['MODIFY'])
const remove = __trigger.bind({}, ['REMOVE'])
const destroy = __trigger.bind({}, ['REMOVE'])
const all = __trigger.bind({}, ['INSERT', 'MODIFY', 'REMOVE'])
const save = __trigger.bind({}, ['INSERT', 'MODIFY'])
const change = __trigger.bind({}, ['INSERT', 'REMOVE'])

export {insert, modify, update, remove, destroy, all, save, change}
