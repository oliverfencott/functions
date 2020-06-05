'use strict';

function _interopDefault (ex) { return (ex && (typeof ex === 'object') && 'default' in ex) ? ex['default'] : ex; }

var http = _interopDefault(require('http'));
var aws = _interopDefault(require('aws-sdk'));

function send({id, payload}, callback) {
  let port = process.env.PORT || 3333;
  let body = JSON.stringify({id, payload});
  let req = http.request({
    method: 'POST',
    port,
    path: '/__arc',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  });
  req.on('error', callback);
  req.on('close', () => callback());
  req.write(body);
  req.end();
}

function send$1({id, payload}, callback) {
  let endpoint;
  let ARC_WSS_URL = process.env.ARC_WSS_URL;
  if (!ARC_WSS_URL.startsWith('wss://')) {
    // This format of env was only alive for a few weeks, can prob safely retire by mid 2020
    endpoint = `https://${ARC_WSS_URL}/${process.env.NODE_ENV}`;
  } else {
    endpoint = `https://${ARC_WSS_URL.replace('wss://', '')}`;
  }
  let api = new aws.ApiGatewayManagementApi({
    apiVersion: '2018-11-29',
    endpoint,
  });
  api.postToConnection(
    {
      ConnectionId: id,
      Data: JSON.stringify(payload),
    },
    function postToConnection(err) {
      if (err) callback(err);
      else callback();
    }
  );
}

/**
 * arc.ws.send
 *
 * publish web socket events
 *
 * @param {Object} params
 * @param {String} params.id - the ws connecton id (required)
 * @param {String} params.payload - a json event payload (required)
 * @param {Function} callback - a node style errback (optional)
 * @returns {Promise} - returned if no callback is supplied
 */
function send$2({id, payload}, callback) {
  // create a promise if no callback is defined
  let promise;
  if (!callback) {
    promise = new Promise(function (res, rej) {
      callback = function (err, result) {
        err ? rej(err) : res(result);
      };
    });
  }

  let local = process.env.NODE_ENV === 'testing' || process.env.ARC_LOCAL;
  let exec = local ? send : send$1;

  exec(
    {
      id,
      payload,
    },
    callback
  );

  return promise
}

module.exports = send$2;
