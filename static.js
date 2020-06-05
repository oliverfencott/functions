'use strict';

var fs = require('fs');
var path = require('path');

/**
 * Architect static asset helper
 * - Returns the live asset path and filename
 *
 * In order to keep this method sync, it does not use reflection to get fingerprint status
 * - Not checking @static fingerprint true (which we used to read from the .arc file) is possibly dangerous, so ensure asset path is valid
 * - ? TODO: add fingerprint state to env vars in Arc 6 to restore config safety?
 * @param {string} path - the path to the asset (eg. /index.js)
 * @returns {string} path - the resolved asset path (eg. /_static/index-xxx.js)
 */
function _static(asset, options = {}) {
  let key = asset[0] === '/' ? asset.substring(1) : asset;
  let isIndex = asset === '/';
  let manifest = path.join(process.cwd(), 'node_modules', '@architect', 'shared', 'static.json');
  let exists = fs.existsSync(manifest);
  let local = process.env.NODE_ENV === 'testing' || process.env.ARC_LOCAL;
  let stagePath = options.stagePath && !local ? '/' + process.env.NODE_ENV : '';
  let path$1 = `${stagePath}/_static`;
  if (!local && exists && !isIndex) {
    let read = (p) => fs.readFileSync(p).toString();
    let pkg = JSON.parse(read(manifest));
    let asset = pkg[key];
    if (!asset) throw ReferenceError(`Could not find asset in static.json (asset fingerprint manifest): ${key}`)
    return `${path$1}/${asset}`
  }
  return `${path$1}/${isIndex ? '' : key}`
}

module.exports = _static;
