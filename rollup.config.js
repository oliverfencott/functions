import json from '@rollup/plugin-json'
import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'

const dirs = ['ws', 'static', 'tables', 'queues']

export default dirs.map((dir) => ({
  input: `src/${dir}/index.js`,
  output: {
    file: `${dir}.js`,
    format: 'cjs',
  },

  external: ['aws-sdk', 'http'],
  plugins: [
    resolve({
      preferBuiltins: true,
    }),
    commonjs(),
    json(),
  ],
}))
