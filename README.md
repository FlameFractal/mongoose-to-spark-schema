# mongoose-to-spark-schema

> Generate Spark StructType schema JSON from a Mongoose model

[![NPM version](https://badge.fury.io/js/mongoose-to-spark-schema.svg)](http://badge.fury.io/js/mongoose-to-spark-schema)

[![npm](https://nodei.co/npm/mongoose-to-spark-schema.png)](https://www.npmjs.com/package/mongoose-to-spark-schema)

## Install

```
$ git clone
$ cd mongoose-to-spark-schema
$ npm install
```

## Usage

```bash
// output to stdout
node index.js --model=./path-to-mongoose-model.js

// output to file
node index.js --model=./path-to-mongoose-model.js --output=./path-to-output-schema.json
```

### Caveats:
- Remove all dependencies from mongoose model file (plugins, hooks etc.) except `mongoose` before running.
- Fields with type `Mixed`/`Object` cannot be handled in Spark StructType. Either remove or replace with the exact structure of the object in the model file. 

## Todo

- [ ] export a `generateSchema()` function that can be imported within a NodeJS app.
    - parameter could be file path or mongoose model object

## Dependencies

- [mongoose](https://www.npmjs.com/package/mongoose)
