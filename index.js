/**
 * @author Vishal Gauba
 * @desc Generate Spark StructType schema JSON from Mongoose model
 * @example node index.js --model=./path-to-mongoose-model.js
 * @example node index.js --model=./path-to-mongoose-model.js --output=./path-to-output-schema.json
 */

const fs = require('fs')
const fsPromises = fs.promises

class Main {

	// mapping mongoose.js types to spark structtype types
	static TypeMappings = {
		String: 'string',
		Date: 'timestamp',
		ObjectId: 'string',
		Boolean: 'boolean',
		Number: 'float',
	}

	static BaseBlock = {
		metadata: {},
		nullable: true,
		name: null,
		type: null,
	}

	constructor(args){
		if (!args.model || !fs.existsSync(args.model)) {
			console.log('ERROR: One or more required arguments is either missing or invalid (--model)')
			process.exit(1)
		}

		this.model_path = args.model
		this.output_path = args.output
	}

	async generateSchema(){
		try {
			var model = require(this.model_path)
		} catch (err) {
			console.log(`ERROR: Could not import mongoose model file. Manually remove all dependencies (plugins, hooks) except 'mongoose' in ${this.model_path} and re-run.`)
			process.exit(1)
		}

		const schema_tree = model.schema.tree

		// recursively parse mongoose schema to spark schema
		const spark_schema_fields = this._parseObject({ value: schema_tree })

		const spark_schema_json = {
			collection_name: model.modelName,
			spark_schema_json: {
				type: "struct",
				fields: spark_schema_fields
			}
		}

		const output = JSON.stringify(spark_schema_json, null, 4)

		if (!this.output_path){
			console.log(output)
		} else {
			await Helpers.writeFile(this.output_path, output)
		}

		return this.output
	}

	/**
	 * Recursively parse mongoose schema
	 */
	_parseObject({key, value}){

		// {name: String, age: Number}
		if (!key){
			let fields = []
			for (let [k, v] of Object.entries(value)) {
				let result = this._parseObject({ key: k, value: v })
				if (result) fields.push(result)
			}
			return fields
		}

		// ignore
		if (key === '__v' || value.constructor.name === 'VirtualType' || value === false) {
			return
		}

		// mongoose subdocuments
		// users: mongoose.Schema()
		if (value.constructor.name === 'Schema') {
			value = value.tree
		}

		// _id: ObjectId
		if (value === 'ObjectId') {
			return {
				...Main.BaseBlock,
				type: this._getSparkType(key, value),
				name: key,
			}
		}

		// age: Number
		if (value.constructor.name === 'Function') {
			return {
				...Main.BaseBlock,
				type: this._getSparkType(key, value.name),
				name: key,
			}
		}

		if (value.constructor.name === 'Object') {
			// age: {type: Number, default: 0, enum: []}
			// _id: { auto: true, type: 'ObjectId' }
			if (value.type
				// handle fields with key name 'type'
				&& (value.type.constructor.name === 'Function' || value.type.constructor.name === 'String')
			) {
				return {
					...Main.BaseBlock,
					...this._parseObject({key, value: value.type}),
					name: key,
				}
			}

			// user: {name: String, age: Number}
			return {
				...Main.BaseBlock,
				type: {
					type: "struct",
					fields: this._parseObject({ value })
				},
				name: key,
			}
		}

		if (value.constructor.name === 'Array') {

			// mongoose subdocuments
			// users: [mongoose.Schema()]
			if (value[0].constructor.name === 'Schema') {
				value[0] = value[0].tree
			}

			// users: [{name: String, age: Number}]
			if (value[0].constructor.name === 'Object'
				// handle fields with key name 'type'
				&& !(value[0].type && (value[0].type.constructor.name === 'Function' || value[0].type.constructor.name === 'String'))
			) {
				return {
					...Main.BaseBlock,
					name: key,
					type: {
						containsNull: true,
						elementType: {
							type: "struct",
							fields: this._parseObject({value: value[0]})
						},
						type: "array"
					}
				}
			}

			// names: [String]
			return this._parseObject({key, value: value[0]})
		}

		console.log(`ERROR: Could not parse model due to ${JSON.stringify({[key]: value}, null, 4)}. Manually fix such fields in ${this.model_path} and re-run.`)
		process.exit(1)
	}

	_getSparkType(key, mongooseType){
		const sparkType = Main.TypeMappings[mongooseType]
		if (!sparkType){
			console.log(`ERROR: Mongoose type ${mongooseType} (at model path ${key}) is not supported yet. Manually fix such fields in ${this.model_path} and re-run.`)
			process.exit(1)
		}
		return sparkType
	}

}

class Helpers {
	static getArgs() {
		const args = {};
		process.argv.slice(2, process.argv.length).forEach(arg => {
			// long arg
			if (arg.slice(0, 2) === '--') {
				const longArg = arg.split('=');
				const longArgFlag = longArg[0].slice(2, longArg[0].length);
				const longArgValue = longArg.length > 1 ? longArg[1] : true;
				args[longArgFlag] = longArgValue;
			}
		});
		return args;
	}

	static writeFile(fileName, content) {
		return fsPromises.writeFile(fileName, content)
	}
}

new Main(Helpers.getArgs())
	.generateSchema()
	.catch(console.error)

// TODO: convert to module, export function
