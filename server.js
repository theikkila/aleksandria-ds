var restify = require('restify');
var fs = require('fs');
var validator = require('is-my-json-valid/require');

var pmongo = require('promised-mongo');
var db = pmongo(process.env.MONGODB || 'mongodb://localhost/aleksandria');

var creds = {
	user: process.env.USER || 'admin',
	passwd: process.env.PASSWORD || 'password'
};


var schemas = fs.readdirSync(__dirname+'/schemas')
.filter(function (filename) {
	return /^.*\.json$/.test(filename);
}).map(function (filename) {
	var name = filename.replace('.json', '');
	var schema = validator('/schemas/'+filename,
	{
		greedy:true,
		verbose:true
	});
	return {
		name: name,
		schema: schema
	};
}).reduce(function (schemas, schema) {
	schemas[schema.name] = schema.schema;
	return schemas;
}, {});


function minmax(low, high, value) {
	return Math.max(low, Math.min(high, value));
}

/*
* Validates post request body with validator (from path param)
*/

function validate(schema, body, cb) {
	var validate_fn = schemas[schema];
	var isvalid = validate_fn(body);
	var result = {};
	if (isvalid) {
		result = {
			code: 200,
			document: {
				schema: schema,
				errors: [],
				document: body
			}
		};
		return cb(true, result.code, result.document);
	}
	var result = {
		code: 400,
		document: {
			schema: schema,
			errors: validate_fn.errors,
			document: body
		}
	};
	return cb(false, result.code, result.document);
}

function isSchema (req, res, next) {
	if (!schemas[req.params.schema]) {
		return next(new restify.InvalidArgumentError("Schema not found"));
	}
	return next();
}


function validateDocument (req, res, next) {
	validate(req.params.schema, req.body, function (valid, code, doc) {
		res.send(code, doc);
		return next();
	});
}

function postDocument (req, res, next) {
	validate(req.params.schema, req.body, function (valid, code, doc) {
		if (!valid) {
			res.send(code, doc);
			return next();
		}
		var collection = db.collection("data_" + req.params.schema);
		doc.document.created = new Date();
		collection.save(doc.document);
		res.send(201, doc);
		next();
	});
}


function getDocuments (req, res, next) {
	var limit = minmax(0, 500, parseInt(req.query.limit)) || 100;
	var skip = parseInt(req.query.skip) || 0;
	var sorting = req.query.order || {created: 1};
	Object.keys(sorting).forEach(function (key) {
		sorting[key] = sorting[key] == '1' ? 1 : -1;
	});
	var collection = db.collection("data_" + req.params.schema);
	collection.find().sort(sorting).limit(limit).skip(skip).toArray().then(function (docs) {
		res.send({
			count: docs.length,
			limit: limit,
			skip: skip,
			order: sorting,
			documents: docs});
	})
	next();
}

function getDocument (req, res, next) {
	var collection = db.collection("data_" + req.params.schema);
	collection.findOne({_id: pmongo.ObjectId(req.params.id)}).then(function (doc) {
		if (!doc) {
			res.send(404, {});
			return next();
		};
		res.send(200, doc);
		next();
	});
}

function getSchema (req, res, next) {
	res.send(schemas[req.params.schema]);
	return next();
}

function isAuthorized (req, res, next) {
	if (!req.authorization.basic
		|| !req.authorization.basic.username
		|| !req.authorization.basic.password) {
		return next(new restify.UnauthorizedError("Credentials not supplied!"));
	}
	if (req.authorization.basic.username !== creds.user
		|| req.authorization.basic.password !== creds.passwd) {
		return next(new restify.UnauthorizedError("Credentials invalid!"));
	}
	return next();
}


var server = restify.createServer({name:"Aleksandria"});

// Middleware
server.use(restify.CORS({
	headers: ['accept'],
	origins: ['*']
}));
server.use(restify.bodyParser());
server.use(restify.queryParser());
server.use(restify.gzipResponse());
server.use(restify.authorizationParser());

// Routes
server.post('/validate/:schema', isSchema, validateDocument);
server.post('/documents/:schema', isSchema, postDocument);
server.opts('/documents/:schema', isSchema, getSchema);
server.get('/documents/:schema', isAuthorized, isSchema, getDocuments);
server.get('/documents/:schema/:id', isAuthorized, isSchema, getDocument);


server.get('/', function (req, res, next) {
	var endpoints = [
	{
		name: "Validate",
		prefix: '/validate/',
		description: "Validates documents"
	}, {
		name: "Documents",
		prefix: '/documents/',
		description: 'Get or post documents'
	}
	];

	var result = {
		title: "Aleksandria Document Storage",
		endpoints: endpoints.map(function (endpoint) {
			endpoint.types = Object.keys(schemas).map(function(name) {
				return endpoint.prefix + name;
			});
			return endpoint;
		})
	};
	res.send(200, result);
});

server.listen(process.env.PORT || 8080, function() {
	console.log('%s listening at %s', server.name, server.url);
});
