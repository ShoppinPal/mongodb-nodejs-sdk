const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const _ = require('lodash');
const Promise = require('bluebird');
var dbHandleForShutDowns;
Promise.promisifyAll(MongoClient);


var insertIntoDb = function insertIntoDb(collectionName, documents) {
  if (documents.length < 1) {
    return Promise.resolve();
  }
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function insertData(db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).insertMany(documents, {w: 1})
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var updateDocument = function updateDocument(collectionName, mutableEntity) {
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function insertData(db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).findOneAndReplace(
        {
          _id: mutableEntity._id
        },
        mutableEntity,
        {
          returnOriginal: false,
          upsert: false
        })
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var upsertDocument = function upsertDocument(collectionName, mutableEntity, upsert, query) {
  if (!upsert){
    upsert = false;
  }
  if (!query){
    query =  {
      _id: mutableEntity._id
    };
  }
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function insertData(db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).findOneAndUpdate(
        query,
        mutableEntity,
        {
          upsert: upsert
        })
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var findOneDocumentBasedOnQuery = function findOneDocumentBasedOnQuery(collectionName, query) {
  // limit = limit ? limit : 5;
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function (db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).find(query).sort({_id: -1}).limit(5)
        .toArray()
        .then(function(documents) {
          return documents[0];
        })
        .catch(function (err) {
          return Promise.reject(err);
        })
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var findDocumentsBasedOnQuery = function findDocumentsBasedOnQuery(collectionName, query, limit, projection) {
  if (isEmpty(limit)) {
    limit = 0; // A limit() value of 0 (i.e. .limit(0)) is equivalent to setting no limit.
  }
  if(!projection){
    projection = {};
  }
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function (db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).find(query).project(projection).limit(limit).toArray()
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var countDocumentsByQuery = function countDocumentsByQuery(collectionName, query) {
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function (db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).find(query).count()
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

/**
 * Asumptions:
 *   a) sorts will happen by `_id` in this method
 *   b) `query._id` is overriden by this method
 *
 * @param {*} db
 * @param {*} collectionName
 * @param {*} query
 * @param {*} projection
 * @param {*} pageSize
 * @param {*} processPage - pass a function to handle the pagedResults
 * @param {*} processPageArgs - additional arguments required by processPage
 */
var workOnItPageByPage = function workOnItPageByPage(db, collectionName, query, projection, pageSize, processPage, processPageArgs) {
  projection = (projection) ? projection['_id'] = true : {'_id': true};
  processPageArgs = processPageArgs || []; // as a fallback, these can be empty
  return db
    .collection(collectionName)
    .find(query)
    // .project(projection)
    .sort({'_id': 1}).limit(pageSize)
    .toArray() // cursor methods return promises: http://mongodb.github.io/node-mongodb-native/2.1/api/Cursor.html#toArray
    .then(function processPagedResults(documents) {
      if (!documents || documents.length < 1) {
        // stop - no data left to traverse
        return Promise.resolve();
      }
      else {
        if (documents.length < pageSize) {
          // stop - last page
          return processPage(db, documents, ...processPageArgs); // process the results of the LAST page
        }
        else {
          return processPage(db, documents, ...processPageArgs) // process the results of the current page
            .then(function getNextPage(){ // then go get the next page
            var last_id = documents[documents.length - 1]['_id'];
            query['_id'] = {'$gt': last_id};
            return workOnItPageByPage(db, collectionName, query, projection, pageSize, processPage, processPageArgs);
          });
        }
      }
    });
};

var connectDb = function connectDb() {
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function (db) {
      dbHandleForShutDowns = db;
      return Promise.resolve(db);
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};

var bulkCreate = function bulkCreate(db, collectionName, documents) {
  if (!documents || documents.length === 0) return Promise.resolve();
  // (1) Initialize the unordered Batch
  var batch = db.collection(collectionName).initializeUnorderedBulkOp();
  // (2) Add some operations to be executed
  for (var i = 0; i < documents.length; i++) {
    //console.log(documents[i]);
    batch.insert(documents[i]);
  }
  // (3) Execute the operations
  return batch.execute();
};

var bulkUpdate = function bulkUpdate(db, collectionName, updates, omits) {
  if (!updates || updates.length === 0) return Promise.resolve();
  // (1) Initialize the unordered Batch
  var batch = db.collection(collectionName).initializeUnorderedBulkOp();
  // (2) Add some operations to be executed
  for (var i = 0; i < updates.length; i++) {
    var update;
    (omits) ? update = _.omit(updates[i], omits.toString()) : update = updates[i];
    var updateOp = {$set: update};
    batch.find({'_id': new ObjectID(update._id)}).updateOne(updateOp);
  }
  // (3) Execute the operations
  return batch.execute();
};

/**
 * Idea came from https://medium.com/@gchudnov/trapping-signals-in-docker-containers-7a57fdda7d86
 *
 * @param {*} shutdown - the calling code user this method to control what actions to take as part of shutdown
 */
var registerForGracefulShutdown = function registerForGracefulShutdown(shutdown) {
  var signals = {
    'SIGINT': 2,
    'SIGTERM': 15
  };
  
  Object.keys(signals).forEach(function (signal) {
    process.on(signal, function () {
      shutdown(signal, signals[signal]);
    });
  });
};

var isEmpty = function (input) {
  if (_.isString(input)) {
    return input === undefined || input === null || input.trim() === '';
  }
  else {
    return input === undefined || input === null;
  }
};

var insertOne = function insertOne(collectionName, document) {
  if (!document) {
    return Promise.resolve();
  }
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function insertData(db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName).insertOne(Object.assign({}, document))
        .finally(db.close.bind(db));
    })
    .catch(function catchErrors(err) {
      if (dbHandleForShutDowns) {
        dbHandleForShutDowns.close();
      }
      throw err;
    });
};


module.exports = {
  bulkCreate: bulkCreate,
  insertOne: insertOne,
  bulkUpdate: bulkUpdate,
  findOneDocumentBasedOnQuery: findOneDocumentBasedOnQuery,
  findDocumentsBasedOnQuery: findDocumentsBasedOnQuery,
  insertIntoDb: insertIntoDb,
  isEmpty: isEmpty,
  countDocumentsByQuery: countDocumentsByQuery,
  updateDocument: updateDocument,
  connectDb: connectDb,
  registerForGracefulShutdown: registerForGracefulShutdown,
  workOnItPageByPage: workOnItPageByPage,
  upsertDocument: upsertDocument
};

