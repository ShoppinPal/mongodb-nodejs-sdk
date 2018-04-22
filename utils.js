const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const _ = require('lodash');
const Promise = require('bluebird');
Promise.promisifyAll(MongoClient);

/**
 * Method to insert documents in a given collection
 * @param collectionName - Name of the collection
 * @param documents - Array of documents that will be inserted.
 * @returns {*}
 */
var insertIntoDb = function insertIntoDb(collectionName, documents) {
  var dbHandleForShutDowns;
  if (documents.length < 1) {
    return Promise.resolve();
  }
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

/**
 * Method to update a document in a given collection based on _id.
 * @param collectionName - Name of the collection
 * @param mutableEntity - Document to update in the collection
 * @param timeStamp - Default is false. If set to true it adds a key lastModifiedAt to the document with the current timestamp.
 * @returns {*}
 */
var updateDocument = function updateDocument(collectionName, mutableEntity, timeStamp) {
  var dbHandleForShutDowns;
  if (timeStamp){
    mutableEntity.lastModifiedAt = new Date().toISOString();
  }
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

/**
 * Method to upsert a document in a given collection
 * @param collectionName - Name of the collection
 * @param mutableEntity - Properties that will be updated.
 * @param upsert - Default is false, if set to true it will create or update the document with the given set of properties.
 * @param query - Default is querying by _id but a custom query can be specified.
 * @returns {*}
 */
var upsertDocument = function upsertDocument(collectionName, mutableEntity, upsert, query) {
  var dbHandleForShutDowns;
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

/**
 * Method to find one document based on a given query
 * @param collectionName - Name of the collection
 * @param query - Query
 * @returns {*}
 */
var findOneDocumentBasedOnQuery = function findOneDocumentBasedOnQuery(collectionName, query) {
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

/**
 * Method to find documents based on query
 * @param collectionName - Name of the collection
 * @param query - Query
 * @param limit - Limit to the query
 * @param projection - Query Projection
 * @returns {*}
 */
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

/**
 * Method to count documents based on query
 * @param collectionName
 * @param query
 * @returns {*}
 */
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

/** This method requires you to connect to the DB first by using connectDb().
 * Assumptions:
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

/**
 * Method to connect to the db
 * @returns {*}
 */
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

/**
 * Method to create documents in bulk in a given collection.
 * @param db
 * @param collectionName - Name of the collection
 * @param documents - Array of documents to be created
 * @returns {*}
 */
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

/**
 * Method to update documents bulk in a given collection
 * @param db
 * @param collectionName - Name of the collection
 * @param updates - Updates in the documents
 * @param omits - Fields to omit while updating the documents in the collection
 * @returns {*}
 */
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

/**
 * Method to insert a single document.
 * @param collectionName
 * @param document
 * @returns {*}
 */
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
/**
 * Method to drop a collection
 * @param name - Name of the collection to drop.
 * @returns {*}
 */
var dropCollection = function dropCollection(name) {
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function dropDb(db) {
      dbHandleForShutDowns = db;
      return db.collection(name).drop()
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
 * Method to find distinct documents in a collection
 * @param collectionName - Name of the collection
 * @param field - Distinct Field
 * @returns {*}
 */
var findDistinctDocuments = function findDistinctDocuments(collectionName, field) {
  var dbHandleForShutDowns;
  return MongoClient.connect(process.env.DB_URL, {promiseLibrary: Promise})
    .then(function (db) {
      dbHandleForShutDowns = db;
      return db.collection(collectionName);
    })
    .then(function(collection){
      return collection.distinct(field, {[field]: {$exists: true}})
        .finally(dbHandleForShutDowns.close.bind(dbHandleForShutDowns));
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
  findDistinctDocuments: findDistinctDocuments,
  dropCollection: dropCollection,
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

