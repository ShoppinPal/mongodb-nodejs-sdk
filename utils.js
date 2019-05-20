const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const _ = require('lodash');
const Promise = require('bluebird');

Promise.promisifyAll(MongoClient);

/**
 * The response will be standard for all the functions
 * {
 *  status : true/false,
 *  resp: //this will be the response if it is a select query, in case of upsert/inserts, this will throw back the ID of the document updated.
 *  text: 'this will be the extra log line to be printed in cases like no documents found, wrong function calls, errors etc. this should be printed directly into logs'
 * }
 * The ID's returned might be useful later on in some cases
*/

class DBConnection {
  constructor() {
    this.dbObject = null;
    this.DB_URL = null;
  }

  /**
   * Method to initialize connection with the mongo database
   * @param {string} DB_URL The details of the mongo DB server in format mongodb://[user]:[password]@[IP]:[port]/[database_name]
   * @returns {null} Nothing
   */
  static async initialize(DB_URL) {// auto reconnects on issues losing network
    this.DB_URL = DB_URL;
    try {
      this.dbObject = await MongoClient.connect(this.DB_URL, { promiseLibrary: Promise });
      // Other events can be added :: http://mongodb.github.io/node-mongodb-native/3.2/api/Server.html#event:error
      this.dbObject
      .on('error', (err) => {
        // This event is unfortunately not bubbled up to the db handle correctly,
        // see https://github.com/mongodb/node-mongodb-native/pull/1545
        // This event is emitted when the driver ran out of `reconnectTries`. At this
        // point you should either crash your app or manually try to reconnect.
        // TODO :: Add graceful shutdown.
        console.log('error', err);
      })
      .on('reconnect', () => {
        console.log('reconnected');
      })
      .on('close', () => {
        console.log('close');
      });
      return {
        status: true,
      };
    } catch (err) {
      this.dbObject = null;
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to insert documents in a given collection
   * @param {string} collectionName Name of the collection
   * @param {array} documents - Array of documents that will be inserted.
   * @returns {object} A document with `acknowledged: true` and an array of successfully inserted _id's
   */
  static async insertIntoDb(collectionName, documents) {
    if (documents.length < 1) {
      return {
        status: true,
        text: 'No documents found to insert',
      };
    }
    try {
      const resp = await DBConnection.dbObject.collection(collectionName).insertMany(documents, { w: 1 });
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
  /**
   * Method to update a document in a given collection based on _id.
   * @param {string} collectionName Name of the collection
   * @param {object} mutableEntity - Document to update in the collection
   * @param {boolean} timeStamp - Default is false. If set to true it adds a key lastModifiedAt to the document with the current timestamp.
   * @returns {*}
   */
  static async updateDocument(collectionName, mutableEntity, timeStamp) {
    if (timeStamp) {
      mutableEntity.lastModifiedAt = new Date().toISOString();
    }
    try {
      const resp = await DBConnection.dbObject.collection(collectionName).findOneAndReplace(
        {
          _id: mutableEntity._id,
        },
        mutableEntity,
        {
          returnOriginal: false,
          upsert: false,
        });
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
  /**
 * Method to upsert a document in a given collection
 * @param {string} collectionName Name of the collection
 * @param {object} mutableEntity Properties that will be updated.
 * @param {boolean} upsert Default is false, if set to true it will create or update the document with the given set of properties.
 * @param {object} query Default is querying by _id but a custom query can be specified.
 * @returns {*}
 */
  static async upsertDocument(collectionName, mutableEntity, upsert, query) {
    if (!upsert) {
      upsert = false;
    }
    if (!query) {
      query = {
        _id: mutableEntity._id,
      };
    }
    try {
      const resp = await DBConnection.dbObject.collection(collectionName).findOneAndUpdate(
        query,
        mutableEntity,
        {
          upsert,
        });
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

/**
 * Method to find one document based on a given query
 * @param {string} collectionName Name of the collection
 * @param {object} query Query
 * @param {object} sort Sort, by default it sorts by _id.
 * @returns {object} an document if a match is found based on the query.
 */
  static async findOneDocumentBasedOnQuery(collectionName, query, sort) {
    if (!sort) {
      sort = {
        _id: 1,
      };
    }
    try {
      const resp = await DBConnection.dbObject.collection(collectionName).find(query).sort(sort).limit(5).toArray()[0];
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  static async findDocumentsBasedOnQuery(collectionName, query, limit, projection) {
    if (isEmpty(limit)) {
      limit = 0; // A limit() value of 0 (i.e. .limit(0)) is equivalent to setting no limit.
    }
    if (!projection) {
      projection = {};
    }

    try {
      const resp = await DBConnection.dbObject.collection(collectionName).find(query).project(projection).limit(limit).toArray();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to count documents based on query
   * @param {string} collectionName Name of the collection
   * @param {object} query Query object
   * @returns {number} the count of documents based on a given query
   */
  static async countDocumentsByQuery(collectionName, query) {
    try {
      const resp = await DBConnection.dbObject.collection(collectionName).find(query).count();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to work on a collection page by page. PageSize can be even 1. Ideal if you want to work in batches.
   * This method requires you to connect to the DB first by using connectDb(). \n \n
   * Assumptions:
   *   a) sorts will happen by `_id` in this method
   *   b) `query._id` is overriden by this method
   *
   * @param {string} collectionName Name of the collection
   * @param {object} query query object
   * @param {object} projection fields to project
   * @param {number} pageSize page size to return from the collection.
   * @param {function} processPage pass a function to handle the pagedResults
   * @param {array} processPageArgs additional arguments required by processPage
   */
  static async workOnItPageByPage(collectionName, query, projection, pageSize, processPage, processPageArgs) {
    projection = (projection) ? projection._id = true : { _id: true };
    processPageArgs = processPageArgs || []; // as a fallback, these can be empty
    const documents = await DBConnection.dbObject
      .collection(collectionName)
      .find(query)
      // .project(projection)
      .sort({ _id: 1 }).limit(pageSize)
      .toArray(); // cursor methods return promises: http://mongodb.github.io/node-mongodb-native/2.1/api/Cursor.html#toArray
    if (!documents || documents.length < 1) {
        // stop - no data left to traverse
      return {
        status: false,
        text: 'no documents found',
      };
    } else if (documents.length < pageSize) {
          // stop - last page
      return processPage(documents, ...processPageArgs); // process the results of the LAST page
    }

    return processPage(documents, ...processPageArgs) // process the results of the current page
        .then(() => { // then go get the next page
          const lastId = documents[documents.length - 1]._id;
          query._id = { $gt: lastId };
          return DBConnection.workOnItPageByPage(collectionName, query, projection, pageSize, processPage, processPageArgs);
        });
  }

  /**
   * Method to create documents in bulk in a given collection.
   * @param {string} collectionName - Name of the collection
   * @param {array }documents Array of documents to be created
   * @returns {*}
   */
  static async bulkCreate(collectionName, documents) {
    if (!documents || documents.length === 0) {
      return {
        status: true,
        text: 'No documents to insert',
      };
    }
    // (1) Initialize the unordered Batch
    const batch = DBConnection.dbObject.collection(collectionName).initializeUnorderedBulkOp();
    // (2) Add some operations to be executed
    for (let i = 0; i < documents.length; i++) {
      batch.insert(documents[i]);
    }
    // (3) Execute the operations
    try {
      const resp = await batch.execute();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to update documents bulk in a given collection
   * @param {string} collectionName Name of the collection
   * @param {array} updates array of documents to update
   * @param {object} omits  Fields to omit while updating the documents in the collection
   * @returns {*}
   */
  static async bulkUpdate(collectionName, updates, omits) {
    if (!updates || updates.length === 0) {
      return {
        status: true,
        text: 'No updates found',
      };
    }
    // (1) Initialize the unordered Batch
    const batch = DBConnection.dbObject.collection(collectionName).initializeUnorderedBulkOp();
    // (2) Add some operations to be executed
    for (let i = 0; i < updates.length; i += 1) {
      let update;
      (omits) ? update = _.omit(updates[i], omits.toString()) : update = updates[i];
      const updateOp = { $set: update };
      batch.find({ _id: new ObjectID(update._id) }).updateOne(updateOp);
    }
    // (3) Execute the operations
    try {
      const resp = await batch.execute();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
  /**
   * Method to insert a single document.
   * @param {string} collectionName name of the collection
   * @param {object} document document to insert
   * @returns {*}
   */
  static async insertOne(collectionName, document) {
    if (!document) {
      return {
        status: false,
        text: 'No documents found to insert',
      };
    }
    try {
      const resp = DBConnection.dbObject.collection(collectionName).insertOne(Object.assign({}, document));
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
  /**
   * Method to drop a collection
   * @param {string} name name of the collection to drop.
   * @returns {*}
   */
  static async dropCollection(name) {
    try {
      const resp = DBConnection.dbObject.collection(name).drop();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to find distinct documents in a collection
   * @param {string} collectionName name of the collection
   * @param {string} field - Distinct Field
   * @returns {array} an array of field values that are in the collection
   */
  static async findDistinctDocuments(collectionName, field) {
    try {
      const collection = await DBConnection.dbObject.collection(collectionName);
      const resp = collection.distinct(field, { [field]: { $exists: true } });
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
  /**
   * Method to process a batch of documents in a collection
   * @param collectionName - Name of the collection
   * @param query - Query on the basis of which documents will be picked from a collection.
   * @param batchSize - Size of the batch you'd want to process
   * @param processBatch - Function/method to run once desired docs are fetched from the DB.
   * @param processBatchArgs - Additional arguments that are required to be passed onto the processBatch method.
   */
  static async processABatchOfDocuments(collectionName, query, batchSize, processBatch, processBatchArgs) {
    processBatchArgs = processBatchArgs || [];
    try {
      const documents = await DBConnection.dbObject
      .collection(collectionName)
      .find(query)
      .sort({ _id: 1 }).limit(batchSize)
      .toArray();
      if (!documents || documents.length < 1) {
        return Promise.resolve(false);
      }
      return processBatch(documents, ...processBatchArgs);
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }

  /**
   * Method to bulk update documents in a collection given a specific query.
   * @param {string} collectionName Name of the collection
   * @param {object} updates Values that will be updated. Can update multiple values or set new values too.
   * @param {object} query Query to find the documents in the collection to update
   * @returns {object}
   */
  static async bulkUpdateByQuery(collectionName, updates, query) {
    const bulk = DBConnection.dbObject.collection(collectionName).initializeUnorderedBulkOp();
    bulk.find(query).update({ $set: updates });
    try {
      const resp = await bulk.execute();
      return {
        status: true,
        resp,
      };
    } catch (err) {
      return {
        status: false,
        text: err.message,
      };
    }
  }
}

/**
 * Idea came from https://medium.com/@gchudnov/trapping-signals-in-docker-containers-7a57fdda7d86
 *
 * @param {*} shutdown - the calling code user this method to control what actions to take as part of shutdown
 */
const registerForGracefulShutdown = function registerForGracefulShutdown(shutdown) {
  const signals = {
    SIGINT: 2,
    SIGTERM: 15,
  };

  Object.keys(signals).forEach((signal) => {
    process.on(signal, () => {
      shutdown(signal, signals[signal]);
    });
  });
};

const isEmpty = function (input) {
  if (_.isString(input)) {
    return input === undefined || input === null || input.trim() === '';
  }

  return input === undefined || input === null;
};

module.exports = DBConnection;

