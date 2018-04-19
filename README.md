# MongoDB NodeJS SDK -

To use this SDK you need to configure the `DB_URL` by setting it as an environment variable.

For example - ```export DB_URL=<MONGODB_VALID_URL_STRING>```

This SDK takes care of everything starting from connecting to mongo and taking care of graceful shutdowns. All you need to be aware of are the methods of this SDK and all the methods returns a promise.

# Sample Usage -

```
var mongoUtils = require('mongo-utils');

var doSomethingWithTheSetOfDocuments = function doSomethingWithTheSetOfDocuments(){
    return mongoUtils.findDocumentsBasedOnQuery('mongoCollectionName', {status: "done"}, 0, {_id: 0})
        .then(function(documents){
            console.log(`Documents found in collection mongoCollectionName`, documents.length); 
            return Promise.resolve(documents); //Do anything with the set of returned documents.
        })
        .catch(function(error){
            return Promise.reject(error);
        });
};
```
