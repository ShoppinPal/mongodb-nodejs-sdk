# MongoDB NodeJS SDK -

To use this SDK you need to configure the `DB_URL` by setting it as an environment variable.

For example - ```export DB_URL="<MONGODB_VALID_URL_STRING>"```

Docs - https://shoppinpal.github.io/mongodb-nodejs-sdk/

NPM - https://www.npmjs.com/package/mongodb-nodejs-sdk

This SDK takes care of everything starting from connecting to mongo and taking care of graceful shutdowns.
All you need to be aware of are the methods of this SDK and all the methods returns a promise.

# Sample Usage -

```

const mongoUtils = require('./utils');

mongoUtils.initialize(DB_URL).then(async (resp) => {
  console.log('connected', resp);
  const resp = await mongoUtils.insertIntoDb('testCollection', ['asds','dasdsa','dasds']);
});

```


const PromClient = require('./prometheus');


await PromClient.initialize({ job: 'jobName', instance: myIP() });
PromClient.expressMiddleware(app);
PromClient.serveExpressMetrics(app);


const counter = PromClient.getCounter({ name: 'nodejs_product_service', help: 'metric_help', labelNames: ['status', 'state'] });
counter.inc({ status: 'retry', state: '0' });
counter.inc({ status: 'fail', state: '0' });


const histogram = PromClient.getHistogram({
    name: 'nodejs_http_request_duration_seconds',
    help: 'metric_help',
    labelNames: ['route'],
    buckets,
  });
const end = histogram.startTimer();
end({ route, method });
