const client = require('prom-client');
const _ = require('lodash');


class PromClient {

  /**
   * Method to initialize prometheus
   * @param {string} defaultLabels Dictionary with key representing label name and value the label value. This will be appended to all the metrics
   * @returns {object} Prometheus client object (does not need to be used though)
   */
  static initialize(defaultLabels) {
    // This needs to be initialized only after Log
    //check if log library is initialised
    // console.log()
    if (PromClient.client) return PromClient.client;
    const registry = new client.Registry();
    registry.setDefaultLabels(defaultLabels);

    client.collectDefaultMetrics({ register: registry });
    PromClient.client = registry;

    PromClient.collectPromiseRejections();
    return PromClient.client;
  }

  /**
   * Method to create event listeners to record promise rejections for the whole service
   */
  static collectPromiseRejections() {
    process.on('unhandledRejection', (reason) => {
      const counter = PromClient.getCounter({ name: 'nodejs_promise_unhandled_rejection_count', help: 'metric_help' });
      counter.inc();
    });

    process.on('rejectionHandled', () => {
      const counter = PromClient.getCounter({ name: 'nodejs_promise_handled_rejection_count', help: 'metric_help' });
      counter.inc();
    });
  }

  /**
   * Method to serve metrics collected
   * @param {object} app app object
   * @param {string} url if you want the exposed url to be something other than default metrics
   * @returns {null}
   */
  static serveExpressMetrics(app, url) {
    if (!PromClient.client) throw new Error('PromClient not initialized');
    if (_.isNil(url)) app.get('/metrics', (req, res) => res.end(PromClient.client.metrics()));
    else app.get(url, (req, res) => res.end(PromClient.client.metrics()));
  }

  /**
   * Method to create histograms to record time taken to serve all URL's
   * @param {object} app app object
   * @param {array} bkts time buckets to be created for the histogram
   * @returns {null}
   */
  static expressMiddleware(app, bkts) {
    // Register this before paths
    if (!PromClient.client) throw new Error('PromClient not initialized');
    const buckets = !_.isNil(bkts) ? bkts : [0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 5, 10, 60, 120];
    app.use((req, res, next) => {
      const histogram = PromClient.getHistogram({
        name: 'nodejs_http_request_duration_seconds',
        help: 'metric_help',
        labelNames: ['route'],
        buckets,
      });
      const end = histogram.startTimer();
      const route = req.path;
      const { method } = req;
      res.on('header', () => {
        end({ route, method });
      });
      next();
    });
  }

  static getCounter(config) {
    if (!config || !config.name) throw new Error('Invalid arguments');
    if (!PromClient.client) throw new Error('PromClient not initialized');
    let counter = PromClient.client.getSingleMetric(config.name);
    if (!counter) {
      const newConfig = _.cloneDeep(config);
      newConfig.registers = [PromClient.client];
      counter = new client.Counter(newConfig);
    }
    return counter;
  }

  static getHistogram(config) {
    if (!config || !config.name) throw new Error('Invalid arguments');
    if (!PromClient.client) throw new Error('PromClient not initialized');
    let histogram = PromClient.client.getSingleMetric(config.name);
    if (!histogram) {
      const newConfig = _.cloneDeep(config);
      newConfig.registers = [PromClient.client];
      histogram = new client.Histogram(newConfig);
    }
    return histogram;
  }
}

module.exports = PromClient;
