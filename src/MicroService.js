const { nanoid } = require('nanoid');
const BaseApp = require('./BaseApp');
const RabbitApp = require('./RabbitApp');
const Response = require('./Response');
const Server = require('./Server');
const { isRpcAction, parseRabbitMessage } = require('./utils');
const debug = require('./utils/debug')('micromq-microservice');

class MicroService extends BaseApp {
  constructor(options) {
    super(options);

    this._requests = new Map();

    if (options.microservices && options.microservices.length) {
      this._microservices = options.microservices.reduce((object, name) => ({
        ...object,
        [name]: new RabbitApp({
          rabbit: options.rabbit,
          name,
        }),
      }), {});
    }
  }

  async _startConsumers() {
    if (this._consumersStarting) {
      return;
    }

    this._consumersStarting = true;

    const connection = await this.createConnection();

    await Promise.all(
      Object.values(this._microservices).map(async (microservice) => {
        // reuse current microservice connection
        microservice.connection = connection;

        const [channel] = await Promise.all([
          microservice.createChannelByPid({
            autoDelete: true,
          }),

          // prepare requests channel for this.ask
          microservice.createRequestsChannel(),
        ]);

        channel.consume(microservice.queuePidName, (message) => {
          const json = parseRabbitMessage(message);

          if (!json) {
            channel.ack(message);

            return;
          }

          const { response, statusCode, requestId } = json;
          const { resolve } = this._requests.get(requestId);

          resolve({ status: statusCode, response });

          this._requests.delete(requestId);
          channel.ack(message);
        });
      }),
    );

    this._consumersReady = true;
  }

  async ask(name, query) {
    if (!this._consumersReady) {
      await this._startConsumers();
    }

    const microservice = this._microservices[name];

    if (!microservice) {
      throw new Error(`Microservice ${name} not found`);
    }

    let resolve;
    const requestId = nanoid();
    const promise = new Promise(r => (resolve = r));

    this._requests.set(requestId, { resolve });

    const channel = await microservice.createRequestsChannel();

    await channel.sendToQueue(microservice.requestsQueueName, Buffer.from(JSON.stringify({
      ...query,
      requestId,
      queue: microservice.queuePidName,
    })));

    return promise;
  }

  listen(port) {
    const server = new Server();

    server.all('(.*)', async (req, res) => {
      req.app = this;
      res.app = this;

      await this._next(req, res);
    });

    return server.createServer(port);
  }

  async start() {

    debug(() => `create request channel ${this.requestsQueueName}`);
    const requestsChannel = await this.createRequestsChannel();

    // prepare responses channel before consume
    debug(() => `create response channel ${this.requestsQueueName}`);
    await this.createResponsesChannel();

    ['error', 'close'].forEach((event) => {
      requestsChannel.on(event, async (err) => {
        debug(() => 're-creating channels');
        this.connection = null;
        this.requestsChannel = null;
        this.responsesChannel = null;
        await this.sleep(3000)
        this.start();
      });
    });

    debug(() => `starting to consume ${this.requestsQueueName}`);

    requestsChannel.consume(this.requestsQueueName, async (message) => {
      const json = parseRabbitMessage(message);

      if (!json) {
        requestsChannel.ack(message);

        return;
      }

      const { requestId, queue, ...request } = json;

      const responsesChannel = await this.createResponsesChannel();
      const response = new Response(responsesChannel, queue, requestId);

      request.app = this;
      response.app = this;

      if (isRpcAction(request)) {
        await this._actions.handle(request, response);
      } else {
        await this._next(request, response);
      }

      requestsChannel.ack(message);
    });
  }
}

module.exports = MicroService;
