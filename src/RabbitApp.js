const amqplib = require('amqplib');
const debug = require('./utils/debug')('micromq-rabbit');
const { nanoid } = require('nanoid');

class RabbitApp {
  constructor(options) {
    this.options = options;

    this.requestsQueueName = `${this.options.name}:requests`;
    this.responsesQueueName = `${this.options.name}:responses`;
  }

  set connection(connection) {
    this._connection = connection;
  }

  get connection() {
    return this._connection;
  }

  get queuePidName() {
    return `${this.responsesQueueName}-${process.pid}`;
    // const requestId = nanoid();
    // return `${this.responsesQueueName}-${requestId}`;
  }

async sleep (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}

  async createConnection() {
    if (!this.connection) {
      debug(() => 'creating connection');

      let _this = this
      while(!this.connection) {
        try {
            _this.connection = await amqplib.connect(this.options.rabbit.url);
        } catch (e) {
            debug(() => 'AMQ connection error ' + e.message);
            _this.connection = null
            await _this.sleep(3000);
        }
      }

      ['error', 'close'].forEach((event) => {
        this.connection.on(event, err => {
          debug(() => 'release connection ' + JSON.stringify(err));
          this.connection = null;
        });
      });
    }

    return this.connection;
  }

  async createChannel(queueName, options) {
    debug(() => `creating channel and asserting to ${queueName} queue`);

    const connection = await this.createConnection();
    const channel = await connection.createChannel();

    if (queueName) {
      await channel.assertQueue(queueName, options);
    }

    return channel;
  }

  async createResponsesChannel() {
    debug(() => 'create response channel, connection = ' + JSON.stringify(!(this.connection === undefined || this.connection === null)));
    debug(() => 'create response channel, response channel = ' + JSON.stringify(!(this.responsesChannel === undefined || this.responsesChannel === null)));
    if (!this.responsesChannel) {
      this.responsesChannel = await this.createChannel(this.responsesQueueName);
    }

    return this.responsesChannel;
  }

  async createRequestsChannel() {
    debug(() => 'create requests channel, connection = ' + JSON.stringify(!(this.connection === undefined || this.connection === null)));
    debug(() => 'create requests channel, response channel = ' + JSON.stringify(!(this.requestsChannel === undefined || this.requestsChannel === null)));
    if (!this.requestsChannel) {
      this.requestsChannel = await this.createChannel(this.requestsQueueName);
    }

    return this.requestsChannel;
  }

  async createChannelByPid(options) {
    if (!this.pidChannel) {
      this.pidChannel = await this.createChannel(this.queuePidName, options);
    }

    return this.pidChannel;
  }
}

module.exports = RabbitApp;
