# Flowpack.JobQueue.RabbitMQ

A job queue backend for the [Flowpack.JobQueue.Common](https://github.com/Flowpack/jobqueue-common) package based on [RabbitMQ](https://www.rabbitmq.com).

## Usage

Install the package using composer:

```
composer require flowpack/jobqueue-rabbitmq
```

Now the queue can be configured like this:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'some-queue':
          className: 'Flowpack\JobQueue\RabbitMQ\RabbitQueue'
          executeIsolated: true
          options:
          	passive: false
          	durable: false
          	exclusive: false
          	autoDelete: false
            client:
              host: localhost
              port: 5672
              username: guest
              password: guest
              vhost: '/'
            defaultTimeout: 3.0
