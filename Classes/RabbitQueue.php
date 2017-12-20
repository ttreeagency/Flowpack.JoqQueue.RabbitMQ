<?php
declare(strict_types=1);

namespace Flowpack\JobQueue\RabbitMQ;

use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Exception;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * A queue implementation using RabbitMQ as the queue backend
 */
class RabbitQueue implements QueueInterface
{

    /**
     * @var string
     */
    protected $name;

    /**
     * @var AMQPStreamConnection
     */
    protected $connection;

    /**
     * @var integer
     */
    protected $defaultTimeout = null;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options = [])
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        $clientOptions = isset($options['client']) ? $options['client'] : [];
        $host = isset($clientOptions['host']) ? $clientOptions['host'] : 'localhost';
        $port = isset($clientOptions['port']) ? $clientOptions['port'] : 5672;
        $username = isset($clientOptions['username']) ? $clientOptions['username'] : 'guest';
        $password = isset($clientOptions['password']) ? $clientOptions['password'] : 'guest';
        $vhost = isset($clientOptions['vhost']) ? $clientOptions['vhost'] : '/';
        $insist = isset($clientOptions['insist']) ? (bool)$clientOptions['insist'] : false;
        $loginMethod = isset($clientOptions['loginMethod']) ? (bool)$clientOptions['loginMethod'] : 'AMQPLAIN';

        $this->connection = new AMQPStreamConnection($host, $port, $username, $password, $vhost, $insist, $loginMethod, null, 'en_US', $this->defaultTimeout, $this->defaultTimeout);
        $this->channel = $this->connection->channel();

        $this->channel->basic_qos(null, 1, null);

        if (isset($options['exchange']) && \is_array($options['exchange'])) {
            $exchangeOptions = $options['exchange'];
            $this->channel->exchange_declare(
                $exchangeOptions['name'],
                isset($exchangeOptions['type']) ? $exchangeOptions['type'] : 'direct',
                isset($exchangeOptions['passive']) ? (bool)$exchangeOptions['passive'] : false,
                isset($exchangeOptions['durable']) ? (bool)$exchangeOptions['durable'] : false,
                isset($exchangeOptions['autoDelete']) ? (bool)$exchangeOptions['autoDelete'] : false
            );
            $this->channel->queue_bind($this->name, $exchangeOptions['name']);
        }

        $passive = isset($options['passive']) ? (bool)$options['passive'] : false;
        $durable = isset($options['durable']) ? (bool)$options['durable'] : false;
        $exclusive = isset($options['exclusive']) ? (bool)$options['exclusive'] : false;
        $autoDelete = isset($options['autoDelete']) ? (bool)$options['autoDelete'] : false;
        $arguments = isset($options['arguments']) ? new AMQPTable($options['arguments']) : null;

        $this->channel->queue_declare($this->name, $passive, $durable, $exclusive, $autoDelete, false, $arguments);
    }

    /**
     * @inheritdoc
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     */
    public function submit($payload, array $options = [])
    {
        return $this->queue($payload, $options);
    }

    /**
     * @inheritdoc
     */
    public function waitAndTake($timeout = null)
    {
        return $this->dequeue(true, $timeout);
    }

    /**
     * @inheritdoc
     */
    public function waitAndReserve($timeout = null)
    {
        return $this->dequeue(false, $timeout);
    }

    /**
     * @inheritdoc
     */
    public function release($messageId, array $options = [])
    {
        throw new Exception('Not implemented');
    }

    /**
     * @inheritdoc
     */
    public function abort($messageId)
    {
        $this->channel->basic_nack($messageId);
    }

    /**
     * @inheritdoc
     */
    public function finish($messageId)
    {
        $this->channel->basic_ack($messageId);
    }

    /**
     * @inheritdoc
     * NOTE: The beanstalkd implementation only supports to peek the UPCOMING job, so this will throw an exception for $limit != 1.
     *
     * @throws JobQueueException
     */
    public function peek($limit = 1)
    {
        throw new Exception('Not implemented');
    }

    /**
     * @inheritdoc
     */
    public function count()
    {
        return (int)$this->channel->queue_declare($this->name, true)[1];
    }

    /**
     * @return void
     */
    public function setUp()
    {

    }

    /**
     * @inheritdoc
     */
    public function flush()
    {
        $this->channel->queue_purge($this->name);
    }


    public function shutdownObject()
    {
        $this->channel->close();
        $this->connection->close();
    }

    protected function queue($payload, array $options = [])
    {
        $correlationIdentifier = \uniqid('', true);
        $message = new AMQPMessage(json_encode($payload), [
            'correlation_identifier' => $correlationIdentifier
        ]);
        $this->channel->basic_publish($message, '', $this->name);
        return $correlationIdentifier;
    }

    protected function dequeue($ack = true, $timeout = null)
    {
        $cache = null;
        $consumerTag = $this->channel->basic_consume($this->name, '', false, false, false, false, function (AMQPMessage $message) use(&$cache, $ack) {
            $deliveryTag = (string)$message->delivery_info['delivery_tag'];
            if ($ack) {
                $this->channel->basic_ack($deliveryTag);
            }
            $cache = new Message($deliveryTag, json_decode($message->body, true));
        });

        while ($cache === null) {
            $this->channel->wait(null, false, $timeout ?: 0);
        }

        $this->channel->basic_cancel($consumerTag);
        return $cache;
    }
}
