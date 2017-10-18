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

        $passive = isset($options['passive']) ? (bool)$options['passive'] : false;
        $durable = isset($options['durable']) ? (bool)$options['durable'] : false;
        $exclusive = isset($options['exclusive']) ? (bool)$options['exclusive'] : false;
        $autoDelete = isset($options['autoDelete']) ? (bool)$options['autoDelete'] : false;

        $this->channel->queue_declare($this->name, $passive, $durable, $exclusive, $autoDelete);
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
        $message = new AMQPMessage(json_encode($payload));
        $this->channel->basic_publish($message);
        return '';
    }

    /**
     * @inheritdoc
     */
    public function waitAndTake($timeout = null)
    {
        $timeStart = microtime(true);
        do {
            /** @var AMQPMessage|null $message */
            $message = $this->channel->basic_get($this->name, true);
            $timeSpent = microtime(true) - $timeStart;
        } while ($timeSpent > $timeout && $message === null);

        if ($message === null) {
            return null;
        }

        return new Message((string)$message->delivery_info['delivery_tag'], json_decode($message->body, true));
    }

    /**
     * @inheritdoc
     */
    public function waitAndReserve($timeout = null)
    {
        $timeStart = microtime(true);
        do {
            /** @var AMQPMessage|null $message */
            $message = $this->channel->basic_get($this->name, false);
            $timeSpent = microtime(true) - $timeStart;
        } while ($timeSpent > $timeout && $message === null);

        if ($message === null) {
            return null;
        }

        return new Message((string)$message->delivery_info['delivery_tag'], json_decode($message->body, true));
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
}
