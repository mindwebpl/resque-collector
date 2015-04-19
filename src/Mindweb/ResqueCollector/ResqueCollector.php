<?php
namespace Mindweb\ResqueCollector;

use Resque;
use Resque_Event;
use Resque_Job;
use Resque_Job_DontPerform;
use Resque_Worker;
use Mindweb\Modifier;
use Mindweb\Forwarder;
use Mindweb\Collector;

class ResqueCollector implements Collector\Collector
{
    /**
     * @var string
     */
    private $host;

    /**
     * @var string
     */
    private $port;

    /**
     * @var string
     */
    private $queue;

    /**
     * @var int
     */
    private $logLevel;

    /**
     * @var int
     */
    private $interval;

    /**
     * @param array $configuration
     */
    public function __construct(array $configuration)
    {
        $this->host = !empty($configuration['host']) ? $configuration['host'] : 'localhost';
        $this->port = !empty($configuration['host']) ? $configuration['port'] : '6379';
        $this->queue = !empty($configuration['host']) ? $configuration['queue'] : 'preAnalytics';
        $this->logLevel = !empty($configuration['host']) ? $configuration['logLevel'] : 0;
        $this->interval = !empty($configuration['interval']) ? $configuration['interval'] : 10;
    }

    /**
     * @param Modifier\Collection $modifiers
     * @param Forwarder\Collection $forwarders
     */
    public function run(Modifier\Collection $modifiers, Forwarder\Collection $forwarders)
    {
        Resque::setBackend($this->host . ':' . $this->port);

        $worker           = new Resque_Worker($this->queue);
        $worker->logLevel = $this->logLevel;

        Resque_Event::listen('beforePerform', function (Resque_Job $job) use ($modifiers, $forwarders) {
            $jobInstance = $job->getInstance();

            if ($jobInstance instanceof Job) {
                $jobInstance->setModifiers($modifiers);
                $jobInstance->setForwarders($forwarders);
            } else {
                throw new Resque_Job_DontPerform();
            }
        });

        $worker->work(10);
    }
}