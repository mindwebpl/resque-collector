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
        $this->port = !empty($configuration['port']) ? $configuration['port'] : '6379';
        $this->queue = !empty($configuration['queue']) ? $configuration['queue'] : 'preAnalytics';
        $this->logLevel = !empty($configuration['logLevel']) ? $configuration['logLevel'] : 0;
        $this->interval = !empty($configuration['interval']) ? $configuration['interval'] : 5;
        $this->numberOfWorkers = !empty($configuration['numberOfWorkers']) ? $configuration['numberOfWorkers'] : 1;
    }

    /**
     * @param Modifier\Collection $modifiers
     * @param Forwarder\Collection $forwarders
     */
    public function run(Modifier\Collection $modifiers, Forwarder\Collection $forwarders)
    {
        Resque::setBackend($this->host . ':' . $this->port);

        if ($this->numberOfWorkers === 1) {
            $this->runWorker($modifiers, $forwarders);
        } elseif ($this->numberOfWorkers > 1) {
            for ($i = 0; $i < $this->numberOfWorkers; ++$i) {
                $pid = pcntl_fork();
                if ($pid == -1) {
                    throw new \RuntimeException('Could not fork.');
                } else if ($pid) {
                    throw new \RuntimeException('Could not fork. (zombie children)');
                } else {
                    $this->runWorker($modifiers, $forwarders);
                }
            }
        }
    }

    /**
     * @param Modifier\Collection $modifiers
     * @param Forwarder\Collection $forwarders
     */
    private function runWorker(Modifier\Collection $modifiers, Forwarder\Collection $forwarders)
    {
        $worker = new Resque_Worker($this->queue);
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

        $worker->work($this->interval);
    }
}