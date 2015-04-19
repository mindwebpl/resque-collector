<?php
namespace Mindweb\ResqueCollector;

use Mindweb\Modifier;
use Mindweb\Forwarder;

class Job
{
    /**
     * @var array
     */
    public $args = array();

    /**
     * @var Modifier\Collection
     */
    private $modifiers;

    /**
     * @var Forwarder\Collection
     */
    private $forwarders;

    /**
     * @param Modifier\Collection $modifiers
     */
    public function setModifiers(Modifier\Collection $modifiers)
    {
        $this->modifiers = $modifiers;
    }

    /**
     * @param Forwarder\Collection $forwarders
     */
    public function setForwarders(Forwarder\Collection $forwarders)
    {
        $this->forwarders = $forwarders;
    }

    public function perform()
    {
        $this->forwarders->forward(
            $this->modifiers->modify(
                $this->args
            )
        );
    }
} 