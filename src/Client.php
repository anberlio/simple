<?php
declare(strict_types=1);

namespace ChaShaoEs;
use Elasticsearch\ClientBuilder;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ContainerInterface;
use Elasticsearch\Client as EsClient;
use Hyperf\Guzzle\RingPHP\PoolHandler;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Swoole\Coroutine;

/**
 * ES客户端
 */
class Client
{
    protected ContainerInterface $container;
    protected array $config;

    /**
     * @param ContainerInterface $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @param string $connection
     * @return EsClient
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function create(string $connection='default'):EsClient
    {
        $config=  $this->container->get(ConfigInterface::class)->get($connection, []);
        $builder = ClientBuilder::create();
        if (Coroutine::getCid() > 0) {
            $handler = make(PoolHandler::class, [
                'option' => [
                    'max_connections' => $config['connections'],
                ],
            ]);
            $builder->setHandler($handler);
        }
        $client = $builder->setHosts([$config['hosts']])->build();
        $logger = $this->container->get(LoggerFactory::class)->get('es','default');
        $logger->info('elasticsearch-logger',$client->info());
        return $client;

    }

}
