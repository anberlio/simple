<?php
declare(strict_types=1);

namespace ChaShaoEs\Model;

use ChaShaoEs\Query\Builder;
use ChaShaoEs\Client;
use Hyperf\Context\ApplicationContext;
use Hyperf\Collection\Collection;
use Hyperf\Contract\Arrayable;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\Jsonable;
use Elasticsearch\Client as EsClient;
use Hyperf\Stringable\Str;
use JsonSerializable;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use function Hyperf\Support\call;

abstract class Model implements Arrayable, Jsonable, JsonSerializable
{
    use HasAttributes;

    protected string $index; //索引
    protected Client $client;
    protected string $connection = 'ChaShao';

    protected string $prefixFormat='%s%s';

    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function __construct()
    {
        $this->client = ApplicationContext::getContainer()->get(Client::class);
    }

    /**
     *
     * @return Builder
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public static function query(): Builder
    {
        return (new static())->newQuery();
    }

    /**
     * @return Builder
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function newQuery(): Builder
    {

        $self = $this->newModelBuilder()->setModel($this);
        try {
            $prefix=$this->getPrefix();
            //注意这里只能用于查询
            if(Str::contains($this->getIndex(),',')){
                $indexes=array_filter(explode(',',$this->getIndex()));
                foreach ($indexes as $key=>$index){
                    $index=Str::lower(sprintf($this->prefixFormat,$prefix,$index));
                    //检查单个索引
                    $this->setIndex($index);
                    //检查索引是否存在，不存在则创建
                    if (!$self->existsIndex()) {
                        $self->createIndex();
                    }
                    $indexes[$key]=$index;
                }
                //充值当前索引
                $this->setIndex(implode(',',$indexes));
            }else{
                $index=$this->getIndex();
                $index=Str::lower(sprintf($this->prefixFormat,$prefix,$index));
                $this->setIndex($index);
                //检查索引是否存在，不存在则创建
                if (!$self->existsIndex()) {
                    $self->createIndex();
                }
            }
        } catch (\Exception $e) {
            echo '当前的连接错误:'.$e->getMessage();
        }

        return $self;
    }


    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public  function getPrefix():string{
        $config=self::getContainer()->get(ConfigInterface::class)->get($this->connection);
        if(empty($config['prefix'])) return '';
        if(Str::contains($config['prefix'],'_')){
            return $config['prefix'];
        }
        return $config['prefix'].'_';
    }

    /**
     * 获取container
     * @return ContainerInterface
     */
    public static function getContainer(): ContainerInterface
    {
       return   ApplicationContext::getContainer();
    }

    /**
     * @return EsClient
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function getClient(): EsClient
    {
        return $this->client->create($this->connection);
    }

    /**
     * Create a new Model Collection instance.
     * @param array $models
     * @return Collection
     */
    public function newCollection(array $models = []): Collection
    {
        return new Collection($models);
    }

    /**
     * @return $this
     */
    public  function newInstance(): self
    {
        return new static();
    }

    public static  function getInstance():self{
        return new static();
    }

    /**
     * Create a new Model query builder
     * @return Builder
     */
    public function newModelBuilder(): Builder
    {
        return new Builder();
    }

    /**
     * @return string
     */
    public function getIndex(): string
    {
        return $this->index;
    }

    /**
     * @param string $index
     */
    public function setIndex(string $index): void
    {
        $this->index = $index;
    }

    /**
     *  Handle dynamic method calls into the model.
     * @param string $method
     * @param array $parameters
     * @return mixed|null
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function __call(string $method, array $parameters)
    {
        return call([$this->newQuery(), $method], $parameters);
    }

    /**
     * Handle dynamic static method calls into the method.
     * @param string $method
     * @param array $parameters
     * @return mixed
     */
    public static function __callStatic(string $method, array $parameters)
    {
        return (new static())->{$method}(...$parameters);
    }
}
