<?php
declare(strict_types=1);

namespace ChaShaoEs\Query;

use Hyperf\Contract\ConfigInterface;
use ChaShaoEs\Exception\LogicException;
use ChaShaoEs\Model\Model;
use ChaShaoEs\Utils\Arr;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Paginator\LengthAwarePaginator;
use Hyperf\Context\ApplicationContext;
use Hyperf\Codec\Json;
use Hyperf\Collection\Collection;
use Elasticsearch\Client;
use Hyperf\Stringable\Str;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Psr\Log\LoggerInterface;
use Psr\Container\ContainerInterface;
use Psr\SimpleCache\CacheInterface;

use Psr\SimpleCache\InvalidArgumentException;
use function Hyperf\Support\make;
use  function Hyperf\Collection\collect;
class Builder
{
    protected Client $client;
    protected LoggerInterface $logger;
    protected ContainerInterface $container;
    protected ConfigInterface $config;
    protected CacheInterface $cache;
    protected array $query;
    protected array $aggs;
    protected array $highlight = []; //高亮查询字段
    protected array $searchAfter = []; //searchAfter分页方式，上次最后一项sort数据
    protected array $sql;
    protected array $sort = [];
    protected Model $model;
    protected int $take = 0;
    protected array $operate = [
        '=', '>', '<', '>=', '<=', '!=', 'in',
        'between', 'match_phrase', 'match', 'multi_match',
        'term', 'regexp', 'prefix', 'wildcard', 'exists','terms'
    ];

    /**
     * @throws NotFoundExceptionInterface
     * @throws ContainerExceptionInterface
     */
    public function __construct()
    {
        $this->container = ApplicationContext::getContainer();
        $this->config = $this->container->get(ConfigInterface::class);
        $this->cache = $this->container->get(CacheInterface::class);
        $this->logger = $this->container->get(LoggerFactory::class)->get('elasticsearch', 'default');
    }

    /**
     * 分页查询数据
     * @param int $page
     * @param int $size
     * @param array $fields
     * @param bool $deep 深度分页 searchAfter 方式，page为1会返回第一页
     * @return LengthAwarePaginator
     * @throws InvalidArgumentException
     */
    public function page(int $page = 1, int $size = 50, array $fields = ['*'], bool $deep = false): LengthAwarePaginator
    {
        $from = 0;

        if ($deep) {
            if (empty($this->sort)) {
                throw new LogicException('page method deep attribute must be used in conjunction with orderBy, which needs to be used in conjunction with a set of sorted values from the previous page.', 400);
            }

            $cacheKey = $this->getCacheKey($size);
            $lastSorted = $this->cache->get($cacheKey);
            if ($lastSorted && $page > 1) {
                $this->searchAfter = unserialize($lastSorted, ['allowed_classes' => false]);
            }
        } else {
            $from = floor($page - 1) * $size;
        }

        if (empty($this->query)) {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'version' => true,
                'seq_no_primary_term' => true,
                'from' => $from,
                'size' => $size,
                'body' => [
                    '_source' => [
                        'includes' => $fields
                    ],
                    'track_total_hits'=>true,//获取到实际的条数
                    'query' => [
                        'match_all' => new \stdClass()
                    ],
                    'search_after' => $this->searchAfter,
                    'highlight' => $this->highlight,
                    'sort' => $this->sort
                ]
            ];
        } else {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'version' => true,
                'seq_no_primary_term' => true,
                'from' => $from,
                'size' => $size,
                'body' => [
                    '_source' => [
                        'includes' => $fields
                    ],
                    'track_total_hits'=>true,//获取到实际的条数
                    'query' => $this->query,
                    'search_after' => $this->searchAfter,
                    'highlight' => $this->highlight,
                    'sort' => $this->sort
                ]
            ];
        }

        $this->sql['body'] = array_filter($this->sql['body']);
        $result = $this->client->search($this->sql);
        $original = $result['hits']['hits'] ?? [];
        $total = $result['hits']['total']['value'] ?? 0;

        //after_search分页方式
        if ($deep) {
            $lastItem = end($original);
            $lastSorted = $lastItem['sort'] ?? [];
            if ($lastSorted) {
                $this->cache->set($cacheKey, serialize($lastSorted));
            }
        }

        $collection = Collection::make($original)->map(function ($value) use ($fields) {
            $attributes = $value['_source'] ?? [];
            if ($attributes) {
                if ($fields === ['*'] || in_array('id', $fields, true)) {
                    $attributes['id'] = is_numeric($value['_id']) ? (int)$value['_id'] : $value['_id'];
                }
            }
            $model = $this->model->newInstance();
            //处理高亮结果
            if (isset($value['highlight'])) {
                foreach ($value['highlight'] as $name => $val) {
                    if (Str::contains($name, '.')) {
                        $name = explode('.', $name)[0];
                    }
                    $attributes[$name] = $val[0];
                }
            }
            $model->setAttributes($attributes);
            $model->setOriginal($value);
            return $model;
        });

        return make(LengthAwarePaginator::class, ['items' => $collection, 'total' => $total, 'perPage' => $size, 'currentPage' => $page]);
    }


    public function getCondition():array
    {
        return $this->query;
    }

    /**
     * @param array $fields
     * @param int $size
     * @return Collection|null
     */
    public function get(array $fields = ['*'], int $size = 50): Collection|null
    {
        if (empty($this->query)) {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'version' => true,
                'seq_no_primary_term' => true,
                'from' => 0,
                'size' => $this->take > 0 ? $this->take : $size,
                'body' => [
                    '_source' => [
                        'includes' => $fields
                    ],
                    'query' => [
                        'match_all' => new \stdClass()
                    ],
                    'highlight' => $this->highlight,
                    'sort' => $this->sort
                ]
            ];
        } else {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'version' => true,
                'seq_no_primary_term' => true,
                'from' => 0,
                'size' => $this->take > 0 ? $this->take : $size,
                'body' => [
                    '_source' => [
                        'includes' => $fields
                    ],
                    'query' => $this->query,
                    'highlight' => $this->highlight,
                    'sort' => $this->sort
                ]
            ];
        }
        $this->sql['body'] = array_filter($this->sql['body']);
        $result = $this->client->search($this->sql);

        $original = $result['hits']['hits'] ?? [];
        return Collection::make($original)->map(function ($value) use ($fields) {
            $attributes = $value['_source'] ?? [];
            if ($attributes) {
                if ($fields === ['*'] || in_array('id', $fields, true)) {
                    $attributes['id'] = is_numeric($value['_id']) ? (int)$value['_id'] : $value['_id'];
                }
            }
            $model = $this->model->newInstance();
            //处理高亮结果
            if (isset($value['highlight'])) {
                foreach ($value['highlight'] as $name => $val) {
                    if (Str::contains($name, '.')) {
                        $name = explode('.', $name)[0];
                    }
                    $attributes[$name] = $val[0];
                }
            }
            $model->setAttributes($attributes);
            $model->setOriginal($value);
            return $model;
        });
    }

    /**
     * @param array $fields
     * @return Model|null
     */
    public function first(array $fields = ['*']): Model|null
    {
        return $this->take(1)->get($fields)?->first();
    }

    /**
     * 查找单条文档
     * @param string|int $id
     * @return Model|null
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function find(string|int $id): Model|null
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id' => $id
        ];
        $result=$this->model->getClient()->get( $this->sql);
        $attributes = $result['_source'] ?? [];
        $id = $result['_id'] ?? 0;
        if ($attributes && $id) {
            $attributes['id'] = is_numeric($id) ? (int)$id : $id;
        }
        $this->model->setAttributes($attributes);
        $this->model->setOriginal($result);
        return $this->model;
    }

    /**
     * 匹配项数
     * @return int
     */
    public function count(): int
    {
        if (empty($this->query)) {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'body' => [
                    'query' => [
                        'match_all' => new \stdClass()
                    ]
                ]
            ];
        } else {
            $this->sql = [
                'index' => $this->model->getIndex(),
                'body' => [
                    'query' => $this->query
                ]
            ];
        }
        $this->sql['body'] = array_filter($this->sql['body']);
        try {
            $result = $this->client->count($this->sql);
        }catch (\Throwable |\Exception $e){
            $this->logger->error('delete',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new LogicException($e->getMessage(),$e->getCode());
        }
        return (int)($result['count'] ?? 0);
    }

    /**
     * 递减字段值
     * @param string $field
     * @param int $count
     * @return bool
     */
    public function increment(string $field, int $count = 1): bool
    {
        $result = $this->updateByQueryScript("ctx._source.$field += params.count", [
            'count' => $count
        ]);
        return $result['updated'] > 0;
    }

    /**
     * 递减字段值
     * @param string $field
     * @param int $count
     * @return bool
     */
    public function decrement(string $field, int $count = 1): bool
    {
        $result = $this->updateByQueryScript("ctx._source.$field -= params.count", [
            'count' => $count
        ]);
        return $result['updated'] > 0;
    }

    /**
     * 根据查询条件检查是否存在数据
     * @return bool
     */
    public function exists(): bool
    {
        if (empty($this->query)) {
            throw new LogicException('Missing query criteria.');
        }

        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'query' => $this->query
            ]
        ];

        $this->sql['body'] = array_filter($this->sql['body']);
        try {
         $result=$this->client->count($this->sql);
        }catch (\Throwable|\Exception $e){
            $this->logger->error('delete',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new LogicException($e->getMessage(),$e->getCode());
        }
        return (bool)($result['count'] ?? 0);
    }

    /**
     * 按查询条件删除文档
     * @return bool
     */
    public function delete(): bool
    {
        if (empty($this->query)) {
            throw new LogicException('Missing query criteria.');
        }

        $this->sql = [
            'index' => $this->model->getIndex(),
            'conflicts' => 'proceed', //如果按查询删除命中版本冲突，默认值为abort
            'refresh' => true, //Elasticsearch 会刷新 请求完成后通过查询删除
            'slices' => 5, //此任务应划分为的切片数。 默认值为 1，表示任务未切片为子任务
            'body' => [
                'query' => $this->query
            ]
        ];
        try {
            $result = $this->client->deleteByQuery($this->sql);
        }catch (\Throwable |\Exception $e){
            $this->logger->error('delete',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new LogicException($e->getMessage(),$e->getCode());
        }
        return isset($result['deleted']) && $result['deleted'] > 0;
    }

    /**
     * 按查询条件更新数据
     * @param array $value
     * @return bool
     */
    public function update(array $value): bool
    {
        if (empty($this->query)) {
            throw new LogicException('Missing query criteria.', 400);
        }

        if (empty($value) || is_numeric(array_key_first($value))) {
            throw new LogicException('Data cannot be empty and can only be non-numeric subscripts.', 400);
        }

        $params = [];
        $script = '';
        foreach ($value as $field => $val) {
            $script = "ctx._source.$field = params.$field;" . $script;
            $params[$field] = $val;
        }
        try {
            $result = $this->updateByQueryScript($script, $params);
        }catch (\Throwable|\Exception $e){
            $this->logger->error('update',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new LogicException($e->getMessage(),$e->getCode());
        }
        return isset($result['updated']) && $result['updated'] > 0;
    }


    /**
     * @param string $script
     * @param array $params
     * @return callable|array
     */
    public function updateByQueryScript(string $script, array $params = []): callable|array
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                "script" => [
                    "source" => $script,
                    'lang' => 'painless',
                    "params" => $params
                ],
                'query' => $this->query
            ]
        ];
        try {
            return $this->client->updateByQuery($this->sql);
        }catch (\Throwable|\Exception $e){
            $this->logger->error('updateByQueryScript',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new  LogicException($e->getMessage(),$e->getCode());
        }
    }

    /**
     * 拿多少条数据
     * @param int $take
     * @return $this
     */
    public function take(int $take): Builder
    {
        $this->take = $take;
        return $this;
    }


    public function  withAggs(string $aggs,string $field,string $alis):Builder{
        if(empty($aggs) || empty($field) || empty($alis)){
            throw  new LogicException('抱歉查询错误');
        }
        $this->aggs[$alis]=[
            $aggs=>[
                'field'=>$field
            ]
        ];
        return $this;
    }

    /**
     * ElasticSearch 聚合查询
     * @return Collection|null
     */
    public function aggs():Collection|null{
        if(empty($this->aggs)) {
            throw  new  LogicException('Missing aggs criteria');
        };
        if(empty($this->query)){
            $this->sql=[
                'index' => $this->model->getIndex(),
                'version' => true,
                'size' => 0,
                'body'=>[
                    'aggs'=>$this->aggs
                ]
            ];
        }else{
            $this->sql=[
                'index' => $this->model->getIndex(),
                'version' => true,
                'size' => 0,
                'body'=>[
                    'query'=>$this->query,
                    'aggs'=>$this->aggs,
                ]
            ];
        }
        $this->sql['body'] = array_filter($this->sql['body']);
        try {
            $result   = $this->client->search($this->sql);
            $original =$result['aggregations']??[];
            return Collection::make($original)->map(function ($value){
                return round($value['value'],4);
            });
        }catch (\Exception |\Throwable $e){
            $this->logger->error('insert',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw new LogicException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * 获取完整查询的唯一缓存键。
     * @param int $size
     * @return string
     */
    public function getCacheKey(int $size): string
    {
        return $this->config->get('cache.default.prefix') . ':' . $this->generateCacheKey($size);
    }

    /**
     * 为查询生成唯一的缓存key
     * @param int $size
     * @return string
     */
    public function generateCacheKey(int $size): string
    {
        $query = empty($this->query) ? ['match_all' => new \stdClass()] : $this->query;
        return md5(Json::encode($query) . $this->model->getIndex() . $size);
    }

    /**
     * 批量插入文档，注意如果包含id字段，多次执行相同数据插入则只会执行更新操作，而非新增数据
     * @param array $values 二维数组
     * @return Collection
     */
    public function insert(array $values): Collection
    {
        $body = [];
        foreach ($values as $value) {
            $index = array_filter([
                '_index' => $this->model->getIndex(),
                '_id' => $value['id'] ?? null
            ]);
            $body['body'][] = [
                'index' => $index
            ];
            $body['body'][] = $value;
        }
        $this->sql = $body;
        try {
            $result = $this->client->bulk($this->sql);
            if(empty($result['items']) || $result['errors']){
                $this->logger->error('批量插入错误insertErrorInside',[
                    'message'=>$result,
                ]);
                throw  new  LogicException('批量插入错误');
            }
        }catch (\Throwable|\Exception $e){
            $this->logger->error('insert',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new  LogicException($e->getMessage(),$e->getCode());
        }
        return collect($result['items'])->map(function ($value, $key) use ($values) {
            $items = Arr::mergeArray($values[$key], ['id' => $value['index']['_id'] ?? null]);
            $model = $this->model->newInstance();
            if ($value['index']['result'] === 'created' || $value['index']['result'] === 'updated') {
                $model->setAttributes($items);
                $model->setOriginal($value);
                return $model;
            }
            return false;
        });
    }

    /**
     * 创建文档
     * @param array $value 注意如果存在id字段，数据又是一样的多次调用只会执行更新操作
     * @return Model
     */
    public function create(array $value): Model
    {
        $body = Arr::except($value, ['routing', 'timestamp']);
        $except = Arr::only($value, ['id', 'routing', 'timestamp']);
        $this->sql = Arr::mergeArray($except, [
            'index' => $this->model->getIndex(),
            'body' => $body
        ]);

        try {
            $result=$this->client->index($this->sql);
            if (!empty($result['result']) && $result['result'] === 'created') {
                $this->model->setOriginal((array)$result);
                $this->model->setAttributes(Arr::mergeArray($body, ['id' => $result['_id'] ?? '']));
            }
        } catch (\Exception $e) {
            // eg. network error like NoNodeAvailableException
            $this->logger->error('Elasticsearch create operation exception, ' . $e->getMessage() . ', index:' . $this->model->getIndex());
            throw new LogicException($e->getMessage(), $e->getCode());
        }
        return $this->model;
    }

    /**
     * 按id更新文档
     * @param array $value
     * @param string|int $id
     * @return int|string|false
     */
    public function updateById(array $value, string|int $id): int|string|false
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id' => $id,
            'body' => [
                'doc' => $value,
            ]
        ];
        try {
            $result=$this->client->update($this->sql);
            if (!empty($result['result']) && ($result['result'] === 'updated' || $result['result'] === 'noop')) {
                return $result['_id'] ?? false;
            }
        } catch (\Throwable |\Exception $e) {
            if($result=Json::decode($e->getMessage())){
                if(isset($result['status']) &&  $result['status']==404){
                    return false;
                }
            }
            $this->logger->error('insert',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw new LogicException($e->getMessage(), $e->getCode());
        }
        return false;
    }

    /**
     * 按id删除文档
     * @param string|int $id
     * @return bool
     */
    public function deleteById(string|int $id): bool
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id' => $id,
        ];
        try {
            $result=$this->client->delete($this->sql);
        } catch (\Throwable |\Exception $e) {
            if($errData=Json::decode($e->getMessage())){
                return $errData['result']=='not_found';
            }
            throw new LogicException($e->getMessage(), $e->getCode());
        }
        return !empty($result['result']) && $result['result'] === 'deleted';
    }

    /**
     * 更新映射
     * @param array $mappings
     * @return bool
     */
    public function updateIndexMapping(array $mappings): bool
    {
        $mappings = collect($mappings)->map(function ($value, $key) {
            $valued = [];
            if (is_string($value)) {
                $valued['type'] = $value;
            }
            if (is_array($value)) {
                $valued = $value;
            }
            return $valued;
        })->toArray();

        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'properties' => array_filter($mappings)
            ]
        ];
        $result=$this->client->indices()->putMapping($this->sql);
        return $result['acknowledged'] ?? false;
    }

    /**
     * 更新索引设置
     * @param array $settings
     * @return bool
     */
    public function updateIndexSetting(array $settings): bool
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'settings' => $settings
            ]
        ];
        try {
            $result = $this->client->indices()->putSettings($this->sql);
        }catch (\Throwable |\Exception $e){
            $this->logger->error('insert',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw new LogicException($e->getMessage(),$e->getCode());
        }
        return $result['acknowledged'] ?? false;
    }

    /**
     *  检查索引是否存在
     * @return bool
     */
    public function existsIndex(): bool
    {
        $this->sql = ['index' => $this->model->getIndex()];
        return   $this->client->indices()->exists($this->sql);
    }

    /**
     * * 创建索引
     * @param array $mappings
     * @param array $settings
     * @return bool
     */
    public function createIndex(array $mappings = [], array $settings = []): bool
    {
        $mappings = Arr::mergeArray(
            Collection::make($this->model->getCasts())->map(function ($value, $key) {
                return $this->convertFieldType($key, $value);
            })->toArray(),
            Collection::make($mappings)->map(function ($value, $key) {
                return $this->convertFieldType($key, $value);
            })->toArray()
        );
        if ($this->existsIndex()) {
            return false; //索引已经存在
        }
        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'settings' => ['number_of_shards' => 3, ...$settings],
                'mappings' => [
                    '_source' => [
                        'enabled' => true
                    ],
                    'properties' => $mappings
                ]
            ]
        ];

        $this->sql['body'] = array_filter($this->sql['body']);
        try {
            $result=$this->client->indices()->create($this->sql);
        } catch (\Throwable |\Exception $e) {
            $this->logger->error('createIndex',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            return false;
        }
        return $result['acknowledged'] ?? false;
    }

    /**
     * 删除索引
     * @return bool
     */
    public function deleteIndex(): bool
    {
        $this->sql = [
            'index' => $this->model->getIndex()
        ];
        try {
            $result = $this->client->indices()->delete($this->sql);
        }catch (\Throwable|\Exception $e){
            $this->logger->error('deleteIndex',[
                'message'=>$e->getMessage(),
                'code'=>$e->getCode()
            ]);
            throw  new  LogicException($e->getMessage(),$e->getCode());
        }
        return $result['acknowledged'] ?? false;
    }

    /**
     * 条件查询
     * @param string $field
     * @param mixed $operate
     * @param mixed|null $value
     * @return $this
     */
    public function where(string $field, mixed $operate, mixed $value = null): Builder
    {
        if (is_null($value)) {
            $value = $operate;
            $operate = '=';
        }
        $operates = ['=', '>', '<', '>=', '<=', '!='];
        if (in_array($operate, $operates, true)) {
            $this->parseQuery($field, $operate, $value);
        } else {
            throw new LogicException('where query condition operate [' . $operate . '] illegally, Supported only [' . implode(',', $operates) . ']');
        }
        return $this;
    }

    /**
     * must字段存在索引
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-exists-query.html#exists-query-top-level-params
     * @param string $field
     * @return $this
     */
    public function whereExistsField(string $field): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'exists', '', type: 'must');
    }

    /**
     * must_not字段不存在索引
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-exists-query.html#exists-query-top-level-params
     * @param string $field
     * @return $this
     */
    public function whereNotExistsField(string $field): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'exists', '', type: 'must_not');
    }

    /**
     * should字段不存在索引
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-exists-query.html#exists-query-top-level-params
     * @param string $field
     * @return $this
     */
    public function whereShouldExistsField(string $field): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'exists', '', type: 'should');
    }

    /**
     * filter字段不存在索引
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-exists-query.html#exists-query-top-level-params
     * @param string $field
     * @return $this
     */
    public function whereFilterExistsField(string $field): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'exists', '', type: 'filter');
    }

    /**
     * must多个确切的条件满足
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-terms-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereIn(string $field, array $value): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'in', $value, type: 'must');
    }

    /**
     * must_not不能有多个确切的条件满足
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-terms-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereNotIn(string $field, array $value): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'in', $value, type: 'must_not');
    }

    /**
     * should过滤多个确切的条件满足
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-terms-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereShouldIn(string $field, array $value): Builder
    {
        return $this->parseQuery($field=='id'?'_id':$field, 'in', $value, type: 'should');
    }

    /**
     * filter过滤多个确切的条件满足
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-terms-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereFilterIn(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'in', $value, type: 'filter');
    }





    /**
     * must正则匹配
     * @param string $field
     * @param array $value
     * @return Builder
     */
    public function whereRegexp(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'regexp', $value, type: 'must');
    }

    /**
     * must_not正则匹配
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereNotRegexp(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'regexp', $value, type: 'must_not');
    }

    /**
     * should正则匹配
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereShouldRegexp(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'regexp', $value, type: 'should');
    }

    /**
     * filter正则匹配
     * @param string $field
     * @param array $value
     * @return Builder
     */
    public function whereFilterRegexp(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'regexp', $value, type: 'filter');
    }

    /**
     * must匹配短语
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query-phrase.html
     * @param string $field
     * @param mixed $value
     * @param int $slop
     * @return $this
     */
    public function whereMatchPhrase(string $field, mixed $value, int $slop = 100): Builder
    {
        return $this->parseQuery($field, 'match_phrase', $value, ['slop' => $slop], 'must');
    }

    /**
     * must_not短语匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query-phrase.html
     * @param string $field
     * @param mixed $value
     * @param int $slop
     * @return $this
     */
    public function whereNotMatchPhrase(string $field, mixed $value, int $slop = 100): Builder
    {
        return $this->parseQuery($field, 'match_phrase', $value, ['slop' => $slop], 'must_not');
    }

    /**
     * should短语匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query-phrase.html
     * @param string $field
     * @param mixed $value
     * @param int $slop
     * @return $this
     */
    public function whereShouldMatchPhrase(string $field, mixed $value, int $slop = 100): Builder
    {
        return $this->parseQuery($field, 'match_phrase', $value, ['slop' => $slop], 'should');
    }

    /**
     * filter短语匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query-phrase.html
     * @param string $field
     * @param mixed $value
     * @param int $slop
     * @return $this
     */
    public function whereFilterMatchPhrase(string $field, mixed $value, int $slop = 100): Builder
    {
        return $this->parseQuery($field, 'match_phrase', $value, ['slop' => $slop], 'filter');
    }

    /**
     * 范围查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-range-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereBetween(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'between', $value, type: 'must');
    }

    /**
     * 不包含范围
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-range-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereNotBetween(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'between', $value, type: 'must_not');
    }

    /**
     * should范围查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-range-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereShouldBetween(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'between', $value, type: 'should');
    }

    /**
     * filter范围查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-range-query.html
     * @param string $field
     * @param array $value
     * @return $this
     */
    public function whereFilterBetween(string $field, array $value): Builder
    {
        return $this->parseQuery($field, 'between', $value, type: 'filter');
    }

    /**
     * must查询指定前缀
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-prefix-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function wherePrefix(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'prefix', $value, type: 'must');
    }

    /**
     * must_not不能是指定前缀
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-prefix-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereNotPrefix(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'prefix', $value, type: 'must_not');
    }

    /**
     * should应该包含指定前缀
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-prefix-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereShouldPrefix(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'prefix', $value, type: 'should');
    }

    /**
     * filter过滤包含指定前缀
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-prefix-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereFilterPrefix(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'prefix', $value, type: 'filter');
    }

    /**
     * must通配符*号匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-wildcard-query.html
     * @param string $field
     * @param string $value
     * @return $this
     */
    public function whereWildcard(string $field, string $value): Builder
    {
        return $this->parseQuery($field, 'wildcard', $value, type: 'must');
    }

    /**
     * must_not通配符*号匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-wildcard-query.html
     * @param string $field
     * @param string $value
     * @return $this
     */
    public function whereNotWildcard(string $field, string $value): Builder
    {
        return $this->parseQuery($field, 'wildcard', $value, type: 'must_not');
    }

    /**
     * should通配符*号匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-wildcard-query.html
     * @param string $field
     * @param string $value
     * @return $this
     */
    public function whereShouldWildcard(string $field, string $value): Builder
    {
        return $this->parseQuery($field, 'wildcard', $value, type: 'should');
    }

    /**
     * filter通配符*号匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-wildcard-query.html
     * @param string $field
     * @param string $value
     * @return $this
     */
    public function whereFilterWildcard(string $field, string $value): Builder
    {
        return $this->parseQuery($field, 'wildcard', $value, type: 'filter');
    }

    /**
     * must，等同于等于，在提供的字段中包含确切术语的文档
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-term-query.html
     * @param string $field 如果为text则需要以：field.raw 格式
     * @param mixed $value
     * @return $this
     */
    public function whereTerm(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'term', $value, type: 'must');
    }

    /**
     * must_not，等同于不等于，在提供的字段中不包含确切术语的文档，
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-term-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereNotTerm(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'term', $value, type: 'must_not');
    }

    /**
     * should，等同于或等于，在提供的字段中应该包含确切术语的文档，
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-term-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereShouldTerm(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'term', $value, type: 'should');
    }

    /**
     * filter，等同于过滤等于，在提供的字段中过滤包含确切术语的文档，
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-term-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereFilterTerm(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'term', $value, type: 'filter');
    }

    /**
     * must多字段匹配查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-multi-match-query.html
     * @param array $fields
     * @param mixed $value
     * @return $this
     */
    public function whereMultiMatch(array $fields, mixed $value): Builder
    {
        return $this->parseQuery($fields, 'multi_match', $value, type: 'must');
    }

    /**
     * must_not多字段匹配查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-multi-match-query.html
     * @param array $fields
     * @param mixed $value
     * @return $this
     */
    public function whereNotMultiMatch(array $fields, mixed $value): Builder
    {
        return $this->parseQuery($fields, 'multi_match', $value, type: 'must_not');
    }

    /**
     * should多字段匹配查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-multi-match-query.html
     * @param array $fields
     * @param mixed $value
     * @return $this
     */
    public function whereShouldMultiMatch(array $fields, mixed $value): Builder
    {
        return $this->parseQuery($fields, 'multi_match', $value, type: 'should');
    }

    /**
     * filter多字段匹配查询
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-multi-match-query.html
     * @param array $fields
     * @param mixed $value
     * @return $this
     */
    public function whereFilterMultiMatch(array $fields, mixed $value): Builder
    {
        return $this->parseQuery($fields, 'multi_match', $value, type: 'filter');
    }

    /**
     * must匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereMatch(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'match', $value, type: 'must');
    }

    /**
     * must_not匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereNotMatch(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'match', $value, type: 'must_not');
    }


    /**
     * should匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereShouldMatch(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'match', $value, type: 'should');
    }

    /**
     * filter匹配
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-match-query.html
     * @param string $field
     * @param mixed $value
     * @return $this
     */
    public function whereFilterMatch(string $field, mixed $value): Builder
    {
        return $this->parseQuery($field, 'match', $value, type: 'filter');
    }

    /**
     * 地理距离查询，过滤在一定距离范围
     * https://www.elastic.co/guide/en/elasticsearch/reference/8.5/query-dsl-geo-distance-query.html
     * @param string $field
     * @param float $longitude
     * @param float $latitude
     * @param string $distance
     * @return $this
     */
    public function whereFilterDistance(string $field, float $longitude, float $latitude, string $distance = '50km'): Builder
    {
        $this->query['bool']['filter'][] = [
            'geo_distance' => [
                'distance' => $distance,//附近 km 范围内
                $field => [
                    'lat' => $latitude,
                    'lon' => $longitude
                ]
            ]
        ];
        return $this;
    }

    /**
     * parseWhere
     * @param string|array $field
     * @param string $operate
     * @param mixed $value
     * @param array $options
     * @param string $type must must_not should filter
     * @return $this
     */
    protected function parseQuery(string|array $field, string $operate, mixed $value, array $options = [], string $type = 'must'): Builder
    {
        if (!in_array($operate, $this->operate, true)) {
            throw new LogicException('where query condition operate [' . $operate . '] illegally, Supported only [' . implode(',', $this->operate) . ']');
        }

        $types = ['must', 'must_not', 'should', 'filter'];
        if (!in_array($type, $types, true)) {
            throw new LogicException('where query condition type [' . $type . '] illegally, Supported only [' . implode(',', $types) . ']');
        }

        switch ($operate) {
            //匹配，match用于执行全文查询（包括模糊匹配）的标准查询 以及短语或邻近查询。
            case 'match':
                $result = ['match' => [$field => $value]];
                break;
            //短语匹配，与match查询类似，但用于匹配确切的短语或单词邻近匹配
            case 'match_phrase':
                $result = ['match_phrase' => [$field => array_merge(['query' => $value, 'slop' => 100], $options)]];
                break;
            //多字段匹配
            case 'multi_match':
                $result = ['multi_match' => ['query' => $value, 'fields' => $field]];
                break;
            //等于 返回在提供的字段中包含确切术语的文档。您可以使用查询根据精确值查找文档，例如 价格、产品 ID 或用户名。
            case '=':
            case 'term':
                $result = ['term' => [$field => $value]];
                break;
            //不等于 返回在提供的字段中包含确切术语的文档。您可以使用查询根据精确值查找文档，例如 价格、产品 ID 或用户名。
            case '!=':
                $type = 'must_not';
                $result = ['term' => [$field => $value]];
                break;
            //大于
            case '>':
                $result = ['range' => [$field => ['gt' => $value]]];
                break;
            //小于
            case '<':
                $result = ['range' => [$field => ['lt' => $value]]];
                break;
            //大于等于
            case '>=':
                $result = ['range' => [$field => ['gte' => $value]]];
                break;
            //小于等于
            case '<=':
                $result = ['range' => [$field => ['lte' => $value]]];
                break;
            //范围
            case 'between':
                if (!isset($value[0], $value[1])) {
                    throw new LogicException('The between query value should contain start and end.', 400);
                }
                $result = ['range' => [$field => ['gte' => $value[0], 'lte' => $value[1]]]];
                break;
            //类似whereIn
            case 'in':
                $result = ['terms' => [$field => $value]];
                break;
            //正则匹配
            case 'regexp':
                $result = ['regexp' => [$field => $value]];
                break;
            //前缀匹配
            case 'prefix':
                $type = 'must';
                $result = ['prefix' => [$field => $value]];
                break;
            //通配符
            case 'wildcard':
                $result = ['wildcard' => [$field => $value]];
                break;
            //存在字段
            case 'exists':
                $result = ['exists' => ['field' => $field]];
                break;
        }
        if (isset($result)) {
            $this->query['bool'][$type][] = $result;
        }
        return $this;
    }

    /**
     * 搜索结果高亮显示
     * @param array $fields
     * @param array $preTags
     * @param array $postTag
     * @return $this
     */
    public function selectHighlight(array $fields, array $preTags = ["<em>"], array $postTag = ["</em>"]): Builder
    {
        if (empty($fields)) {
            return $this;
        }

        $fields = Collection::make($fields)
            ->map(function ($item) {
                return [
                    $item => new \stdClass()
                ];
            })->toArray();

        $this->highlight = [
            "pre_tags" => $preTags,
            "post_tags" => $postTag,
            'fields' => $fields
        ];
        return $this;
    }

    /**
     * 排序
     * @param string $field 对于text类型的字段可以用：field.raw 方式
     * @param string $direction
     * @param string $mode
     * min 选择最低值。
     * max 选择最高值。
     * sum 使用所有值的总和作为排序值。仅适用于 基于数字的数组字段。
     * avg 使用所有值的平均值作为排序值。仅适用于 对于基于数字的数组字段。
     * median 使用所有值的中位数作为排序值。仅适用于 对于基于数字的数组字段。
     * @return $this
     */
    public function orderBy(string $field, string $direction = 'asc', string $mode = 'min'): Builder
    {
        $this->sort[] = [$field => [
            'order' => strtolower($direction) === 'asc' ? 'asc' : 'desc',
            'mode' => $mode
        ]];
        return $this;
    }

    /**
     * 按用户当前经纬度距离排序
     * @param string $field
     * @param float $longitude
     * @param float $latitude
     * @param string $direction
     * @param string $unit m 或 km
     * @param string $mode
     * min 选择最低值。
     * max 选择最高值。
     * sum 使用所有值的总和作为排序值。仅适用于 基于数字的数组字段。
     * avg 使用所有值的平均值作为排序值。仅适用于 对于基于数字的数组字段。
     * median 使用所有值的中位数作为排序值。仅适用于 对于基于数字的数组字段。
     * @return $this
     */
    public function orderByDistance(string $field, float $longitude, float $latitude, string $direction = 'asc', string $unit = 'km', string $mode = 'min'): Builder
    {
        $this->sort[] = [
            '_geo_distance' => [
                $field => [
                    $latitude, //纬度
                    $longitude //经度
                ],
                'order' => strtolower($direction) === 'asc' ? 'asc' : 'desc',
                'unit' => $unit,
                'mode' => $mode,
                'distance_type' => 'arc',
                'ignore_unmapped' => true //未映射字段导致搜索失败
            ]
        ];
        return $this;
    }

    /**
     * 转换字段类型
     * @param string $key
     * @param string|array $value
     * @return array
     */
    protected function convertFieldType(string $key, string|array $value): array
    {
        $valued = [];
        $types = $this->model->getCastTypes();//映射后的字段类型
        if (is_string($value)) {
            $type = $types[$value];
            $valued['type'] = $type;

            //文本类型，做中文分词处理
            if ($type === 'text') {
                $valued['analyzer'] = 'ik_max_word';
                $valued['search_analyzer'] = 'ik_smart';
                $valued['fields'] = [
                    'raw' => [
                        'type' => 'keyword'
                    ],
                    'keyword' => [
                        'type' => 'text',
                        'analyzer' => 'keyword'
                    ],
                    'english' => [
                        'type' => 'text',
                        'analyzer' => 'english'
                    ],
                    'standard' => [
                        'type' => 'text',
                        'analyzer' => 'standard'
                    ],
                    'smart' => [
                        'type' => 'text',
                        'analyzer' => 'ik_smart'
                    ]
                ];
            }

            //日期格式
            if ($type === 'date') {
                $valued['format'] = 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis||epoch_second';
            }
            return $valued;
        }

        if (is_array($value)) {
            $valued = $value;
        }

        return $valued;
    }

    /**
     * 设置初始化模型
     * @param Model $model
     * @return $this
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function setModel(Model $model): Builder
    {
        $this->model = $model;
        $this->client = $model->getClient();
        $this->highlight = [];
        $this->sort = [];
        return $this;
    }
}
