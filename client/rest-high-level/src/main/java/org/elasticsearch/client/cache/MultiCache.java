/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.cache;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *  抽象缓存类，未提供保证上下级缓存数据一致性方法
 *  不主动提供异步操作
 *  在MultiCacheBuilder中显示指定支持async才会保证异步有效
 * @param <K>
 * @param <V>
 */
public abstract class MultiCache<K, V> implements AsyncCache<K, V>, Closeable {

    private final static Logger logger = LoggerFactory.getLogger(MultiCache.class);

    // 直接下层的缓存
    private BasisCache<K,V> nextLevelCache;

    // 是否可执行异步操作
    private boolean isAsync = false;

    // 缓存是否被关系和清理
    protected boolean closed = false;

    // 缓存支持异步操作但其下级不支持异步操作
    protected ExecutorService executorService;

    MultiCache(MultiCacheBuilder<?, ? super K, ? super V> builder){
        if (builder.hasNextLevelCacheBuilder()){
            nextLevelCache = (BasisCache<K, V>) builder.getNextLevelCacheBuilder().build();
        }
        isAsync = builder.isAsync();
        if (isAsync){
            executorService = Executors.newScheduledThreadPool(builder.getThreadPoolCoreSize());
        }
    }

    @Override
    public boolean isAsync() {
        return isAsync;
    }

    @Override
    public void close(){
        try {
            closed = true;
            if (Objects.nonNull(executorService)){
                closeExecutor();
            }
            if (Objects.nonNull(nextLevelCache) && nextLevelCache instanceof Closeable){
                ((Closeable) nextLevelCache).close();
            }
            nextLevelCache = null;
        }catch (Exception e){
            logger.error("exception occurs when closing cache", e);
        }
    }

    private void closeExecutor() throws InterruptedException{
        executorService.shutdown();
        if (!executorService.awaitTermination(20, TimeUnit.SECONDS)){
            executorService.shutdownNow();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)){
                logger.warn("waiting for shutdown executor time out");
            }
        }
    }

    /**
     * 缓存执行同步添加操作，其下所有缓存均执行异步添加操作
     * @param key
     * @param value
     */
    public abstract void putWithLowerLayerAsync(K key, V value);

    /**
     * 缓存执行同步删除操作，其下所有缓存均执行异步删除操作
     * @param key
     */
    public abstract void inValidateWithLowerLayerAsync(K key);

    /**
     *  本层缓存无法获取目标键值，去下层缓存中同步拉取并存储到本层缓存中
     * @param key
     * @return
     */
    protected abstract V load(K key);

    protected V getFromNextLayer(K key){
        if (Objects.nonNull(nextLevelCache)){
            return nextLevelCache.get(key);
        }
        return null;
    }

    /**
     * 对应键值放在下级缓存中
     * @param key
     * @param value
     */
    protected void putToNextLayer(K key, V value){
        if (Objects.isNull(nextLevelCache)){
            return;
        }
        if (nextLevelCache instanceof AsyncCache && ((AsyncCache)nextLevelCache).isAsync()){
            ((AsyncCache<K, V>) nextLevelCache).addAsync(key, value);
        }else {
            executorService.execute(() -> nextLevelCache.put(key, value));
        }
    }

    /**
     *  异步将对应键值存储到下层缓存中
     * @param key
     * @param value
     */
    protected void putToNextLayerAsync(K key, V value){
        if (Objects.isNull(nextLevelCache)){
            return;
        }else if (nextLevelCache instanceof AsyncCache && ((AsyncCache<K, V>) nextLevelCache).isAsync()){
            ((AsyncCache<K, V>) nextLevelCache).addAsync(key,value);
        }else {
            executorService.execute(() -> nextLevelCache.put(key, value));
        }
    }

    /**
     * 使下级缓存中对应键值失效
     * @param key
     */
    public void inValidateOfNextLayer(K key){
        if (Objects.nonNull(nextLevelCache)){
            nextLevelCache.inValidate(key);
        }
    }

    /**
     *  将下级缓存中对应键值无效
     * @param key
     */
    protected void inValidateOfNextLayerAsync(K key){
        if (Objects.isNull(nextLevelCache)) {
            return;
        }
        if (nextLevelCache instanceof AsyncCache && ((AsyncCache<K, V>) nextLevelCache).isAsync()){
            ((AsyncCache<K, V>) nextLevelCache).inValidateAsync(key);
        }else {
            executorService.execute(() -> nextLevelCache.inValidate(key));
        }
    }

    /**
     *  检查异步操作需要条件是否全部具备，如果不具备会抛出异常
     */
    protected void checkAsyncSupport(){
        if (!isAsync){
            throw new AsyncException("async status is not set, can not execute async operations");
        }
        if (Objects.isNull(executorService)){
            throw new AsyncException("async executor has not bean initialized");
        }
    }

    /**
     *  检查缓存本身是否处于可用状态，
     *  如果缓存已关闭，会抛出异常
     */
    protected void checkCacheOpen(){
        if (closed){
            throw new CacheClosedException("cache has bean closed, can not execute any operations");
        }
    }

    /**
     * 检查各个目标对象是否均为非空对象
     */
    protected void checkNotNull(Object... objects){
        if (ArrayUtils.isEmpty(objects)) return;
        for (Object object : objects) {
            if (Objects.isNull(object)){
                throw new NullPointerException("object does not pass non-null check");
            }
        }
    }

    protected boolean hasNextLayer(){
        return nextLevelCache != null;
    }

}
