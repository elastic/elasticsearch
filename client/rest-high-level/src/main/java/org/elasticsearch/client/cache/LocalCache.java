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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LocalCache<K, V> extends MultiCache<K, V> {

    private final static Logger logger = LoggerFactory.getLogger(LocalCache.class);

    // 执行操作的
    private LoadingCache<K, V> loadingCache;

    // 避免关闭时重复操作排它锁
    private final Object mutex = new Object();

    LocalCache(LocalCacheBuilder<? super K, ? super V> builder){
        super(builder);
        loadingCache = initLoadingCacheBuilder(builder).build(new CacheLoader<K, V>(){
            @Override
            public V load(K key) throws Exception{
                return getFromNextLayer(key);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private CacheBuilder<K, V> initLoadingCacheBuilder(LocalCacheBuilder<? super K, ? super V> localCacheBuilder) {

        CacheBuilder<K, V>  cacheBuilder = (CacheBuilder<K, V>) CacheBuilder.newBuilder();

        Long expireAfterWriteMills = localCacheBuilder.getExpireAfterWrite();

        if (expireAfterWriteMills != CacheConstant.CommonUsed.UNSET_NUMBER.longValue()){
            cacheBuilder.expireAfterWrite(expireAfterWriteMills, TimeUnit.MICROSECONDS);
        }

        Long expireAfterAccessMills = localCacheBuilder.getExpireAfterAccess();
        if (expireAfterAccessMills != CacheConstant.CommonUsed.UNSET_NUMBER.longValue()){
            cacheBuilder.expireAfterAccess(expireAfterAccessMills, TimeUnit.MILLISECONDS);
        }

        Long maximumSize = localCacheBuilder.getMaximumSize();
        if (maximumSize != CacheConstant.CommonUsed.UNSET_NUMBER.longValue()){
            cacheBuilder.maximumSize(maximumSize);
        }
        return cacheBuilder;
    }

    @Override
    public void addAsync(K key, V value) {
        checkCacheOpen();
        checkAsyncSupport();
        checkNotNull(key, value);
        executorService.execute(() -> loadingCache.put(key, value));
        putToNextLayerAsync(key, value);
    }

    @Override
    public void inValidateAsync(K key) {
        checkCacheOpen();
        checkAsyncSupport();
        checkNotNull(key);
        inValidateOfNextLayerAsync(key);
        executorService.execute(() -> inValidate(key));
    }

    @Override
    public void put(K key, V value) {
        checkCacheOpen();
        checkNotNull(key, value);
        loadingCache.put(key, value);
        putToNextLayer(key, value);
    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public void inValidate(K key) {
        checkCacheOpen();
        checkNotNull(key);
        inValidateOfNextLayer(key);
        loadingCache.invalidate(key);
    }

    @Override
    public void putWithLowerLayerAsync(K key, V value) {
        checkCacheOpen();
        checkAsyncSupport();
        checkNotNull(key, value);
        loadingCache.put(key, value);
        putToNextLayerAsync(key, value);
    }

    @Override
    public void inValidateWithLowerLayerAsync(K key) {
        checkCacheOpen();
        checkAsyncSupport();
        checkNotNull(key);
        inValidateOfNextLayer(key);
        loadingCache.invalidate(key);
    }

    /**
     * loadingCache本身存在load机制
     * @param key
     * @return
     */
    @Override
    protected V load(K key) {
        return null;
    }

    @Override
    public void close(){
        if (!closed){
            synchronized (mutex){
                if (!closed){
                    closed = true;
                    loadingCache.cleanUp();
                    loadingCache = null;
                    super.close();
                }
            }
        }
    }
}
