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


import org.apache.commons.lang3.Validate;

/**
 *  多层缓存构造器
 * @param <T>
 * @param <K>
 * @param <V>
 */
public abstract class MultiCacheBuilder<T extends MultiCacheBuilder<T, K, V>, K, V> implements BaseCacheBuilder<K, V> {

    // 下级缓存构造器
    protected BaseCacheBuilder<K, V> nextLevelCacheBuilder;

    // 异步操作的标志
    private boolean isAsync = false;

    // 异步之后，创建线程池时线程池核心线程最小数
    private int threadPoolCoreSize = CacheConstant.CommonUsed.UNSET_NUMBER.intValue();

    @SuppressWarnings("unchecked")
    private final T thisAsT = (T) this;

    /**
     *  添加本级缓存的下一级缓存，
     *  <>注意：该方法要在构造器所有必须参数全部配置好之后，调用build()方法之前调用</>
     * @param nextLevelCacheBuilder
     * @return
     */
    public T addNextLevelCacheBuilder(BaseCacheBuilder<K, V> nextLevelCacheBuilder){
        Validate.isTrue(this.nextLevelCacheBuilder == null, "next level cache of cache has bean set");
        this.nextLevelCacheBuilder = nextLevelCacheBuilder;
        return thisAsT;
    }

    public BaseCacheBuilder<K, V> getNextLevelCacheBuilder(){
        return nextLevelCacheBuilder;
    }

    public boolean hasNextLevelCacheBuilder(){
        return nextLevelCacheBuilder != null;
    }

    public T async(){
        async(CacheConstant.CommonUsed.DEFAULT_THREAD_POOL_CORE_SIZE_ASYNC_CACHE);
        return thisAsT;
    }

    public T async(Integer threadPoolCoreSize){
        Validate.isTrue(!isAsync, "async status has bean set true");
        Validate.isTrue(this.threadPoolCoreSize == CacheConstant.CommonUsed.UNSET_NUMBER.intValue(),
            "thread pool core size async cache has bean set");
        Validate.isTrue(threadPoolCoreSize > 0, "threa pool core size async cache must be larger than zero");
        isAsync = true;
        this.threadPoolCoreSize = threadPoolCoreSize;
        return thisAsT;
    }

    public boolean isAsync(){
        return isAsync;
    }

    public int getThreadPoolCoreSize(){
        return threadPoolCoreSize;
    }
}
