/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.cache.filter;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class IndicesFilterCache extends AbstractComponent implements RemovalListener<WeightedFilterCache.FilterCacheKey, DocIdSet> {

    private final ThreadPool threadPool;

    private Cache<WeightedFilterCache.FilterCacheKey, DocIdSet> cache;

    private volatile String size;
    private volatile long sizeInBytes;
    private volatile TimeValue expire;
    private volatile int concurrencyLevel;

    private final TimeValue cleanInterval;
    private final int minimumEntryWeight;

    private final Set<Object> readersKeysToClean = ConcurrentCollections.newConcurrentSet();

    private volatile boolean closed;

    public static final String INDICES_CACHE_FILTER_SIZE = "indices.cache.filter.size";
    public static final String INDICES_CACHE_FILTER_EXPIRE = "indices.cache.filter.expire";
    public static final String INDICES_CACHE_FILTER_CONCURRENCY_LEVEL = "indices.cache.filter.concurrency_level";
    public static final String INDICES_CACHE_FILTER_CLEAN_INTERVAL = "indices.cache.filter.clean_interval";
    public static final String INDICES_CACHE_FILTER_MINIMUM_ENTRY_WEIGHT = "indices.cache.filter.minimum_entry_weight";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean replace = false;
            String size = settings.get(INDICES_CACHE_FILTER_SIZE, IndicesFilterCache.this.size);
            if (!size.equals(IndicesFilterCache.this.size)) {
                logger.info("updating [{}] from [{}] to [{}]",
                        INDICES_CACHE_FILTER_SIZE, IndicesFilterCache.this.size, size);
                IndicesFilterCache.this.size = size;
                replace = true;
            }
            TimeValue expire = settings.getAsTime(INDICES_CACHE_FILTER_EXPIRE, IndicesFilterCache.this.expire);
            if (!Objects.equal(expire, IndicesFilterCache.this.expire)) {
                logger.info("updating [{}] from [{}] to [{}]",
                        INDICES_CACHE_FILTER_EXPIRE, IndicesFilterCache.this.expire, expire);
                IndicesFilterCache.this.expire = expire;
                replace = true;
            }
            final int concurrencyLevel = settings.getAsInt(INDICES_CACHE_FILTER_CONCURRENCY_LEVEL, IndicesFilterCache.this.concurrencyLevel);
            if (concurrencyLevel <= 0) {
                throw new ElasticsearchIllegalArgumentException("concurrency_level must be > 0 but was: " + concurrencyLevel);
            }
            if (!Objects.equal(concurrencyLevel, IndicesFilterCache.this.concurrencyLevel)) {
                logger.info("updating [{}] from [{}] to [{}]",
                        INDICES_CACHE_FILTER_CONCURRENCY_LEVEL, IndicesFilterCache.this.concurrencyLevel, concurrencyLevel);
                IndicesFilterCache.this.concurrencyLevel = concurrencyLevel;
                replace = true;
            }
            if (replace) {
                Cache<WeightedFilterCache.FilterCacheKey, DocIdSet> oldCache = IndicesFilterCache.this.cache;
                computeSizeInBytes();
                buildCache();
                oldCache.invalidateAll();
            }
        }
    }

    @Inject
    public IndicesFilterCache(Settings settings, ThreadPool threadPool, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.threadPool = threadPool;
        this.size = settings.get(INDICES_CACHE_FILTER_SIZE, "10%");
        this.expire = settings.getAsTime(INDICES_CACHE_FILTER_EXPIRE, null);
        this.minimumEntryWeight = settings.getAsInt(INDICES_CACHE_FILTER_MINIMUM_ENTRY_WEIGHT, 1024); // 1k per entry minimum
        if (minimumEntryWeight <= 0) {
            throw new ElasticsearchIllegalArgumentException("minimum_entry_weight must be > 0 but was: " + minimumEntryWeight);
        }
        this.cleanInterval = settings.getAsTime(INDICES_CACHE_FILTER_CLEAN_INTERVAL, TimeValue.timeValueSeconds(60));
        // defaults to 4, but this is a busy map for all indices, increase it a bit
        this.concurrencyLevel =  settings.getAsInt(INDICES_CACHE_FILTER_CONCURRENCY_LEVEL, 16);
        if (concurrencyLevel <= 0) {
            throw new ElasticsearchIllegalArgumentException("concurrency_level must be > 0 but was: " + concurrencyLevel);
        }
        computeSizeInBytes();
        buildCache();
        logger.debug("using [node] weighted filter cache with size [{}], actual_size [{}], expire [{}], clean_interval [{}]",
                size, new ByteSizeValue(sizeInBytes), expire, cleanInterval);

        nodeSettingsService.addListener(new ApplySettings());
        threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, new ReaderCleaner());
    }

    private void buildCache() {
        CacheBuilder<WeightedFilterCache.FilterCacheKey, DocIdSet> cacheBuilder = CacheBuilder.newBuilder()
                .removalListener(this)
                .maximumWeight(sizeInBytes).weigher(new WeightedFilterCache.FilterCacheValueWeigher(minimumEntryWeight));

        cacheBuilder.concurrencyLevel(this.concurrencyLevel);

        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }

        cache = cacheBuilder.build();
    }

    private void computeSizeInBytes() {
        this.sizeInBytes = MemorySizeValue.parseBytesSizeValueOrHeapRatio(size).bytes();
    }

    public void addReaderKeyToClean(Object readerKey) {
        readersKeysToClean.add(readerKey);
    }

    public void close() {
        closed = true;
        cache.invalidateAll();
    }

    public Cache<WeightedFilterCache.FilterCacheKey, DocIdSet> cache() {
        return this.cache;
    }

    @Override
    public void onRemoval(RemovalNotification<WeightedFilterCache.FilterCacheKey, DocIdSet> removalNotification) {
        WeightedFilterCache.FilterCacheKey key = removalNotification.getKey();
        if (key == null) {
            return;
        }
        if (key.removalListener != null) {
            key.removalListener.onRemoval(removalNotification);
        }
    }

    /**
     * The reason we need this class is because we need to clean all the filters that are associated
     * with a reader. We don't want to do it every time a reader closes, since iterating over all the map
     * is expensive. There doesn't seem to be a nicer way to do it (and maintaining a list per reader
     * of the filters will cost more).
     */
    class ReaderCleaner implements Runnable {

        // this is thread safe since we only schedule the next cleanup once the current one is
        // done, so no concurrent execution
        private final ObjectOpenHashSet<Object> keys = ObjectOpenHashSet.newInstance();

        @Override
        public void run() {
            if (closed) {
                return;
            }
            if (readersKeysToClean.isEmpty()) {
                schedule();
                return;
            }
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        keys.clear();
                        for (Iterator<Object> it = readersKeysToClean.iterator(); it.hasNext(); ) {
                            keys.add(it.next());
                            it.remove();
                        }
                        if (!keys.isEmpty()) {
                            for (Iterator<WeightedFilterCache.FilterCacheKey> it = cache.asMap().keySet().iterator(); it.hasNext(); ) {
                                WeightedFilterCache.FilterCacheKey filterCacheKey = it.next();
                                if (keys.contains(filterCacheKey.readerKey())) {
                                    // same as invalidate
                                    it.remove();
                                }
                            }
                        }
                        cache.cleanUp();
                        schedule();
                        keys.clear();
                    }
                });
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not run ReaderCleaner - execution rejected", ex);
            }
        }

        private void schedule() {
            try {
                threadPool.schedule(cleanInterval, ThreadPool.Names.SAME, this);
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Can not schedule ReaderCleaner - execution rejected", ex);
            }
        }
    }
}