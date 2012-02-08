/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.cache.CacheBuilderHelper;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.cache.filter.support.FilterCacheValue;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IndicesFilterCache extends AbstractComponent implements RemovalListener<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> {

    private Cache<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> cache;

    private volatile String size;
    private volatile long sizeInBytes;
    private volatile TimeValue expire;

    private volatile Map<String, RemovalListener<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>>> removalListeners =
            ImmutableMap.of();


    static {
        MetaData.addDynamicSettings(
                "indices.cache.filter.size",
                "indices.cache.filter.expire"
        );
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean replace = false;
            String size = settings.get("indices.cache.filter.size", IndicesFilterCache.this.size);
            if (!size.equals(IndicesFilterCache.this.size)) {
                logger.info("updating [indices.cache.filter.size] from [{}] to [{}]", IndicesFilterCache.this.size, size);
                IndicesFilterCache.this.size = size;
                replace = true;
            }
            TimeValue expire = settings.getAsTime("indices.cache.filter.expire", IndicesFilterCache.this.expire);
            if (!Objects.equal(expire, IndicesFilterCache.this.expire)) {
                logger.info("updating [indices.cache.filter.expire] from [{}] to [{}]", IndicesFilterCache.this.expire, expire);
                IndicesFilterCache.this.expire = expire;
                replace = true;
            }
            if (replace) {
                Cache<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> oldCache = IndicesFilterCache.this.cache;
                computeSizeInBytes();
                buildCache();
                oldCache.invalidateAll();
            }
        }
    }

    @Inject
    public IndicesFilterCache(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.size = componentSettings.get("size", "20%");
        this.expire = componentSettings.getAsTime("expire", null);
        computeSizeInBytes();
        buildCache();
        logger.debug("using [node] filter cache with size [{}], actual_size [{}]", size, new ByteSizeValue(sizeInBytes));

        nodeSettingsService.addListener(new ApplySettings());
    }

    private void buildCache() {
        CacheBuilder<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> cacheBuilder = CacheBuilder.newBuilder()
                .removalListener(this)
                .maximumWeight(sizeInBytes).weigher(new WeightedFilterCache.FilterCacheValueWeigher());

        // defaults to 4, but this is a busy map for all indices, increase it a bit
        cacheBuilder.concurrencyLevel(8);

        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }

        CacheBuilderHelper.disableStats(cacheBuilder);

        cache = cacheBuilder.build();
    }

    private void computeSizeInBytes() {
        if (size.endsWith("%")) {
            double percent = Double.parseDouble(size.substring(0, size.length() - 1));
            sizeInBytes = (long) ((percent / 100) * JvmInfo.jvmInfo().getMem().getHeapMax().bytes());
        } else {
            sizeInBytes = ByteSizeValue.parseBytesSizeValue(size).bytes();
        }
    }

    public synchronized void addRemovalListener(String index, RemovalListener<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> listener) {
        removalListeners = MapBuilder.newMapBuilder(removalListeners).put(index, listener).immutableMap();
    }

    public synchronized void removeRemovalListener(String index) {
        removalListeners = MapBuilder.newMapBuilder(removalListeners).remove(index).immutableMap();
    }

    public void close() {
        cache.invalidateAll();
    }

    public Cache<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> cache() {
        return this.cache;
    }

    @Override
    public void onRemoval(RemovalNotification<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> removalNotification) {
        WeightedFilterCache.FilterCacheKey key = removalNotification.getKey();
        if (key == null) {
            return;
        }
        RemovalListener<WeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> listener = removalListeners.get(key.index());
        if (listener != null) {
            listener.onRemoval(removalNotification);
        }
    }
}