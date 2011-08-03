/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.elasticsearch.common.concurrentlinkedhashmap.EvictionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.cache.filter.support.AbstractWeightedFilterCache;
import org.elasticsearch.index.cache.filter.support.FilterCacheValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class IndicesNodeFilterCache extends AbstractComponent implements EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> {

    private ConcurrentMap<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> cache;

    private volatile long sizeInBytes;

    private final CopyOnWriteArrayList<EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>>> evictionListeners =
            new CopyOnWriteArrayList<EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>>>();

    @Inject public IndicesNodeFilterCache(Settings settings, ThreadPool threadPool) {
        super(settings);

        String size = componentSettings.get("size", "20%");
        if (size.endsWith("%")) {
            double percent = Double.parseDouble(size.substring(0, size.length() - 1));
            sizeInBytes = (long) ((percent / 100) * JvmInfo.jvmInfo().getMem().getHeapMax().bytes());
        } else {
            sizeInBytes = ByteSizeValue.parseBytesSizeValue(size).bytes();
        }
        TimeValue catchupTime = componentSettings.getAsTime("catchup", TimeValue.timeValueSeconds(10));

        int weightedSize = (int) Math.min(sizeInBytes / AbstractWeightedFilterCache.FilterCacheValueWeigher.FACTOR, Integer.MAX_VALUE);

        cache = new ConcurrentLinkedHashMap.Builder<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>>()
                .maximumWeightedCapacity(weightedSize)
                .weigher(new AbstractWeightedFilterCache.FilterCacheValueWeigher())
                .listener(this)
                .catchup(threadPool.scheduler(), catchupTime.millis(), TimeUnit.MILLISECONDS)
                .build();

        logger.debug("using [node] filter cache with size [{}]", new ByteSizeValue(sizeInBytes));
    }

    public void addEvictionListener(EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> listener) {
        evictionListeners.add(listener);
    }

    public void removeEvictionListener(EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> listener) {
        evictionListeners.remove(listener);
    }

    public void close() {
        cache.clear();
    }

    public ConcurrentMap<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> cache() {
        return this.cache;
    }

    @Override public void onEviction(AbstractWeightedFilterCache.FilterCacheKey filterCacheKey, FilterCacheValue<DocSet> docSetFilterCacheValue) {
        for (EvictionListener<AbstractWeightedFilterCache.FilterCacheKey, FilterCacheValue<DocSet>> listener : evictionListeners) {
            listener.onEviction(filterCacheKey, docSetFilterCacheValue);
        }
    }
}