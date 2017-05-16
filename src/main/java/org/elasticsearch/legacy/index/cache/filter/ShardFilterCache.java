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

package org.elasticsearch.legacy.index.cache.filter;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.lucene.docset.DocIdSets;
import org.elasticsearch.legacy.common.metrics.CounterMetric;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.legacy.index.settings.IndexSettings;
import org.elasticsearch.legacy.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.legacy.index.shard.ShardId;

/**
 */
public class ShardFilterCache extends AbstractIndexShardComponent implements RemovalListener<WeightedFilterCache.FilterCacheKey, DocIdSet> {

    final CounterMetric evictionsMetric = new CounterMetric();
    final CounterMetric totalMetric = new CounterMetric();

    @Inject
    public ShardFilterCache(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    public FilterCacheStats stats() {
        return new FilterCacheStats(totalMetric.count(), evictionsMetric.count());
    }

    public void onCached(long sizeInBytes) {
        totalMetric.inc(sizeInBytes);
    }

    @Override
    public void onRemoval(RemovalNotification<WeightedFilterCache.FilterCacheKey, DocIdSet> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictionsMetric.inc();
        }
        if (removalNotification.getValue() != null) {
            totalMetric.dec(DocIdSets.sizeInBytes(removalNotification.getValue()));
        }
    }
}
