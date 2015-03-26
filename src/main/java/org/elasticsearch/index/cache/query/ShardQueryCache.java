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

package org.elasticsearch.index.cache.query;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.lucene.search.DocIdSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;

/**
 */
public class ShardQueryCache extends AbstractIndexShardComponent implements RemovalListener<IndicesQueryCache.Key, IndicesQueryCache.Value> {

    final CounterMetric evictionsMetric = new CounterMetric();
    final CounterMetric totalMetric = new CounterMetric();
    final CounterMetric hitCount = new CounterMetric();
    final CounterMetric missCount = new CounterMetric();

    @Inject
    public ShardQueryCache(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    public QueryCacheStats stats() {
        return new QueryCacheStats(totalMetric.count(), evictionsMetric.count(), hitCount.count(), missCount.count());
    }

    public void onHit() {
        hitCount.inc();
    }

    public void onMiss() {
        missCount.inc();
    }

    public void onCached(IndicesQueryCache.Key key, IndicesQueryCache.Value value) {
        totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
    }

    @Override
    public void onRemoval(RemovalNotification<IndicesQueryCache.Key, IndicesQueryCache.Value> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictionsMetric.inc();
        }
        long dec = 0;
        if (removalNotification.getKey() != null) {
            dec += removalNotification.getKey().ramBytesUsed();
        }
        if (removalNotification.getValue() != null) {
            dec += removalNotification.getValue().ramBytesUsed();
        }
        totalMetric.dec(dec);
    }
}
