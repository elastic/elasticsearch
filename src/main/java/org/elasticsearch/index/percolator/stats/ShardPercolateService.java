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

package org.elasticsearch.index.percolator.stats;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.TimeUnit;

/**
 * Shard level percolator service that maintains percolator metrics:
 * <ul>
 *     <li> total time spent in percolate api
 *     <li> the current number of percolate requests
 *     <li> number of registered percolate queries
 *     <li> the estimated amount of memory the registered queries take
 * </ul>
 */
public class ShardPercolateService extends AbstractIndexShardComponent {

    @Inject
    public ShardPercolateService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    private final MeanMetric percolateMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();

    private final CounterMetric numberOfQueries = new CounterMetric();
    private final CounterMetric memorySizeInBytes = new CounterMetric();

    public void prePercolate() {
        currentMetric.inc();
    }

    public void postPercolate(long tookInNanos) {
        currentMetric.dec();
        percolateMetric.inc(tookInNanos);
    }

    public void addedQuery(HashedBytesRef id, Query previousQuery, Query newQuery) {
        if (previousQuery != null) {
            memorySizeInBytes.dec(computeSizeInMemory(id, previousQuery));
        } else {
            numberOfQueries.inc();
        }
        memorySizeInBytes.inc(computeSizeInMemory(id, newQuery));
    }

    public void removedQuery(HashedBytesRef id, Query query) {
        numberOfQueries.dec();
        memorySizeInBytes.dec(computeSizeInMemory(id, query));
    }

    /**
     * @return The current metrics
     */
    public PercolateStats stats() {
        return new PercolateStats(percolateMetric.count(), TimeUnit.NANOSECONDS.toMillis(percolateMetric.sum()), currentMetric.count(), memorySizeInBytes.count(), numberOfQueries.count());
    }

    private static long computeSizeInMemory(HashedBytesRef id, Query query) {
        long size = (3 * RamUsageEstimator.NUM_BYTES_INT) + RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + id.bytes.bytes.length;
        size += RamUsageEstimator.sizeOf(query);
        return size;
    }

}
