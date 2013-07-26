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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.TimeUnit;

/**
 */
public class ShardPercolateService extends AbstractIndexShardComponent {

    @Inject
    public ShardPercolateService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    private final MeanMetric percolateMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();

    public void prePercolate() {
        currentMetric.inc();
    }

    public void postPercolate(long tookInNanos) {
        currentMetric.dec();
        percolateMetric.inc(tookInNanos);
    }

    public PercolateStats stats() {
        return new PercolateStats(percolateMetric.count(), TimeUnit.NANOSECONDS.toMillis(percolateMetric.sum()), currentMetric.count());
    }

}
