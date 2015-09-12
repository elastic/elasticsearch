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

package org.elasticsearch.index.warmer;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.TimeUnit;

/**
 */
public class ShardIndexWarmerService extends AbstractIndexShardComponent {

    private final CounterMetric current = new CounterMetric();
    private final MeanMetric warmerMetric = new MeanMetric();

    public ShardIndexWarmerService(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    public ESLogger logger() {
        return this.logger;
    }

    public void onPreWarm() {
        current.inc();
    }

    public void onPostWarm(long tookInNanos) {
        current.dec();
        warmerMetric.inc(tookInNanos);
    }

    public WarmerStats stats() {
        return new WarmerStats(current.count(), warmerMetric.count(), TimeUnit.NANOSECONDS.toMillis(warmerMetric.sum()));
    }
}
