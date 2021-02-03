/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.warmer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.TimeUnit;

public class ShardIndexWarmerService extends AbstractIndexShardComponent {

    private final CounterMetric current = new CounterMetric();
    private final MeanMetric warmerMetric = new MeanMetric();

    public ShardIndexWarmerService(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
    }

    public Logger logger() {
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
