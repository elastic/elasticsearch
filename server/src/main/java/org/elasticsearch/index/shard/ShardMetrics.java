/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Node-wide APM instruments that shards record into.
 */
public record ShardMetrics(MergeMetrics merge, ShardSearchPhaseAPMMetrics search, IndexingMetrics indexing) {

    public static final ShardMetrics NOOP = new ShardMetrics(
        MergeMetrics.NOOP,
        new ShardSearchPhaseAPMMetrics(MeterRegistry.NOOP),
        new IndexingMetrics(MeterRegistry.NOOP)
    );

    public static ShardMetrics create(MeterRegistry meterRegistry) {
        return new ShardMetrics(
            new MergeMetrics(meterRegistry),
            new ShardSearchPhaseAPMMetrics(meterRegistry),
            new IndexingMetrics(meterRegistry)
        );
    }
}
