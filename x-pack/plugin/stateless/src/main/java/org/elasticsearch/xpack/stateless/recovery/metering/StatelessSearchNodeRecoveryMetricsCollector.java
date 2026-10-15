/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.metering;

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.util.Map;

/// Collects recovery metrics specific to search nodes in stateless deployments.
///
/// Extends [StatelessRecoveryMetricsCollector] to track bytes pulled from indexing nodes during recovery,
/// which is unique to search nodes that replicate shard data from indexing nodes rather than directly from the object store.
public class StatelessSearchNodeRecoveryMetricsCollector extends StatelessRecoveryMetricsCollector {

    public static final String RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_warmed.total";
    public static final String RECOVERY_BYTES_READ_FROM_INDEXING_METRIC = "es.recovery.shard.indexing_node.bytes_read.total";

    private final LongCounter shardRecoveryTotalBytesReadFromIndexingMetric;
    private final LongCounter shardRecoveryTotalBytesWarmedFromIndexingMetric;

    public StatelessSearchNodeRecoveryMetricsCollector(MeterRegistry meterRegistry) {
        super(meterRegistry);
        shardRecoveryTotalBytesReadFromIndexingMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_READ_FROM_INDEXING_METRIC,
            "Bytes read from indexing node during the shard recovery",
            "bytes"
        );
        shardRecoveryTotalBytesWarmedFromIndexingMetric = meterRegistry.registerLongCounter(
            RECOVERY_BYTES_WARMED_FROM_INDEXING_METRIC,
            "Bytes warmed from indexing node during the shard recovery",
            "bytes"
        );
    }

    @Override
    protected void updateMetrics(final IndexShard indexShard, final Store store, final Map<String, Object> metricLabels) {
        super.updateMetrics(indexShard, store, metricLabels);
        assert indexShard.routingEntry().isPromotableToPrimary() == false
            : "Index shard is promotable to primary, but this recovery metrics collector is only for search nodes";
        final SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
        shardRecoveryTotalBytesReadFromIndexingMetric.incrementBy(searchDirectory.totalBytesReadFromIndexing(), metricLabels);
        shardRecoveryTotalBytesWarmedFromIndexingMetric.incrementBy(searchDirectory.totalBytesWarmedFromIndexing(), metricLabels);
    }
}
