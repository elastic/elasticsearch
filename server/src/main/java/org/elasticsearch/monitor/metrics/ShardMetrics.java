/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/// Records shard-level metrics like shard sizes.
public class ShardMetrics extends AbstractLifecycleComponent {
    public static String SHARD_SIZE_HISTOGRAM = "es.shard.total_size.histogram";

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final TimeValue collectionInterval;
    private final DoubleHistogram shardSizeHistogram;

    // Assigned under `lifecycle` lock.
    private Scheduler.Cancellable scheduledFuture;

    public ShardMetrics(
        MeterRegistry meterRegistry,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndicesService indicesService,
        TimeValue collectionInterval
    ) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.collectionInterval = collectionInterval;

        this.shardSizeHistogram = meterRegistry.registerDoubleHistogram(
            SHARD_SIZE_HISTOGRAM,
            "Total size of shard data in gigabytes",
            // Use gigabytes to avoid high cardinality that megabytes would cause (a shard can be hundreds of GBs).
            // Histograms that we use have a predefined number of buckets and lose precision of large values
            // when cardinality is high.
            "gigabytes"
        );
    }

    // visible for tests
    public void runNow() {
        gatherAndEmitMetrics();
    }

    @Override
    protected void doStart() {
        assert Thread.holdsLock(lifecycle);
        scheduledFuture = threadPool.scheduleWithFixedDelay(this::gatherAndEmitMetrics, collectionInterval, threadPool.generic());
    }

    @Override
    protected void doStop() {
        assert Thread.holdsLock(lifecycle);
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
            scheduledFuture = null;
        }
    }

    @Override
    protected void doClose() throws IOException {}

    private void gatherAndEmitMetrics() {
        ClusterState state = clusterService.state();

        for (IndexService indexService : indicesService) {
            Optional<IndexAbstraction> indexAbstraction = state.metadata()
                .lookupProject(indexService.index())
                .flatMap(pm -> Optional.ofNullable(pm.getIndicesLookup().get(indexService.index().getName())));

            if (indexAbstraction.isEmpty()) {
                continue;
            }
            if (indexService.getMetadata().isSystem() || indexAbstraction.get().isHidden()) {
                continue;
            }

            for (IndexShard shard : indexService) {
                if (shard.routingEntry().started() == false) {
                    continue;
                }
                // Do not report the same size multiple times on replicas.
                if (shard.routingEntry().primary() == false) {
                    continue;
                }

                try {
                    var stats = shard.storeStats();
                    shardSizeHistogram.record(
                        stats.totalDataSetSize().getGbFrac(),
                        Map.of("es_is_in_datastream", indexAbstraction.get().getParentDataStream() != null)
                    );
                } catch (Exception e) {
                    // Ignore individual shard failures and keep going.
                }
            }
        }
    }
}
