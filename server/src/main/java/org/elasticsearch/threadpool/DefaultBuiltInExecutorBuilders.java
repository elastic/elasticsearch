/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.internal.BuiltInExecutorBuilders;

import java.util.Map;

import static org.elasticsearch.threadpool.ThreadPool.searchAutoscalingEWMA;

public class DefaultBuiltInExecutorBuilders implements BuiltInExecutorBuilders {
    @Override
    @SuppressWarnings("rawtypes")
    public void registerBuilders(Settings settings, int allocatedProcessors, Map<String, ExecutorBuilder> builders) {
        final int halfProc = Util.halfAllocatedProcessors(allocatedProcessors);
        final int halfProcMaxAt5 = Util.halfAllocatedProcessorsMaxFive(allocatedProcessors);
        final int halfProcMaxAt10 = Util.halfAllocatedProcessorsMaxTen(allocatedProcessors);
        final int genericThreadPoolMax = Util.boundedBy(4 * allocatedProcessors, 128, 512);

        builders.put(
            ThreadPool.Names.GENERIC,
            new ScalingExecutorBuilder(ThreadPool.Names.GENERIC, 4, genericThreadPoolMax, TimeValue.timeValueSeconds(30), false)
        );
        builders.put(
            ThreadPool.Names.WRITE,
            new FixedExecutorBuilder(settings, ThreadPool.Names.WRITE, allocatedProcessors, 10000, new EsExecutors.TaskTrackingConfig(true, 0.1))
        );
        int searchOrGetThreadPoolSize = Util.searchOrGetThreadPoolSize(allocatedProcessors);
        builders.put(
            ThreadPool.Names.GET,
            new FixedExecutorBuilder(settings, ThreadPool.Names.GET, searchOrGetThreadPoolSize, 1000, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(ThreadPool.Names.ANALYZE, new FixedExecutorBuilder(settings, ThreadPool.Names.ANALYZE, 1, 16, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK));
        builders.put(
            ThreadPool.Names.SEARCH,
            new FixedExecutorBuilder(
                settings,
                ThreadPool.Names.SEARCH,
                searchOrGetThreadPoolSize,
                1000,
                new EsExecutors.TaskTrackingConfig(true, searchAutoscalingEWMA)
            )
        );
        builders.put(
            ThreadPool.Names.SEARCH_WORKER,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SEARCH_WORKER, searchOrGetThreadPoolSize, -1, EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            ThreadPool.Names.SEARCH_COORDINATION,
            new FixedExecutorBuilder(
                settings,
                ThreadPool.Names.SEARCH_COORDINATION,
                halfProc,
                1000,
                new EsExecutors.TaskTrackingConfig(true, searchAutoscalingEWMA)
            )
        );
        builders.put(
            ThreadPool.Names.AUTO_COMPLETE,
            new FixedExecutorBuilder(settings, ThreadPool.Names.AUTO_COMPLETE, Math.max(allocatedProcessors / 4, 1), 100, EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            ThreadPool.Names.SEARCH_THROTTLED,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SEARCH_THROTTLED, 1, 100, EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            ThreadPool.Names.MANAGEMENT,
            new ScalingExecutorBuilder(ThreadPool.Names.MANAGEMENT, 1, Util.boundedBy(allocatedProcessors, 1, 5), TimeValue.timeValueMinutes(5), false)
        );
        builders.put(ThreadPool.Names.FLUSH, new ScalingExecutorBuilder(ThreadPool.Names.FLUSH, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5), false));
        // TODO: remove (or refine) this temporary stateless custom refresh pool sizing once ES-7631 is solved.
        final int refreshThreads = DiscoveryNode.isStateless(settings) ? allocatedProcessors : halfProcMaxAt10;
        builders.put(ThreadPool.Names.REFRESH, new ScalingExecutorBuilder(ThreadPool.Names.REFRESH, 1, refreshThreads, TimeValue.timeValueMinutes(5), false));
        builders.put(ThreadPool.Names.WARMER, new ScalingExecutorBuilder(ThreadPool.Names.WARMER, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5), false));
        final int maxSnapshotCores = Util.getMaxSnapshotThreadPoolSize(allocatedProcessors);
        builders.put(ThreadPool.Names.SNAPSHOT, new ScalingExecutorBuilder(ThreadPool.Names.SNAPSHOT, 1, maxSnapshotCores, TimeValue.timeValueMinutes(5), false));
        builders.put(
            ThreadPool.Names.SNAPSHOT_META,
            new ScalingExecutorBuilder(
                ThreadPool.Names.SNAPSHOT_META,
                1,
                Math.min(allocatedProcessors * 3, 50),
                TimeValue.timeValueSeconds(30L),
                false
            )
        );
        builders.put(
            ThreadPool.Names.FETCH_SHARD_STARTED,
            new ScalingExecutorBuilder(ThreadPool.Names.FETCH_SHARD_STARTED, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5), false)
        );
        builders.put(
            ThreadPool.Names.FORCE_MERGE,
            new FixedExecutorBuilder(
                settings,
                ThreadPool.Names.FORCE_MERGE,
                Util.oneEighthAllocatedProcessors(allocatedProcessors),
                -1,
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        builders.put(
            ThreadPool.Names.CLUSTER_COORDINATION,
            new FixedExecutorBuilder(settings, ThreadPool.Names.CLUSTER_COORDINATION, 1, -1, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            ThreadPool.Names.FETCH_SHARD_STORE,
            new ScalingExecutorBuilder(ThreadPool.Names.FETCH_SHARD_STORE, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5), false)
        );
        builders.put(
            ThreadPool.Names.SYSTEM_READ,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SYSTEM_READ, halfProcMaxAt5, 2000, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            ThreadPool.Names.SYSTEM_WRITE,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SYSTEM_WRITE, halfProcMaxAt5, 1000, new EsExecutors.TaskTrackingConfig(true, 0.1))
        );
        builders.put(
            ThreadPool.Names.SYSTEM_CRITICAL_READ,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SYSTEM_CRITICAL_READ, halfProcMaxAt5, 2000, EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            ThreadPool.Names.SYSTEM_CRITICAL_WRITE,
            new FixedExecutorBuilder(settings, ThreadPool.Names.SYSTEM_CRITICAL_WRITE, halfProcMaxAt5, 1500, new EsExecutors.TaskTrackingConfig(true, 0.1))
        );
    }
}
