/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import java.util.List;

/**
 * The thread pools of the engine. A flush and a compaction do blocking I/O for a long time (a merge, an upload, a table commit), and
 * they run on pools of their own and not on {@code generic} for a reason that the close of an index shows: the action that verifies a
 * shard before it closes runs on {@code generic} and blocks on the future of our flush. If the flush ran on {@code generic} too, a node
 * that closes more shards than {@code generic} has threads would have every thread waiting for a flush that has no thread to run on, and
 * a node with assertions on refuses to complete a future on the pool of its waiter ("could deadlock if pool was full").
 *
 * <p>The pools are small, because every task of them holds a row group of a file in memory: half the processors of the node, as the
 * pools of the snapshots are.
 */
public final class ParquetThreadPools {

    public static final String FLUSH = "parquet_flush";
    public static final String COMPACTION = "parquet_compaction";

    private ParquetThreadPools() {}

    static List<ExecutorBuilder<?>> builders(Settings settings) {
        final int max = Math.max(1, EsExecutors.allocatedProcessors(settings) / 2);
        return List.of(
            new ScalingExecutorBuilder(FLUSH, 1, max, TimeValue.timeValueSeconds(30), false),
            new ScalingExecutorBuilder(COMPACTION, 1, max, TimeValue.timeValueSeconds(30), false)
        );
    }
}
