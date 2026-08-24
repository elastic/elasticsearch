/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.monitor.jvm.HotThreads;

import java.util.function.IntSupplier;

import static org.elasticsearch.common.Strings.format;

/// Schedules periodic hot-thread dumps during slow relocation operations.
/// Shared by [StatelessPrimaryRelocationSourceService] and [StatelessPrimaryRelocationTargetService].
class SlowRelocationLogger {

    private SlowRelocationLogger() {}

    static final int MAX_SLOW_OPERATION_THREAD_DUMPS = 5;

    /// Returns a listener whose completion (via `onResponse` or `onFailure`) cancels any pending thread dumps. Dumps are
    /// logged at INFO level on an exponential back-off starting at `timeout`, up to [MAX_SLOW_OPERATION_THREAD_DUMPS] samples.
    ///
    /// @param activeOperationsCount if non-null, the current count is appended to each dump message
    static ActionListener<Void> slowShardOperationListener(
        Logger logger,
        IndexShard indexShard,
        String targetAllocationId,
        TimeValue timeout,
        String label,
        @Nullable IntSupplier activeOperationsCount
    ) {
        final var threadDumpListener = new SubscribableListener<Void>();
        if (logger.isInfoEnabled()) {
            scheduleSlowShardOperationThreadDump(
                logger,
                indexShard,
                targetAllocationId,
                timeout,
                label,
                activeOperationsCount,
                threadDumpListener,
                1
            );
        }
        return threadDumpListener;
    }

    private static void scheduleSlowShardOperationThreadDump(
        Logger logger,
        IndexShard indexShard,
        String targetAllocationId,
        TimeValue delay,
        String label,
        @Nullable IntSupplier activeOperationsCount,
        SubscribableListener<Void> threadDumpListener,
        int sample
    ) {
        final var threadPool = indexShard.getThreadPool();
        try {
            threadPool.schedule(() -> {
                if (threadDumpListener.isDone()) {
                    return;
                }
                HotThreads.logLocalHotThreads(
                    logger,
                    Level.INFO,
                    indexShard.shardId()
                        + " recovery ["
                        + targetAllocationId
                        + "]: "
                        + label
                        + " #"
                        + sample
                        + (activeOperationsCount == null
                            ? ""
                            : " with [" + activeOperationsCount.getAsInt() + "] operations holding permits"),
                    ReferenceDocs.LOGGING
                );
                if (sample < MAX_SLOW_OPERATION_THREAD_DUMPS) {
                    scheduleSlowShardOperationThreadDump(
                        logger,
                        indexShard,
                        targetAllocationId,
                        TimeValue.timeValueMillis(delay.millis() * 2),
                        label,
                        activeOperationsCount,
                        threadDumpListener,
                        sample + 1
                    );
                }
            }, delay, threadPool.generic());
        } catch (Exception e) {
            logger.debug(() -> format("%s failed to schedule slow operation thread dump", indexShard.shardId()), e);
        }
    }
}
