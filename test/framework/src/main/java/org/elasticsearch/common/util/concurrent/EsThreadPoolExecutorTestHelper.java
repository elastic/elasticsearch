/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.elasticsearch.test.ESTestCase.randomIdentifier;

public class EsThreadPoolExecutorTestHelper {

    public static EsExecutors.HotThreadsOnLargeQueueConfig getHotThreadsOnLargeQueueConfig(EsThreadPoolExecutor executor) {
        return executor.getHotThreadsOnLargeQueueConfig();
    }

    public static long getStartTimeMillisOfLargeQueue(EsThreadPoolExecutor executor) {
        return executor.getStartTimeMillisOfLargeQueue();
    }

    public static EsThreadPoolExecutor newEsThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        EsExecutors.HotThreadsOnLargeQueueConfig hotThreadsOnLargeQueueConfig,
        LongSupplier currentTimeMillisSupplier
    ) {
        return new EsThreadPoolExecutor(
            name,
            corePoolSize,
            maximumPoolSize,
            5,
            TimeUnit.MINUTES,
            new EsExecutors.ExecutorScalingQueue<>(),
            EsExecutors.daemonThreadFactory(randomIdentifier(), name),
            new EsExecutors.ForceQueuePolicy(false, false),
            new ThreadContext(Settings.EMPTY),
            hotThreadsOnLargeQueueConfig,
            currentTimeMillisSupplier
        );
    }
}
