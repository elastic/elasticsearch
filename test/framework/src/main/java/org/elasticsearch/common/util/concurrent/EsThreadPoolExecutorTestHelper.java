/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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

    public static long getStartTimeOfLargeQueue(EsThreadPoolExecutor executor) {
        return executor.getStartTimeOfLargeQueue();
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
