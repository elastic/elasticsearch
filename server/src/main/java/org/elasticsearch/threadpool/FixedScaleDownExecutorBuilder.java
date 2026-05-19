/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A builder for fixed scale-down executors.
 * A hybrid between a fixed and scaling executor for scenarios where you wish to limit the max number of threads executing and queued
 * but would like the pool to scale down when fewer than the max threads are in use and nothing is in the queue
 */
public final class FixedScaleDownExecutorBuilder extends FixedExecutorBuilder {

    private final TimeValue scaleDownDelay;

    public FixedScaleDownExecutorBuilder(
        Settings settings,
        String name,
        int defaultSize,
        int defaultQueueSize,
        TimeValue scaleDownDelay,
        String prefix,
        EsExecutors.TaskTrackingConfig taskTrackingConfig,
        boolean isSystemThread
    ) {
        super(settings, name, defaultSize, defaultQueueSize, prefix, taskTrackingConfig, isSystemThread);
        this.scaleDownDelay = scaleDownDelay;
    }

    @Override
    ThreadPool.ExecutorHolder build(FixedExecutorSettings settings, ThreadContext threadContext) {
        int size = settings.getSize();
        int queueSize = settings.getQueueSize();
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings.nodeName, name(), isSystemThread());
        final EsThreadPoolExecutor executor = EsExecutors.newFixed(
            settings.nodeName + "/" + name(),
            size,
            Math.max(queueSize, 0),
            threadFactory,
            threadContext,
            getTaskTrackingConfig()
        );

        executor.setKeepAliveTime(scaleDownDelay.millis(), TimeUnit.MILLISECONDS);
        executor.allowCoreThreadTimeOut(true);

        final ThreadPool.Info info = new ThreadPool.Info(
            name(),
            ThreadPool.ThreadPoolType.FIXED,
            0,
            size,
            scaleDownDelay,
            queueSize < 0 ? null : (long) queueSize
        );
        return new ThreadPool.ExecutorHolder(executor, info);
    }
}
