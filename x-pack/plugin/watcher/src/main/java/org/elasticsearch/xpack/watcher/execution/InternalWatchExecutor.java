/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.SpanId;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.watcher.execution.Wid;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class InternalWatchExecutor implements WatchExecutor {

    public static final String THREAD_POOL_NAME = XPackField.WATCHER;

    private final ThreadPool threadPool;
    private final Tracer tracer;

    public InternalWatchExecutor(ThreadPool threadPool, Tracer tracer) {
        this.threadPool = threadPool;
        this.tracer = tracer;
    }

    @Override
    public BlockingQueue<Runnable> queue() {
        return executor().getQueue();
    }

    @Override
    public Stream<Runnable> tasks() {
        return executor().getTasks();
    }

    @Override
    public long largestPoolSize() {
        return executor().getLargestPoolSize();
    }

    @Override
    public void execute(Runnable runnable) {
        executor().execute(runnable);
    }

    @Override
    public void startTrace(Wid id) {
        this.tracer.startTrace(
            threadPool.getThreadContext(),
            SpanId.forBareString("watcher-" + id),
            "Executing watch " + id.watchId(),
            Map.of("watchId", id.watchId())
        );
    }

    @Override
    public void stopTrace(Wid id) {
        this.tracer.stopTrace(SpanId.forBareString("watcher-" + id));
    }

    private EsThreadPoolExecutor executor() {
        return (EsThreadPoolExecutor) threadPool.executor(THREAD_POOL_NAME);
    }

}
