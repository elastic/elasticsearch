/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase-2 discovery must not join on SEARCH or {@code esql_external_io}. Post-discovery CPU hops
 * to SEARCH without waiting for object-store IO (IO has already finished).
 */
public class ComputeServiceAsyncDiscoveryTests extends ESTestCase {

    public void testRunOnSearchFromIoPoolLandsOnSearch() throws Exception {
        ExecutorService search = Executors.newFixedThreadPool(1, EsExecutors.daemonThreadFactory("test", ThreadPool.Names.SEARCH));
        ExecutorService io = Executors.newFixedThreadPool(
            1,
            EsExecutors.daemonThreadFactory("test", EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME)
        );
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        AtomicReference<String> continuationPool = new AtomicReference<>();
        try {
            io.submit(() -> ComputeService.runOnSearch(search, () -> {
                continuationPool.set(EsExecutors.executorName(Thread.currentThread()));
                done.onResponse(null);
            }, done)).get(10, TimeUnit.SECONDS);
            done.actionGet(30, TimeUnit.SECONDS);
            assertEquals(ThreadPool.Names.SEARCH, continuationPool.get());
        } finally {
            search.shutdownNow();
            io.shutdownNow();
        }
    }

    /**
     * Already-SEARCH callers run inline. Hopping to the same 1-thread SEARCH pool and joining
     * would deadlock; executePlanAfterDiscovery relies on this.
     */
    public void testRunOnSearchFromSearchRunsInline() throws Exception {
        ExecutorService search = Executors.newFixedThreadPool(1, EsExecutors.daemonThreadFactory("test", ThreadPool.Names.SEARCH));
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        AtomicBoolean sameThread = new AtomicBoolean();
        try {
            search.submit(() -> {
                Thread caller = Thread.currentThread();
                ComputeService.runOnSearch(search, () -> {
                    sameThread.set(Thread.currentThread() == caller);
                    done.onResponse(null);
                }, done);
                done.actionGet(10, TimeUnit.SECONDS);
            }).get(30, TimeUnit.SECONDS);
            assertTrue("SEARCH inbound must run post-discovery CPU inline", sameThread.get());
        } finally {
            search.shutdownNow();
        }
    }

    /**
     * SEARCH's executor snapshots {@link ThreadContext} at {@code execute} time. Completing
     * discovery on an IO thread without restoring the inbound context would submit an empty
     * snapshot; {@code restoreContextOnCompletion} restores the user before that hop.
     */
    public void testAfterDiscoveryRestoresInboundContextOntoSearch() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("es-security-user", "elastic");
        ExecutorService search = Executors.newFixedThreadPool(1, EsExecutors.daemonThreadFactory("test", ThreadPool.Names.SEARCH));
        ExecutorService io = Executors.newFixedThreadPool(
            1,
            EsExecutors.daemonThreadFactory("test", EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME)
        );
        // Mimic EsThreadPoolExecutor: preserve whatever context is installed at submit time.
        Executor searchPreserving = command -> search.execute(threadContext.preserveContext(command));
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        AtomicReference<String> seenUser = new AtomicReference<>();
        ActionListener<Void> afterDiscovery = ComputeService.restoreContextOnCompletion(
            ActionListener.wrap(ignored -> ComputeService.runOnSearch(searchPreserving, () -> {
                seenUser.set(threadContext.getHeader("es-security-user"));
                done.onResponse(null);
            }, done), done::onFailure),
            threadContext
        );
        try {
            io.submit(() -> {
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    assertNull("SDK/Netty thread has no inbound user", threadContext.getHeader("es-security-user"));
                    afterDiscovery.onResponse(null);
                }
            }).get(10, TimeUnit.SECONDS);
            done.actionGet(30, TimeUnit.SECONDS);
            assertEquals("elastic", seenUser.get());
        } finally {
            search.shutdownNow();
            io.shutdownNow();
        }
    }

    /** Failure from the IO hop must also run with the inbound user, not an empty SDK context. */
    public void testAfterDiscoveryFailureRestoresInboundContext() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("es-security-user", "elastic");
        ExecutorService io = Executors.newFixedThreadPool(
            1,
            EsExecutors.daemonThreadFactory("test", EsqlPlugin.EXTERNAL_IO_THREAD_POOL_NAME)
        );
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        AtomicReference<String> seenUser = new AtomicReference<>();
        ActionListener<Void> afterDiscovery = ComputeService.restoreContextOnCompletion(ActionListener.wrap(ignored -> {
            done.onFailure(new IllegalStateException("success path must not run"));
        }, e -> {
            seenUser.set(threadContext.getHeader("es-security-user"));
            done.onResponse(null);
        }), threadContext);
        try {
            io.submit(() -> {
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    afterDiscovery.onFailure(new RuntimeException("discovery failed"));
                }
            }).get(10, TimeUnit.SECONDS);
            done.actionGet(30, TimeUnit.SECONDS);
            assertEquals("elastic", seenUser.get());
        } finally {
            io.shutdownNow();
        }
    }
}
