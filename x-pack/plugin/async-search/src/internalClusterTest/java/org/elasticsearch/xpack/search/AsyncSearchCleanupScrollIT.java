/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * The async-results cleanup delete-by-query holds a scroll ({@code keepAlive=5m}) until it finishes.
 * {@link #restartTaskNode} must not drain search contexts while that scroll is still open: the drain
 * only polls for 10 seconds, and the scroll outlives it.
 */
public class AsyncSearchCleanupScrollIT extends AsyncSearchIntegTestCase {

    @After
    public void releaseCleanupScrollBlock() {
        CleanupScrollBlockPlugin.release();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<Class<? extends Plugin>>(super.nodePlugins());
        plugins.add(CleanupScrollBlockPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        // The context drain only observes contexts tracked by MockSearchService, which the suite otherwise installs at random.
        var plugins = new ArrayList<>(super.getMockPlugins());
        if (plugins.contains(MockSearchService.TestPlugin.class) == false) {
            plugins.add(MockSearchService.TestPlugin.class);
        }
        return plugins;
    }

    /**
     * Holds the cleanup scroll open across the start of {@link #restartTaskNode}. The restart drains
     * contexts only after its delete-by-query wait, so the drain must not be on the stack until this
     * test releases the scroll.
     */
    public void testRestartDoesNotDrainWhileCleanupScrollIsOpen() throws Exception {
        final String indexName = "test-cleanup-scroll";
        createIndex(indexName);
        indexRandom(true, prepareIndex(indexName).setSource("field", "value"));

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(1));
        request.setKeepOnCompletion(true);
        final String searchId;
        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            searchId = response.getId();
        } finally {
            response.decRef();
        }
        ensureTaskCompletion(searchId);
        // A cleanup runs every 100ms. Wait until one has finished so the next scroll is the one we block.
        assertBusy(MockSearchService::assertNoInFlightContext);

        CleanupScrollBlockPlugin.arm();
        final AtomicBoolean drainedTooEarly = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread restartThread = null;
        try {
            assertTrue(
                "cleanup delete-by-query did not open a scroll on " + XPackPlugin.ASYNC_RESULTS_INDEX,
                CleanupScrollBlockPlugin.awaitEntered(30, TimeUnit.SECONDS)
            );
            AssertionError leak = expectThrows(AssertionError.class, MockSearchService::assertNoInFlightContext);
            assertThat(leak.getMessage(), containsString("LegacyReaderContext"));
            assertThat(leak.getMessage(), containsString(XPackPlugin.ASYNC_RESULTS_INDEX));
            assertThat(leak.getMessage(), containsString("keepAlive=5m"));

            // Resolve via reflection so a rename of ensureAllSearchContextsReleased fails this test
            // instead of leaving inDrain always false (silent pass while assertBusy still matches).
            final String drainMethod = ESTestCase.class.getMethod("ensureAllSearchContextsReleased").getName();
            restartThread = new Thread(() -> {
                try {
                    restartTaskNode(searchId, indexName);
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            }, "restart-task-node");
            restartThread.start();
            final Thread thread = restartThread;
            assertBusy(() -> {
                Throwable thrown = failure.get();
                if (thrown != null) {
                    throw new IllegalStateException("restartTaskNode failed before the drain check", thrown);
                }
                StackTraceElement[] stack = thread.getStackTrace();
                boolean inRestart = stackContainsMethod(stack, "restartTaskNode");
                boolean inDrain = stackContainsMethod(stack, drainMethod);
                boolean inAssertBusy = stackContainsMethod(stack, "assertBusy");
                assertTrue(
                    "restartTaskNode has not reached the delete-by-query wait or the context drain" + renderStack(stack),
                    thread.isAlive() && inRestart && (inDrain || inAssertBusy)
                );
                drainedTooEarly.set(inDrain);
            });
        } finally {
            CleanupScrollBlockPlugin.release();
            if (restartThread != null) {
                restartThread.join(TimeUnit.MINUTES.toMillis(2));
                assertFalse("restartTaskNode did not finish", restartThread.isAlive());
            }
        }
        assertFalse("context drain started while the cleanup scroll was still open", drainedTooEarly.get());
        Throwable thrown = failure.get();
        if (thrown != null) {
            throw new AssertionError("restartTaskNode failed", thrown);
        }
    }

    private static boolean stackContainsMethod(StackTraceElement[] stack, String methodName) {
        for (StackTraceElement frame : stack) {
            if (methodName.equals(frame.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    private static String renderStack(StackTraceElement[] stack) {
        StringBuilder builder = new StringBuilder();
        for (StackTraceElement frame : stack) {
            builder.append('\n').append(frame);
        }
        return builder.toString();
    }

    /**
     * Blocks the query phase of a cleanup scroll on the async-results index while {@link #arm()} is set.
     * The context is already registered with {@link MockSearchService} by the time {@code onPreQueryPhase} runs.
     */
    public static class CleanupScrollBlockPlugin extends Plugin {
        private static final AtomicBoolean armed = new AtomicBoolean();
        private static volatile CountDownLatch entered;
        private static volatile CountDownLatch blocked;

        static void arm() {
            release();
            entered = new CountDownLatch(1);
            blocked = new CountDownLatch(1);
            armed.set(true);
        }

        static void release() {
            armed.set(false);
            CountDownLatch latch = blocked;
            if (latch != null) {
                latch.countDown();
            }
        }

        static boolean awaitEntered(long timeout, TimeUnit unit) throws InterruptedException {
            CountDownLatch latch = entered;
            return latch != null && latch.await(timeout, unit);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getIndex().getName().startsWith(XPackPlugin.ASYNC_RESULTS_INDEX) == false) {
                return;
            }
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    if (armed.get() == false || searchContext.readerContext() instanceof LegacyReaderContext == false) {
                        return;
                    }
                    CountDownLatch enteredLatch = entered;
                    if (enteredLatch != null) {
                        enteredLatch.countDown();
                    }
                    CountDownLatch blockLatch = blocked;
                    if (blockLatch != null) {
                        try {
                            if (blockLatch.await(60, TimeUnit.SECONDS) == false) {
                                throw new AssertionError("cleanup scroll block was not released");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                }
            });
        }
    }
}
