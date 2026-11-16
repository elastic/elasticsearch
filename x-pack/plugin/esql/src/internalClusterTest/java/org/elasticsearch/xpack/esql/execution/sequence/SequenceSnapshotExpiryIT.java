/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Real PIT reaping qualifies the idle-lease diagnostic without making the test wait five minutes. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SequenceSnapshotExpiryIT extends AbstractSequenceIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), LeaseProbePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(ordinal, otherSettings)).put("search.keep_alive_interval", "100ms").build();
    }

    /** The first real search waits while a real, shortened PIT lease expires; no reader is destroyed by the fixture. */
    public void testIdleSnapshotExpiryExplainsLeaseWithoutClaimingCause() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("sequence-expiry-events").setSettings(indexSettings(1, 0)).setMapping("""
            {"properties":{"@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"}}}
            """));
        prepareIndex("sequence-expiry-events").setId("A")
            .setSource("@timestamp", "2026-11-15T00:00:00.000000001Z", "key", "shared", "phase", "A")
            .get();
        prepareIndex("sequence-expiry-events").setId("B")
            .setSource("@timestamp", "2026-11-15T00:00:01.000000001Z", "key", "shared", "phase", "B")
            .get();
        refresh("sequence-expiry-events");
        for (SearchService search : internalCluster().getInstances(SearchService.class)) {
            assertEquals(0, search.getActiveContexts());
        }
        var future = new PlainActionFuture<Void>();
        try (var gate = new LeaseGate()) {
            client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("""
                SEQUENCE BY key WITHIN 30s PRIMARY 2
                (FROM sequence-expiry-events | WHERE phase=="A"),
                (FROM sequence-expiry-events | WHERE phase=="B")
                | KEEP key | LIMIT 10
                """).allowPartialResults(false), ActionListener.<EsqlQueryResponse>wrap(response -> {
                response.close();
                future.onResponse(null);
            }, future::onFailure));
            assertTrue("first native PIT search was not parked", gate.started.await(30, TimeUnit.SECONDS));
            assertEquals(TimeValue.timeValueMinutes(5), gate.openLease);
            assertEquals(TimeValue.timeValueMinutes(5), gate.searchLease);
            int contextsBeforeExpiry = 0;
            for (SearchService search : internalCluster().getInstances(SearchService.class)) {
                contextsBeforeExpiry += search.getActiveContexts();
            }
            assertTrue("a real PIT reader existed before idle expiry", contextsBeforeExpiry > 0);
            assertBusy(() -> {
                for (SearchService search : internalCluster().getInstances(SearchService.class)) {
                    assertEquals(0, search.getActiveContexts());
                }
            }, 10, TimeUnit.SECONDS);
            gate.release();
            Exception failure = expectThrows(Exception.class, () -> {
                future.actionGet(DEFAULT_REQUEST_TIMEOUT);
                fail("an expired snapshot must not return partial rows");
            });
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("SEQUENCE point-in-time snapshot is no longer available"));
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("5 minutes without a read"));
        } finally {
            if (future.isDone() == false) {
                try {
                    future.actionGet(DEFAULT_REQUEST_TIMEOUT);
                } catch (Exception ignored) {
                    // The fixture must wait for query completion even when an earlier assertion failed.
                }
            }
        }
        ensureBlocksReleased();
    }

    /** Closing the fixture always resumes the parked real action, including failures in the assertions above. */
    private static final class LeaseGate implements AutoCloseable {
        private static final AtomicReference<LeaseGate> ACTIVE = new AtomicReference<>();
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch handled = new CountDownLatch(1);
        private volatile TimeValue openLease;
        private volatile TimeValue searchLease;
        private boolean released;
        private Runnable continuation;

        private LeaseGate() {
            assertTrue(ACTIVE.compareAndSet(null, this));
        }

        private void install(Runnable next) {
            boolean run;
            synchronized (this) {
                run = released;
                if (run == false) continuation = next;
            }
            started.countDown();
            if (run) next.run();
        }

        private void release() {
            Runnable next;
            synchronized (this) {
                released = true;
                next = continuation;
                continuation = null;
            }
            if (next != null) next.run();
        }

        @Override
        public void close() throws InterruptedException {
            ACTIVE.compareAndSet(this, null);
            release();
            if (claimed.get()) {
                assertTrue("parked search did not complete after release", handled.await(30, TimeUnit.SECONDS));
            }
        }
    }

    /** Only this test's native requests are altered; the real search and reaper retain their ordinary implementation. */
    public static class LeaseProbePlugin extends Plugin implements ActionPlugin {
        private volatile ThreadPool threadPool;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            threadPool = services.threadPool();
            return List.of();
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    LeaseGate gate = LeaseGate.ACTIVE.get();
                    if (gate == null) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    if (request instanceof OpenPointInTimeRequest open) {
                        gate.openLease = open.keepAlive();
                        open.keepAlive(TimeValue.timeValueSeconds(5));
                    }
                    if (action.equals(TransportSearchAction.NAME) == false
                        || request instanceof SearchRequest == false
                        || ((SearchRequest) request).getPointInTimeAuthorizationIndices() == null
                        || gate.claimed.compareAndSet(false, true) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    SearchRequest search = (SearchRequest) request;
                    gate.searchLease = search.source().pointInTimeBuilder().getKeepAlive();
                    request.mustIncRef();
                    var terminal = ActionListener.notifyOnce(ActionListener.runAfter(listener, () -> {
                        try {
                            request.decRef();
                        } finally {
                            gate.handled.countDown();
                        }
                    }));
                    try {
                        Supplier<ThreadContext.StoredContext> context = threadPool.getThreadContext().newRestorableContext(true);
                        gate.install(() -> {
                            try (var ignored = context.get()) {
                                chain.proceed(task, action, request, terminal);
                            } catch (Exception failure) {
                                terminal.onFailure(failure);
                            }
                        });
                    } catch (Exception failure) {
                        terminal.onFailure(failure);
                    }
                }
            });
        }
    }
}
