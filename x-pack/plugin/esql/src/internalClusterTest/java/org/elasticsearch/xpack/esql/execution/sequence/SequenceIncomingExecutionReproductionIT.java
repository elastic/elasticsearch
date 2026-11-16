/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Derived incoming-build witnesses for finding4 and the separate INLINE STATS caveat; no production hook is added. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceIncomingExecutionReproductionIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "incoming-sequence-execution";
    private static final String SEQUENCE = """
        SEQUENCE BY key WITHIN 30s PRIMARY 1
        (FROM incoming-sequence-execution | WHERE phase=="A"),
        (FROM incoming-sequence-execution | WHERE phase=="B")
        | KEEP key
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CountRequestsPlugin.class);
    }

    /** EXPLAIN must retain a visible plan while doing no PIT opens or candidate searches, like its plain FROM control. */
    public void testFinding4ExplainHasPlanAndNoReaderWork() throws Exception {
        populate();
        try (var counts = new Counts()) {
            try (
                var plain = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("EXPLAIN (FROM " + INDEX + " | KEEP key)"))
                    .actionGet(TimeValue.timeValueSeconds(30))
            ) {
                assertFalse(getValuesList(plain).isEmpty());
            }
            assertEquals("plain EXPLAIN control", List.of(0, 0), counts.values());
            counts.reset();
            try (
                var response = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("EXPLAIN (" + SEQUENCE + ")"))
                    .actionGet(TimeValue.timeValueSeconds(30))
            ) {
                String plan = getValuesList(response).toString().toLowerCase(Locale.ROOT);
                assertTrue("SEQUENCE must remain visible in EXPLAIN", plan.contains("sequence"));
            }
            logger.info("INCOMING finding4 PIT opens/candidate searches={}", counts.values());
            assertEquals("EXPLAIN may not execute the SEQUENCE reader", List.of(0, 0), counts.values());
        }
    }

    /** Ordinary INLINE STATS lowering is a documented double execution, not a reader correctness defect. */
    public void testInlineStatsRecordsTwoExecutionsAndTwoSnapshots() throws Exception {
        populate();
        try (
            var counts = new Counts();
            var response = client().execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest(SEQUENCE + " | INLINE STATS total=COUNT(*) | KEEP key,total").profile(true)
            ).actionGet(TimeValue.timeValueSeconds(30))
        ) {
            assertEquals(List.of(List.of("k", 1L)), getValuesList(response));
            long executions = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .count();
            logger.info("INCOMING INLINE STATS executions={} PIT opens/candidate searches={}", executions, counts.values());
            assertEquals("two serial physical SEQUENCE executions", 2L, executions);
            assertEquals("one PIT and two candidate stage searches per execution", List.of(2, 4), counts.values());
        }
    }

    private void populate() {
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(1, 0)).setMapping("""
            {"properties":{"@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},"phase":{"type":"keyword"}}}
            """));
        prepareIndex(INDEX).setId("A").setSource("@timestamp", "2026-11-16T00:00:00Z", "key", "k", "phase", "A").get();
        prepareIndex(INDEX).setId("B").setSource("@timestamp", "2026-11-16T00:00:01Z", "key", "k", "phase", "B").get();
        refresh(INDEX);
    }

    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService search : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, search.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }

    private static class Counts implements AutoCloseable {
        private static final AtomicReference<Counts> ACTIVE = new AtomicReference<>();
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger searches = new AtomicInteger();

        Counts() {
            assertTrue(ACTIVE.compareAndSet(null, this));
        }

        List<Integer> values() {
            return List.of(opens.get(), searches.get());
        }

        void reset() {
            opens.set(0);
            searches.set(0);
        }

        @Override
        public void close() {
            assertTrue(ACTIVE.compareAndSet(this, null));
        }
    }

    /** Counts actual ordinary action dispatch; it never changes a request, response or listener lifetime. */
    public static class CountRequestsPlugin extends Plugin implements ActionPlugin {
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
                    Counts counts = Counts.ACTIVE.get();
                    if (counts != null) {
                        if (request instanceof OpenPointInTimeRequest) counts.opens.incrementAndGet();
                        if (action.equals(TransportSearchAction.NAME)
                            && request instanceof SearchRequest search
                            && search.getPointInTimeAuthorizationIndices() != null
                            && search.source() != null
                            && search.source().size() != 0) {
                            counts.searches.incrementAndGet();
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}
