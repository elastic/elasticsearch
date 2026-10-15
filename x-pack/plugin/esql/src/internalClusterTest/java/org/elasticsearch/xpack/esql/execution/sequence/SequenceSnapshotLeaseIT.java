/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Real shard expiry proves that a live source preserves its snapshot across stage and downstream pauses. */
@Nightly // Real elapsed expiry is intentionally excluded from the ordinary fast test group.
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceSnapshotLeaseIT extends AbstractEsqlIntegTestCase {
    private static final TimeValue LEASE = TimeValue.timeValueSeconds(3);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), "100ms")
            .build();
    }

    public void testRenewalSurvivesStageAndDownstreamPausesBeyondActualExpiry() throws Exception {
        createEvents();
        try (var harness = new Harness()) {
            // Only terminal delivery is delayed. The stage planner, compute drivers, client and shard readers are real.
            while (harness.deferredStage.get() == null) {
                assertNull(harness.source.getOutput());
                if (harness.deferredStage.get() == null) {
                    harness.awaitUnblocked();
                }
            }
            BytesReference unrenewed = client().execute(
                TransportOpenPointInTimeAction.TYPE,
                new OpenPointInTimeRequest("a", "b").keepAlive(LEASE)
            ).actionGet().getPointInTimeId();
            try {
                // Real elapsed time and a negative control distinguish lease renewal from merely delayed reaping.
                Thread.sleep(LEASE.millis() * 3);
                Exception expired = expectThrows(Exception.class, () -> {
                    var response = client().prepareSearch()
                        .setSource(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(unrenewed).setKeepAlive(LEASE)))
                        // The randomized test client can add a preference before the PIT is attached; PIT searches forbid it.
                        .setPreference(null)
                        .setAllowPartialSearchResults(false)
                        .get();
                    response.decRef();
                });
                assertThat(ExceptionsHelper.stackTrace(expired), containsString("No search context found"));
                harness.deferredStage.getAndSet(null).onResponse(null);
                Page first = harness.nextPage();
                try (Releasable ignored = first::releaseBlocks) {
                    assertKey(first, "k1");
                    // Do not pull more output while a downstream consumer retains the first page.
                    Thread.sleep(LEASE.millis() * 3);
                    Page second = harness.nextPage();
                    try (Releasable ignoredSecond = second::releaseBlocks) {
                        assertKey(second, "k2");
                    }
                }
            } finally {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(unrenewed)).actionGet();
            }
        }
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
    }

    private static void assertKey(Page page, String key) {
        assertEquals(1, page.getPositionCount());
        assertEquals(1, page.getBlockCount());
        assertEquals(key, page.<BytesRefBlock>getBlock(0).getBytesRef(0, new BytesRef()).utf8ToString());
    }

    private void createEvents() {
        List<String> nodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .sorted()
            .toList();
        Instant base = Instant.parse("2026-09-25T21:14:51.389Z");
        for (int stage = 0; stage < 2; stage++) {
            String index = stage == 0 ? "a" : "b";
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.routing.allocation.include._name", nodes.get(stage))
                    )
                    .setMapping("@timestamp", "type=date", "key", "type=keyword")
            );
            for (int event = 0; event < 2; event++) {
                prepareIndex(index).setId("event-" + event)
                    .setSource("@timestamp", base.plusMillis(2L * event + stage).toString(), "key", "k" + (event + 1))
                    .get();
            }
        }
        ensureGreen("a", "b");
        refresh("a", "b");
    }

    /** Owns actual compute resources while allowing the consumer to stop pulling output. */
    private final class Harness implements Releasable {
        private final CircuitBreaker breaker;
        private final MockBlockFactory blocks;
        private final DriverContext context;
        private final SequenceSourceOperator source;
        private final AtomicInteger stages = new AtomicInteger();
        private final AtomicReference<ActionListener<Void>> deferredStage = new AtomicReference<>();

        private Harness() {
            Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb")
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build();
            breaker = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getBreaker(CircuitBreaker.REQUEST);
            blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
            context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blocks, null);
            Map<String, EsField> mapping = Map.of(
                "@timestamp",
                DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
                "key",
                new EsField("key", DataType.KEYWORD, Map.of(), true, NONE)
            );
            var analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(TransportVersion.current());
            for (String index : List.of("a", "b")) {
                analyzer.addIndex(
                    new EsIndex(index, mapping, Map.of(index, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of())
                );
            }
            Sequence sequence = analyzer.query("""
                sequence by key with {"maxspan":"30s"} (from a | keep key), (from b | keep key) | limit 10
                """).collect(Sequence.class).getFirst();
            source = new SequenceSourceOperator(
                new SequenceSourceOperator.Factory(
                    new SequenceExec(sequence.source(), sequence),
                    client(),
                    client(),
                    null,
                    EsqlTestUtils.TEST_CFG,
                    this::planStage,
                    this::executeStage,
                    client().threadPool().getThreadContext().newRestorableContext(true),
                    null
                ),
                context,
                LEASE
            );
        }

        private List<Driver> planStage(PhysicalPlan physical, Consumer<Page> output) {
            Settings settings = Settings.builder()
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
                .build();
            var planner = new LocalExecutionPlanner(
                "sequence-lease",
                "",
                null,
                BigArrays.NON_RECYCLING_INSTANCE,
                blocks,
                settings,
                EsqlTestUtils.TEST_CFG,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                ProjectMetadata.builder(ProjectId.DEFAULT).build(),
                new EsPhysicalOperationProviders(
                    FoldContext.small(),
                    EmptyIndexedByShardId.instance(),
                    null,
                    PlannerSettings.DEFAULTS,
                    () -> 0L,
                    QueryWarnings.EMIT
                ),
                null,
                null,
                null,
                0,
                MatcherWatchdog.noop(),
                TransportVersion.current()
            );
            return planner.plan(
                "sequence stage",
                FoldContext.small(),
                PlannerSettings.DEFAULTS,
                new OutputExec(physical, output),
                EmptyIndexedByShardId.instance(),
                true
            ).createDrivers("sequence-lease");
        }

        private void executeStage(List<Driver> drivers, ActionListener<Void> listener) {
            assertEquals(1, drivers.size());
            var completed = new PlainActionFuture<Void>();
            Driver.start(
                client().threadPool().getThreadContext(),
                Runnable::run,
                drivers.getFirst(),
                Driver.DEFAULT_MAX_ITERATIONS,
                completed
            );
            assertTrue(completed.isDone());
            completed.actionGet();
            if (stages.incrementAndGet() == 1) {
                assertTrue(deferredStage.compareAndSet(null, listener));
            } else {
                listener.onResponse(null);
            }
        }

        private void awaitUnblocked() {
            var done = new PlainActionFuture<Void>();
            source.isBlocked().listener().addListener(done);
            done.actionGet(TimeValue.timeValueSeconds(30));
        }

        private Page nextPage() {
            long deadline = System.nanoTime() + TimeValue.timeValueSeconds(30).nanos();
            while (source.isFinished() == false) {
                assertTrue("source made no progress within the test deadline", System.nanoTime() < deadline);
                Page page = source.getOutput();
                if (page != null) {
                    return page;
                }
                awaitUnblocked();
            }
            throw new AssertionError("source exhausted before the expected sequence");
        }

        @Override
        public void close() {
            source.close();
            var deferred = deferredStage.getAndSet(null);
            if (deferred != null) {
                deferred.onResponse(null);
            }
            context.finish();
            var done = new PlainActionFuture<Void>();
            context.waitForAsyncActions(done);
            done.actionGet(TimeValue.timeValueSeconds(30));
            blocks.ensureAllBlocksAreReleased();
            assertEquals(0, breaker.getUsed());
        }
    }
}
