/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ParallelHashAggregationOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.ExtensiblePlugin.ExtensionLoader;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.spill.SpillSettings;
import org.elasticsearch.xpack.esql.spill.SpillStats;
import org.elasticsearch.xpack.esql.spill.disk.DiskSpillPlugin;
import org.elasticsearch.xpack.esql.spill.disk.DiskSpillSettings;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.ToLongFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Spilling across the stages of a query on a cluster with two data nodes, ten shards and late materialization in the
 * node-level reduction, over the disk store behind the failure-injecting provider: the reduce stage spills on both
 * aggregation routes, on the Top-N and on {@code LIMIT BY}, with the results of a run that fits; the data drivers'
 * aggregations never spill; the same query trips the breaker without the feature; cancellation, the disk cap, a
 * failing store, a node restart and the kill switch leave every node clean; and turning node reduce off moves the
 * spill to the coordinator rather than losing it. The invariants of every exit path are checked by {@link #assertClean}:
 * the spill directories empty, the stores with nothing in use and no scope open, the breakers back at zero.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class EsqlSpillNodeReduceIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "test";
    private static final int SHARDS = 10;
    private static final int GROUPS = 3_000;
    private static final int DOCS = 50_000;
    private static final int PAYLOAD_BYTES = 1_000;
    /** Rows of the sort with a limit: well over the request breaker without spilling, with the payload of each. */
    private static final int SORT_LIMIT = 45_000;
    /** The query whose result, the payload of every row, is what goes to the store by the tens of megabytes. */
    private static final String SORT_QUERY = "FROM " + INDEX + " | SORT sorted DESC, key | LIMIT " + SORT_LIMIT + " | KEEP key, payload";
    /** The payload of every document, by document number: the sort's expected rows are computed from it. */
    private static final List<String> PAYLOADS = new ArrayList<>();
    /** The query whose operators spill on every stage: little data, a great many runs; the limit returns every group. */
    private static final String STATS_QUERY = "FROM " + INDEX + " | STATS s = SUM(sorted) BY group | LIMIT " + 2 * GROUPS;

    /**
     * The ES|QL plugin with the store providers loaded and a block factory that names the blocks left behind, so a
     * leak the breaker check finds is attributed to its allocation.
     */
    public static class EsqlSpillTestPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }

        @Override
        public void loadExtensions(ExtensionLoader loader) {
            loadSpillStoreProviders(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        for (Class<? extends Plugin> plugin : super.nodePlugins()) {
            plugins.add(plugin == EsqlPluginWithEnterpriseOrTrialLicense.class ? EsqlSpillTestPlugin.class : plugin);
        }
        plugins.add(DiskSpillPlugin.class);
        plugins.add(CrankySpillStorePlugin.class);
        // registers the inactive-sink interval the node settings shorten
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled()) {
            // registered behind the flag only; a release build would refuse the settings before the test could skip
            settings.put(SpillSettings.ENABLED.getKey(), true)
                .put(SpillSettings.STORE.getKey(), CrankySpillStorePlugin.NAME)
                .put(DiskSpillSettings.CAP.getKey(), "512mb");
        }
        return settings.put(PlannerSettings.REDUCTION_LATE_MATERIALIZATION.getKey(), true)
            .put(AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getKey(), 100_000)
            // a failed query leaves its sinks to the reaper, and a data node's scope closes with its last sink
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueSeconds(between(2, 3)))
            // small enough that the sort's result would trip it without spilling, large enough for the rest
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "40mb")
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .build();
    }

    @Before
    public void setupIndex() throws Exception {
        assumeTrue("the spill flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        if (indexExists(INDEX)) {
            // the suite's cluster keeps the index across the tests
            ensureGreen(INDEX);
            return;
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0))
                .setMapping("group", "type=long", "key", "type=keyword", "sorted", "type=long", "payload", "type=keyword")
        );
        // group: a few thousand distinct values, so every node-local group has rows on every shard and the reduce
        // stage merges by ten or more rows per key; key: unique per document; payload: a kilobyte, so rows are wide
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        PAYLOADS.clear();
        for (int doc = 0; doc < DOCS; doc++) {
            String payload = randomAlphaOfLength(PAYLOAD_BYTES);
            PAYLOADS.add(payload);
            // sorted is a permutation of the document numbers, 7919 being prime to their count: no ties
            bulk.add(
                client().prepareIndex(INDEX).setSource("group", doc % GROUPS, "key", "k" + doc, "sorted", sortedOf(doc), "payload", payload)
            );
            if (bulk.numberOfActions() == 5_000) {
                assertFalse(bulk.get().hasFailures());
                bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
        }
        if (bulk.numberOfActions() > 0) {
            assertFalse(bulk.get().hasFailures());
        }
        ensureGreen(INDEX);
    }

    /**
     * A budget every driver crosses at its first page, no own-state minimum, partitioning from a few hundred keys on:
     * every stage spills whatever it holds. Pages of a few hundred rows, so what the exchange buffers of the wide rows
     * stays well under the breaker while the result goes to its run.
     */
    private static QueryPragmas spilling() {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.SPILL_BUDGET.getKey(), "1kb")
                .put(QueryPragmas.SPILL_MIN_BYTES.getKey(), "0b")
                .put(QueryPragmas.SPILL_RESULT_BUDGET.getKey(), "1mb")
                .put(QueryPragmas.PAGE_SIZE.getKey(), between(100, 300))
                .put(PlannerSettings.AGG_PARTITIONING_COUNT_THRESHOLD.getKey(), between(100, 500))
                .build()
        );
    }

    /**
     * A budget no operator crosses: the reference run, whose result the collector may still send to a run.
     */
    private static QueryPragmas reference() {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.SPILL_BUDGET.getKey(), "1gb")
                .put(QueryPragmas.SPILL_RESULT_BUDGET.getKey(), "1mb")
                .put(QueryPragmas.PAGE_SIZE.getKey(), between(100, 300))
                .build()
        );
    }

    private static QueryPragmas with(QueryPragmas pragmas, String key, Object value) {
        return new QueryPragmas(Settings.builder().put(pragmas.getSettings()).put(key, value.toString()).build());
    }

    private static EsqlQueryRequest request(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        return request;
    }

    private EsqlQueryResponse run(String query, QueryPragmas pragmas) {
        return run(request(query, pragmas), TimeValue.timeValueMinutes(5));
    }

    public void testReduceAggregationSpillsOnBothRoutes() throws Exception {
        // MAX over longs partitions, so the slice route; SUM over longs does not, so the freeze route
        for (String function : List.of("MAX(sorted)", "SUM(sorted)")) {
            String query = "FROM " + INDEX + " | STATS m = " + function + ", c = COUNT(*) BY group | LIMIT " + 2 * GROUPS;
            try (EsqlQueryResponse spilled = run(query, spilling()); EsqlQueryResponse expected = run(query, reference())) {
                assertSameRows(spilled, expected, true);
                String stages = "with " + function + ": " + spilledByStage(spilled);
                // the freeze route spills on every stage that aggregates; the slice route where the partitioned blocks
                // land, which is the final stage here, the reduce passing them on
                if (function.startsWith("SUM")) {
                    assertThat("the reduce stage spilled " + stages, spilledBytes(spilled, "node_reduce"), greaterThan(0L));
                }
                assertThat("the final stage spilled " + stages, spilledBytes(spilled, "final"), greaterThan(0L));
                assertThat("the data drivers' aggregations never spill " + stages, spilledBytes(spilled, "data"), equalTo(0L));
            }
            assertClean();
        }
    }

    /**
     * The sort cannot run without spilling under this breaker, which is the point of {@link #testWithoutSpillTheSameQueryTrips},
     * so its expected rows are computed from the documents rather than taken from a reference run.
     */
    public void testReduceTopNSpillsWithTheRaisedLimit() throws Exception {
        try (EsqlQueryResponse spilled = run(SORT_QUERY, spilling())) {
            assertSameRows(spilled, expectedSortRows(), false);
            // keys and document references only under late materialization, the full rows on the coordinator
            assertThat(spilledBytes(spilled, "node_reduce"), greaterThan(0L));
            assertThat(spilledBytes(spilled, "final"), greaterThan(0L));
            assertThat("the result went to a run too", spilled.isSpilled(), equalTo(true));
        }
        assertClean();
    }

    public void testLimitBySpillsOnTheReduce() throws Exception {
        String query = "FROM " + INDEX + " | LIMIT 3 BY group | STATS rows = COUNT(*), groups = COUNT_DISTINCT(group)";
        try (EsqlQueryResponse spilled = run(query, spilling()); EsqlQueryResponse expected = run(query, reference())) {
            assertSameRows(spilled, expected, true);
            assertThat(getValuesList(spilled).get(0), equalTo(List.of(3L * GROUPS, (long) GROUPS)));
            assertThat(spilledBytes(spilled, "node_reduce"), greaterThan(0L));
        }
        assertClean();
    }

    public void testNodeReduceOffMovesTheSpillToTheCoordinator() throws Exception {
        try (EsqlQueryResponse spilled = run(STATS_QUERY, with(spilling(), QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), false))) {
            assertThat(spilledBytes(spilled, "node_reduce"), equalTo(0L));
            assertThat(spilledBytes(spilled, "final"), greaterThan(0L));
            assertThat(getValuesList(spilled), hasSize(GROUPS));
        }
        assertClean();
    }

    /**
     * The point of the feature, at cluster level: the sort's result is wider than the request breaker, so without
     * spilling the breaker stops it with the memory message, not the disk one.
     */
    public void testWithoutSpillTheSameQueryTrips() throws Exception {
        updateClusterSettings(Settings.builder().put(SpillSettings.ENABLED.getKey(), false));
        try {
            Exception e = expectThrows(Exception.class, () -> run(SORT_QUERY, spilling()).close());
            Throwable breaking = ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
            assertThat("the memory message: " + ExceptionsHelper.stackTrace(e), breaking, notNullValue());
            assertThat(breaking.getMessage(), containsString("request"));
        } finally {
            updateClusterSettings(Settings.builder().putNull(SpillSettings.ENABLED.getKey()));
        }
        assertClean();
    }

    public void testCancelWhileTheReduceSpills() throws Exception {
        // held back enough that the sort is still running when the cancellation comes
        slowStore(50);
        try {
            ActionFuture<EsqlQueryResponse> response = client().execute(EsqlQueryAction.INSTANCE, request(SORT_QUERY, spilling()));
            assertBusy(() -> assertThat(runningSpilledBytes("node_reduce"), greaterThan(0L)), 1, TimeUnit.MINUTES);
            cancelQuery();
            Exception e = expectThrows(Exception.class, response);
            assertThat(
                "cancelled: " + ExceptionsHelper.stackTrace(e),
                ExceptionsHelper.unwrap(e, TaskCancelledException.class),
                notNullValue()
            );
        } finally {
            slowStore(0);
        }
        assertClean();
    }

    public void testDiskCapFailsOnlyTheSpillingQuery() throws Exception {
        updateClusterSettings(Settings.builder().put(DiskSpillSettings.CAP.getKey(), "1mb"));
        try {
            // the sort's rows are what cross a cap of a megabyte; an aggregation's state is far smaller. The limit
            // exception is not registered for the transport, so one raised on a data node arrives as its message
            Exception e = expectThrows(Exception.class, () -> run(SORT_QUERY, spilling()).close());
            assertThat("the disk message", ExceptionsHelper.stackTrace(e), containsString(DiskSpillSettings.CAP.getKey()));
            // a query that spills nothing is untouched
            try (EsqlQueryResponse small = run("FROM " + INDEX + " | STATS c = COUNT(*)", spilling())) {
                assertThat(getValuesList(small), equalTo(List.of(List.of((long) DOCS))));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(DiskSpillSettings.CAP.getKey()));
        }
        assertClean();
    }

    /**
     * The failure-injecting store over real drivers: every run either succeeds with the right rows or fails naming
     * the injection, and leaves every node clean; at least one injection happened, else the runs proved nothing.
     */
    public void testCrankyStoreOnTheReduce() throws Exception {
        long seed = randomLong();
        // the sort issues some thousand operations per run: at this rate a run sees about one injection, so some
        // runs come through and some fail, and eight runs see one at least
        updateClusterSettings(
            Settings.builder().put(CrankySpillStorePlugin.FAILURE_RATE.getKey(), 0.001).put(CrankySpillStorePlugin.SEED.getKey(), seed)
        );
        try {
            for (int i = 0; i < 8; i++) {
                try (EsqlQueryResponse response = run(SORT_QUERY, spilling())) {
                    assertSameRows(response, expectedSortRows(), false);
                } catch (Exception e) {
                    // the injection, or the breaker the suite keeps small, which a run held back may reach
                    assertThat(
                        "seed [" + seed + "]: " + ExceptionsHelper.stackTrace(e),
                        ExceptionsHelper.stackTrace(e),
                        either(containsString("cranky spill")).or(containsString("Data too large"))
                    );
                }
                assertClean();
            }
            assertThat("seed [" + seed + "] injected nothing", crankyTotal(CrankySpillStore::injections), greaterThan(0L));
        } finally {
            updateClusterSettings(
                Settings.builder().putNull(CrankySpillStorePlugin.FAILURE_RATE.getKey()).putNull(CrankySpillStorePlugin.SEED.getKey())
            );
        }
        assertThat("the callers kept the store's contract", crankyTotal(CrankySpillStore::violations), equalTo(0L));
    }

    /**
     * The kill switch applies to queries started afterwards: one in flight finishes its spills and reads them back,
     * one started after it never spills.
     */
    public void testTheKillSwitchAppliesToLaterQueries() throws Exception {
        slowStore(2);
        try {
            ActionFuture<EsqlQueryResponse> inFlight = client().execute(EsqlQueryAction.INSTANCE, request(SORT_QUERY, spilling()));
            assertBusy(() -> assertThat(runningSpilledBytes("node_reduce"), greaterThan(0L)), 1, TimeUnit.MINUTES);
            updateClusterSettings(Settings.builder().put(SpillSettings.ENABLED.getKey(), false));
            try {
                try (EsqlQueryResponse response = inFlight.actionGet(5, TimeUnit.MINUTES)) {
                    assertThat(getValuesList(response), hasSize(SORT_LIMIT));
                    assertThat("the query that started with the switch on spilled", spilledBytes(response, "final"), greaterThan(0L));
                }
                // a query whose state fits the breaker, since without the feature the breaker is all there is
                try (EsqlQueryResponse later = run(STATS_QUERY, spilling())) {
                    assertThat(getValuesList(later), hasSize(GROUPS));
                    for (String stage : List.of("data", "node_reduce", "final")) {
                        assertThat("a query started with the switch off never spills", spilledBytes(later, stage), equalTo(0L));
                    }
                }
            } finally {
                updateClusterSettings(Settings.builder().putNull(SpillSettings.ENABLED.getKey()));
            }
        } finally {
            slowStore(0);
        }
        assertClean();
    }

    /**
     * A node restarted with runs on its disk sweeps them at start; the query fails, every node ends clean.
     */
    public void testANodeRestartSweepsItsRuns() throws Exception {
        // held back enough that the sort is still spilling when the node goes
        slowStore(50);
        String dataNode = null;
        try {
            ActionFuture<EsqlQueryResponse> response = client().execute(EsqlQueryAction.INSTANCE, request(SORT_QUERY, spilling()));
            Map<String, Long> spilledByNode = new HashMap<>();
            assertBusy(() -> {
                spilledByNode.clear();
                runningDrivers("node_reduce", (node, status) -> spilledByNode.merge(node, spilledBytes(status), Long::sum));
                assertThat(spilledByNode.values().stream().mapToLong(Long::longValue).sum(), greaterThan(0L));
            }, 1, TimeUnit.MINUTES);
            dataNode = spilledByNode.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
            internalCluster().restartNode(dataNode);
            // the node's going ends the query; one still running after the restart is cancelled, and the sweep of the
            // restarted node's directory is what the clean check then proves
            if (response.isDone() == false) {
                cancelQueryIfRunning();
            }
            expectThrows(Exception.class, () -> response.actionGet(2, TimeUnit.MINUTES));
        } finally {
            slowStore(0);
        }
        // the next test's queries must find the restarted node's shards, from every node's view of the cluster
        ensureStableCluster(internalCluster().size());
        ensureGreen(INDEX);
        assertClean();
    }

    /**
     * Several queries spilling at once share the disk cap: with a cap they fit under, every result is right; with a
     * cap they cannot share, whichever fails names the cap and the rest are right, and nothing is left behind.
     */
    public void testConcurrentQueriesShareTheCap() throws Exception {
        // two at once: each holds some ten megabytes of buffers on a data node while its rows go to the store
        List<ActionFuture<EsqlQueryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(client().execute(EsqlQueryAction.INSTANCE, request(SORT_QUERY, spilling())));
        }
        for (ActionFuture<EsqlQueryResponse> future : futures) {
            try (EsqlQueryResponse response = future.actionGet(5, TimeUnit.MINUTES)) {
                assertThat(getValuesList(response), hasSize(SORT_LIMIT));
            }
        }
        assertClean();

        // room for one result and some of the other: one of the two fails naming the cap, or both do, and nothing is left
        updateClusterSettings(Settings.builder().put(DiskSpillSettings.CAP.getKey(), "60mb"));
        try {
            futures.clear();
            for (int i = 0; i < 2; i++) {
                futures.add(client().execute(EsqlQueryAction.INSTANCE, request(SORT_QUERY, spilling())));
            }
            for (ActionFuture<EsqlQueryResponse> future : futures) {
                try (EsqlQueryResponse response = future.actionGet(5, TimeUnit.MINUTES)) {
                    assertThat(getValuesList(response), hasSize(SORT_LIMIT));
                } catch (Exception e) {
                    assertThat("the disk message", ExceptionsHelper.stackTrace(e), containsString(DiskSpillSettings.CAP.getKey()));
                }
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(DiskSpillSettings.CAP.getKey()));
        }
        assertClean();
    }

    // helpers

    private static long sortedOf(int doc) {
        return (doc * 7919L) % DOCS;
    }

    /** The rows of {@link #SORT_QUERY}: the documents by their sort value descending, the first {@link #SORT_LIMIT}. */
    private static List<List<Object>> expectedSortRows() {
        List<Integer> docs = new ArrayList<>();
        for (int doc = 0; doc < DOCS; doc++) {
            docs.add(doc);
        }
        docs.sort(Comparator.comparingLong((Integer doc) -> sortedOf(doc)).reversed());
        List<List<Object>> rows = new ArrayList<>();
        for (int doc : docs.subList(0, SORT_LIMIT)) {
            rows.add(List.of("k" + doc, PAYLOADS.get(doc)));
        }
        return rows;
    }

    private static void assertSameRows(EsqlQueryResponse actual, EsqlQueryResponse expected, boolean unordered) {
        assertSameRows(actual, getValuesList(expected), unordered);
    }

    private static void assertSameRows(EsqlQueryResponse actual, List<List<Object>> expectedRows, boolean unordered) {
        List<List<Object>> actualRows = getValuesList(actual);
        if (unordered) {
            Comparator<List<Object>> byText = Comparator.comparing(Object::toString);
            actualRows.sort(byText);
            expectedRows.sort(byText);
        }
        assertThat(actualRows.size(), equalTo(expectedRows.size()));
        for (int i = 0; i < expectedRows.size(); i++) {
            assertThat("row " + i, actualRows.get(i), equalTo(expectedRows.get(i)));
        }
    }

    /** The spill counters an operator's status carries, or none. */
    private static SpillStatus spillOf(Operator.Status status) {
        if (status instanceof GroupedLimitOperator.Status grouped) {
            return grouped.spill();
        }
        if (status instanceof TopNOperatorStatus topN) {
            return topN.spill();
        }
        if (status instanceof OutputOperator.Status output) {
            return output.spill();
        }
        if (status instanceof HashAggregationOperator.Status aggregation) {
            SpillStatus sum = SpillStatus.NONE;
            for (Operator.Status.ExtraStatus extra : aggregation.extraFields()) {
                if (extra instanceof ParallelHashAggregationOperator.PartitioningStatus partitioning) {
                    sum = sum.plus(partitioning.spill());
                } else if (extra instanceof HashAggregationOperator.FreezeStatus freeze) {
                    sum = sum.plus(freeze.spill());
                }
            }
            return sum;
        }
        return SpillStatus.NONE;
    }

    private static long spilledBytes(List<OperatorStatus> operators) {
        long sum = 0;
        for (OperatorStatus operator : operators) {
            if (operator.status() != null) {
                sum += spillOf(operator.status()).spilledBytes();
            }
        }
        return sum;
    }

    private static long spilledBytes(DriverStatus status) {
        return spilledBytes(status.completedOperators()) + spilledBytes(status.activeOperators());
    }

    /** Bytes spilled by every driver of the stage, out of the response's profile. */
    private static long spilledBytes(EsqlQueryResponse response, String stage) {
        long sum = 0;
        for (DriverProfile driver : response.profile().drivers()) {
            if (driver.description().equals(stage)) {
                sum += spilledBytes(driver.operators());
            }
        }
        return sum;
    }

    /** Bytes spilled so far by the running drivers of the stage, out of the task statuses. */
    private long runningSpilledBytes(String stage) {
        long[] sum = new long[1];
        runningDrivers(stage, (node, status) -> sum[0] += spilledBytes(status));
        return sum[0];
    }

    private void runningDrivers(String stage, BiConsumer<String, DriverStatus> consumer) {
        List<TaskInfo> tasks = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(DriverTaskRunner.ACTION_NAME)
            .setDetailed(true)
            .get()
            .getTasks();
        for (TaskInfo task : tasks) {
            if (task.status() instanceof DriverStatus status && status.description().equals(stage)) {
                consumer.accept(task.node(), status);
            }
        }
    }

    private void cancelQuery() {
        List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
        assertThat(tasks, hasSize(1));
        cancel(tasks.get(0));
    }

    private void cancelQueryIfRunning() {
        for (TaskInfo task : client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks()) {
            cancel(task);
        }
    }

    private void cancel(TaskInfo task) {
        CancelTasksRequest cancel = new CancelTasksRequest().setTargetTaskId(task.taskId()).setReason("test cancel");
        cancel.setWaitForCompletion(true);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, cancel).actionGet();
    }

    private void slowStore(long maxDelayMillis) {
        Settings.Builder settings = Settings.builder();
        if (maxDelayMillis == 0) {
            settings.putNull(CrankySpillStorePlugin.MAX_DELAY.getKey());
        } else {
            settings.put(CrankySpillStorePlugin.MAX_DELAY.getKey(), TimeValue.timeValueMillis(maxDelayMillis));
        }
        updateClusterSettings(settings);
    }

    private long crankyTotal(ToLongFunction<CrankySpillStore> figure) {
        long sum = 0;
        for (DiscoveryNode node : clusterService().state().nodes()) {
            CrankySpillStore store = CrankySpillStorePlugin.storeOf(node.getId());
            if (store != null) {
                sum += figure.applyAsLong(store);
            }
        }
        return sum;
    }

    /**
     * The invariants of every exit path, on every node: the spill directories empty, the stores with nothing in use
     * and no scope open, the breakers back at zero. Scopes close after the response went out, so the check waits.
     */
    private void assertClean() throws Exception {
        assertBusy(() -> {
            // the figures first: they say whether a scope is still open or a store still counts bytes
            EsqlStatsResponse stats = client().execute(EsqlStatsAction.INSTANCE, new EsqlStatsRequest()).actionGet();
            assertThat(stats.failures(), empty());
            Map<String, String> figures = new HashMap<>();
            for (EsqlStatsResponse.NodeStatsResponse node : stats.getNodes()) {
                Counters counters = node.getStats();
                figures.put(
                    node.getNode().getName(),
                    "scopes="
                        + counters.get(SpillStats.SCOPES)
                        + " in_use="
                        + counters.get(SpillStats.BYTES_IN_USE)
                        + " debt="
                        + counters.get(SpillStats.CLEANUP_DEBT_BYTES)
                );
            }
            for (EsqlStatsResponse.NodeStatsResponse node : stats.getNodes()) {
                Counters counters = node.getStats();
                assertThat("no scope open on " + node.getNode().getName() + ": " + figures, counters.get(SpillStats.SCOPES), equalTo(0L));
                assertThat(
                    "nothing in use on " + node.getNode().getName() + ": " + figures,
                    counters.get(SpillStats.BYTES_IN_USE),
                    equalTo(0L)
                );
                assertThat(
                    "no debt on " + node.getNode().getName() + ": " + figures,
                    counters.get(SpillStats.CLEANUP_DEBT_BYTES),
                    equalTo(0L)
                );
            }
            for (Environment environment : internalCluster().getInstances(Environment.class)) {
                Path root = environment.dataDirs()[0].resolve(DiskSpillSettings.DEFAULT_DIRECTORY);
                if (Files.isDirectory(root) == false) {
                    continue;
                }
                try (DirectoryStream<Path> nodes = Files.newDirectoryStream(root)) {
                    for (Path node : nodes) {
                        assertThat("spill directory [" + node + "] is empty: " + figures, entries(node), empty());
                    }
                }
            }
        }, 1, TimeUnit.MINUTES);
        ensureBlocksReleased();
    }

    /** Bytes spilled per stage, for a message. */
    private static Map<String, Long> spilledByStage(EsqlQueryResponse response) {
        Map<String, Long> byStage = new HashMap<>();
        for (DriverProfile driver : response.profile().drivers()) {
            byStage.merge(driver.description(), spilledBytes(driver.operators()), Long::sum);
        }
        return byStage;
    }

    private static List<Path> entries(Path directory) throws IOException {
        List<Path> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                entries.add(entry);
            }
        }
        return entries;
    }
}
