/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.ParallelHashAggregationOperator;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.spill.SpillSettings;
import org.elasticsearch.xpack.esql.spill.disk.DiskSpillPlugin;
import org.elasticsearch.xpack.esql.spill.disk.DiskSpillSettings;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * The first real-disk spill: {@code LIMIT BY} over a key set larger than the query's budget, with the disk module
 * installed and spilling on. The operator runs on the data nodes and on the coordinator, and spills at both; the
 * results are those of a run that fits; the spill directory of every node is empty once the response is closed.
 */
public class EsqlSpillIT extends AbstractEsqlIntegTestCase {

    /**
     * The ES|QL plugin of the test cluster takes the spill store providers from the extension loader, which the base
     * test plugin bypasses; the loader finds the disk module installed next to it.
     */
    public static class EsqlPluginWithSpill extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensionLoader loader) {
            loadSpillStoreProviders(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        for (Class<? extends Plugin> plugin : super.nodePlugins()) {
            plugins.add(plugin == EsqlPluginWithEnterpriseOrTrialLicense.class ? EsqlPluginWithSpill.class : plugin);
        }
        plugins.add(DiskSpillPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled()) {
            // registered behind the flag only; a release build would refuse the setting before the test could skip
            settings.put(SpillSettings.ENABLED.getKey(), true);
        }
        return settings.build();
    }

    public void testLimitBySpillsOnEveryStageAndCleansUp() throws Exception {
        assumeTrue("the spill flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        String index = "spill_limit_by";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", between(1, 3)).put("index.number_of_replicas", 0))
                .setMapping("key", "type=long", "value", "type=keyword")
        );
        int keys = between(2_000, 5_000);
        int rowsPerKey = 4;
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int row = 0; row < keys * rowsPerKey; row++) {
            bulk.add(client().prepareIndex(index).setSource("key", row % keys, "value", "v" + row));
        }
        assertFalse(bulk.get().hasFailures());

        // a budget every driver crosses at its first page and no own-state minimum, so that every stage's operator
        // freezes, the data node drivers with their few keys each included
        Settings pragmas = Settings.builder()
            .put(QueryPragmas.SPILL_BUDGET.getKey(), "1kb")
            .put(QueryPragmas.SPILL_MIN_BYTES.getKey(), "0b")
            .put(QueryPragmas.PAGE_SIZE.getKey(), between(50, 500))
            .build();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(
            "FROM " + index + " | LIMIT 2 BY key | STATS c = COUNT(*) BY key | STATS keys = COUNT(*), rows = SUM(c), most = MAX(c)"
        );
        request.pragmas(new QueryPragmas(pragmas));
        request.acceptedPragmaRisks(true);
        request.profile(true);
        // every stage replays through the depth bound here, so the query is slow by design
        try (EsqlQueryResponse response = run(request, TimeValue.timeValueMinutes(3))) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            assertThat("every key survives", values.get(0).get(0), equalTo((long) keys));
            assertThat("two rows per key", values.get(0).get(1), equalTo(2L * keys));
            assertThat(values.get(0).get(2), equalTo(2L));

            List<GroupedLimitOperator.Status> dataNode = new ArrayList<>();
            List<GroupedLimitOperator.Status> other = new ArrayList<>();
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof GroupedLimitOperator.Status status) {
                        (driver.description().contains("data") ? dataNode : other).add(status);
                    }
                }
            }
            assertThat("the operator runs on the data nodes", dataNode, hasSize(greaterThan(0)));
            assertThat("the operator runs after the exchange too", other, hasSize(greaterThan(0)));
            assertThat(
                "a data node driver spilled: " + dataNode,
                dataNode.stream().mapToLong(GroupedLimitOperator.Status::rowsSpilled).sum(),
                greaterThan(0L)
            );
            assertThat(
                "a later stage spilled: " + other,
                other.stream().mapToLong(GroupedLimitOperator.Status::rowsSpilled).sum(),
                greaterThan(0L)
            );
            long spilledBytes = 0;
            for (GroupedLimitOperator.Status status : dataNode) {
                spilledBytes += status.spill().spilledBytes();
            }
            for (GroupedLimitOperator.Status status : other) {
                spilledBytes += status.spill().spilledBytes();
            }
            assertThat("bytes went to disk", spilledBytes, greaterThan(0L));
        }
        assertSpillDirectoriesEmpty();
    }

    /**
     * The slice route: {@code STATS ... BY} over a key set larger than the budget, with the parallel operator engaged
     * on every stage by a low partitioning threshold. Every generation spills and comes back; the results are those
     * of a run that fits; the spill directories are empty afterwards.
     */
    public void testStatsBySpillsOnEveryStageAndCleansUp() throws Exception {
        assumeTrue("the spill flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        String index = "spill_stats_by";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", between(1, 3)).put("index.number_of_replicas", 0))
                .setMapping("key", "type=long", "value", "type=long")
        );
        int keys = between(2_000, 5_000);
        int rowsPerKey = 3;
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int row = 0; row < keys * rowsPerKey; row++) {
            bulk.add(client().prepareIndex(index).setSource("key", row % keys, "value", row));
        }
        assertFalse(bulk.get().hasFailures());

        // a budget every driver crosses at its first page, no own-state minimum, and partitioning from a few hundred
        // keys on, so that every stage runs the parallel operator and spills every generation; the functions are
        // ones that partition, which SUM over longs does not
        Settings pragmas = Settings.builder()
            .put(QueryPragmas.SPILL_BUDGET.getKey(), "1kb")
            .put(QueryPragmas.SPILL_MIN_BYTES.getKey(), "0b")
            .put(QueryPragmas.PAGE_SIZE.getKey(), between(50, 500))
            .put(PlannerSettings.AGG_PARTITIONING_COUNT_THRESHOLD.getKey(), between(100, 500))
            .build();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(
            "FROM "
                + index
                + " | STATS c = COUNT(*), m = MAX(value) BY key"
                + " | STATS keys = COUNT(*), rows = SUM(c), most = MAX(c), top = MAX(m)"
        );
        request.pragmas(new QueryPragmas(pragmas));
        request.acceptedPragmaRisks(true);
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TimeValue.timeValueMinutes(3))) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            assertThat("every key survives", values.get(0).get(0), equalTo((long) keys));
            assertThat("every row is counted", values.get(0).get(1), equalTo((long) keys * rowsPerKey));
            assertThat(values.get(0).get(2), equalTo((long) rowsPerKey));
            assertThat("the largest value survives", values.get(0).get(3), equalTo((long) keys * rowsPerKey - 1));

            // the data node's partial aggregation emits partitioned blocks rather than promoting itself: the parallel
            // operator runs where those blocks land, the node reduce, and on the coordinator's final aggregation
            Map<String, List<ParallelHashAggregationOperator.PartitioningStatus>> byStage = new HashMap<>();
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof HashAggregationOperator.Status status) {
                        for (Operator.Status.ExtraStatus extra : status.extraFields()) {
                            if (extra instanceof ParallelHashAggregationOperator.PartitioningStatus partitioning) {
                                byStage.computeIfAbsent(driver.description(), k -> new ArrayList<>()).add(partitioning);
                            }
                        }
                    }
                }
            }
            // which stages promote depends on the plan: the coordinator's final aggregation always does here, the node
            // reduce only when the data stage hands it partitioned blocks
            assertThat("the parallel operator ran: " + byStage.keySet(), byStage.size(), greaterThan(0));
            long readBack = 0;
            for (Map.Entry<String, List<ParallelHashAggregationOperator.PartitioningStatus>> stage : byStage.entrySet()) {
                int runs = stage.getValue().stream().mapToInt(s -> s.spill().spillRuns()).sum();
                assertThat("stage [" + stage.getKey() + "] spilled: " + stage.getValue(), runs, greaterThan(0));
                readBack += stage.getValue().stream().mapToLong(s -> s.spill().readBackBytes()).sum();
            }
            assertThat("what went to disk came back", readBack, greaterThan(0L));
        }
        assertSpillDirectoriesEmpty();
    }

    /**
     * The freeze route: {@code STATS ... BY} with a function that does not partition, so the stages that consume
     * intermediate pages freeze their groups under the budget and replay the rest from disk. The results are those
     * of a run that fits and the spill directories are empty afterwards.
     */
    public void testStatsByFreezesAndCleansUp() throws Exception {
        assumeTrue("the spill flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        String index = "spill_stats_freeze";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", between(1, 3)).put("index.number_of_replicas", 0))
                .setMapping("key", "type=long", "value", "type=long")
        );
        int keys = between(2_000, 5_000);
        int rowsPerKey = 3;
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int row = 0; row < keys * rowsPerKey; row++) {
            bulk.add(client().prepareIndex(index).setSource("key", row % keys, "value", row));
        }
        assertFalse(bulk.get().hasFailures());

        // a budget every driver crosses at its first page and no own-state minimum; SUM over longs has no splitter,
        // so no stage takes the slice route and the freeze is what spills
        Settings pragmas = Settings.builder()
            .put(QueryPragmas.SPILL_BUDGET.getKey(), "1kb")
            .put(QueryPragmas.SPILL_MIN_BYTES.getKey(), "0b")
            .put(QueryPragmas.PAGE_SIZE.getKey(), between(50, 500))
            .build();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(
            "FROM " + index + " | STATS s = SUM(value), c = COUNT(*) BY key | STATS keys = COUNT(*), total = SUM(s), rows = SUM(c)"
        );
        request.pragmas(new QueryPragmas(pragmas));
        request.acceptedPragmaRisks(true);
        request.profile(true);
        long total = (long) keys * rowsPerKey * (keys * rowsPerKey - 1) / 2;
        try (EsqlQueryResponse response = run(request, TimeValue.timeValueMinutes(3))) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(1));
            assertThat("every key survives", values.get(0).get(0), equalTo((long) keys));
            assertThat("every value is summed once", values.get(0).get(1), equalTo(total));
            assertThat("every row is counted once", values.get(0).get(2), equalTo((long) keys * rowsPerKey));

            Map<String, List<HashAggregationOperator.FreezeStatus>> byStage = new HashMap<>();
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof HashAggregationOperator.Status status) {
                        for (Operator.Status.ExtraStatus extra : status.extraFields()) {
                            if (extra instanceof HashAggregationOperator.FreezeStatus freeze) {
                                byStage.computeIfAbsent(driver.description(), k -> new ArrayList<>()).add(freeze);
                            }
                        }
                    }
                }
            }
            assertThat("a stage froze: " + byStage.keySet(), byStage.size(), greaterThan(0));
            long rowsSpilled = 0;
            long readBack = 0;
            for (List<HashAggregationOperator.FreezeStatus> stage : byStage.values()) {
                rowsSpilled += stage.stream().mapToLong(HashAggregationOperator.FreezeStatus::rowsSpilled).sum();
                readBack += stage.stream().mapToLong(f -> f.spill().readBackBytes()).sum();
            }
            assertThat("rows went to disk: " + byStage, rowsSpilled, greaterThan(0L));
            assertThat("what went to disk came back", readBack, greaterThan(0L));
        }
        assertSpillDirectoriesEmpty();
    }

    /**
     * The value run: {@code SORT ... | LIMIT} over rows wider than the budget allows the limit's rows to be, so the
     * top-N operators keep their keys and send their values to segments, and read them back in the sort's order at the
     * end. The results are those of a resident run and the spill directories are empty afterwards.
     */
    public void testSortLimitSpillsValuesAndCleansUp() throws Exception {
        assumeTrue("the spill flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        String index = "spill_sort";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", between(1, 3)).put("index.number_of_replicas", 0))
                .setMapping("key", "type=long", "value", "type=keyword")
        );
        int rows = between(2_000, 5_000);
        int limit = between(500, 1_500);
        Map<Long, String> valueOf = new HashMap<>();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (long key = 0; key < rows; key++) {
            String value = randomAlphaOfLengthBetween(200, 400);
            valueOf.put(key, value);
            bulk.add(client().prepareIndex(index).setSource("key", key, "value", value));
        }
        assertFalse(bulk.get().hasFailures());

        // a budget every operator crosses at its first page and no own-state minimum
        Settings pragmas = Settings.builder()
            .put(QueryPragmas.SPILL_BUDGET.getKey(), "1kb")
            .put(QueryPragmas.SPILL_MIN_BYTES.getKey(), "0b")
            .put(QueryPragmas.PAGE_SIZE.getKey(), between(50, 500))
            .build();
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM " + index + " | SORT key DESC | LIMIT " + limit + " | KEEP key, value");
        request.pragmas(new QueryPragmas(pragmas));
        request.acceptedPragmaRisks(true);
        request.profile(true);
        try (EsqlQueryResponse response = run(request, TimeValue.timeValueMinutes(3))) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(limit));
            for (int i = 0; i < limit; i++) {
                long key = rows - 1 - i;
                assertThat("row " + i + " is the next key down", values.get(i).get(0), equalTo(key));
                assertThat("row " + i + " carries its value", values.get(i).get(1), equalTo(valueOf.get(key)));
            }

            long rowsSpilled = 0;
            long readBack = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof TopNOperatorStatus status) {
                        rowsSpilled += status.rowsSpilled();
                        readBack += status.spill().readBackBytes();
                    }
                }
            }
            assertThat("values went to segments", rowsSpilled, greaterThan(0L));
            assertThat("what went to segments came back", readBack, greaterThan(0L));
        }
        assertSpillDirectoriesEmpty();
    }

    private void assertSpillDirectoriesEmpty() throws Exception {
        // the scopes close once the response is gone; their files go on the I/O pool
        assertBusy(() -> {
            for (Environment environment : internalCluster().getInstances(Environment.class)) {
                Path root = environment.dataDirs()[0].resolve(DiskSpillSettings.DEFAULT_DIRECTORY);
                if (Files.isDirectory(root) == false) {
                    continue;
                }
                try (DirectoryStream<Path> nodes = Files.newDirectoryStream(root)) {
                    for (Path node : nodes) {
                        assertThat("spill directory [" + node + "] is empty", entries(node), empty());
                    }
                }
            }
        });
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
