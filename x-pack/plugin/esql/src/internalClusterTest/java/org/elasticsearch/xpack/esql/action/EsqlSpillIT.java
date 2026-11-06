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
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
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
import java.util.List;

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
