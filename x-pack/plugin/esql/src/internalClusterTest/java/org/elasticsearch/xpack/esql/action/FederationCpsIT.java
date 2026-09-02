/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end coverage for dataset queries under cross-project search (CPS) when no linked
 * project has a namesake index.
 * <p>
 * Under CPS, view resolution and the dataset rewrite each speculatively add a shadow relation for a
 * possible remote namesake, and the lenient linked-index lookup for a name that matches nothing
 * returns a valid-but-empty {@code IndexResolution}. Those shadows must be treated as unmatched and
 * stripped so speculative unions collapse. A surviving union under a view pipeline fails
 * post-optimization with "Nested subqueries are not supported". A surviving union under {@code FORK}
 * is source expansion, not a user subquery, and must still run.
 * <p>
 * CPS is a serverless deployment mode, so {@code serverless.cross_project.enabled} is not a
 * registered node setting in this distribution; {@link CpsSettingPlugin} registers it for the test
 * cluster the same way {@code TermsEnumCpsIT} does, and {@code CrossProjectModeDecider} picks it up
 * from node settings. With no linked projects configured, the lenient shadow lookups run against
 * names that match no index, reproducing the exact no-namesake scenario.
 */
public class FederationCpsIT extends AbstractExternalDataSourceIT {

    private static final String DATASET = "employees_cps";
    private static final String VIEW = "employees_cps_view";

    private Path csvFixture;
    private boolean viewCreated;

    /** Registers the CPS enable flag, which only the serverless distribution registers in production. */
    public static class CpsSettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CpsSettingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("serverless.cross_project.enabled", true).build();
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("cps-federation-dataset-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
    }

    @After
    public void cleanupView() throws Exception {
        if (viewCreated) {
            client().execute(DeleteViewAction.INSTANCE, new DeleteViewAction.Request(TIMEOUT, TIMEOUT, new String[] { VIEW }))
                .get(30, SECONDS);
            viewCreated = false;
        }
    }

    /**
     * A view body with a pipeline stage ({@code EVAL}) over the dataset places the dataset's
     * speculative union underneath a node that view compaction cannot lift a union through, so the
     * query only succeeds if both no-match shadows are stripped and both unions collapse.
     */
    public void testPipelineViewOverDatasetWithNoRemoteNamesakes() throws Exception {
        registerDataset(DATASET, csvFixture.toUri().toString(), Map.of("format", "csv"));
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(VIEW, "FROM " + DATASET + " | EVAL marker = 1"))
            )
        );
        viewCreated = true;

        try (var response = run(syncEsqlQueryRequest("FROM " + VIEW + " | SORT emp_no | KEEP emp_no, first_name, marker"), TIMEOUT)) {
            assertThat(
                response.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("emp_no", "integer", null),
                        new ColumnInfoImpl("first_name", "keyword", null),
                        new ColumnInfoImpl("marker", "integer", null)
                    )
                )
            );
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice", 1), List.of(2, "Bob", 1), List.of(3, "Carol", 1))));
        }
    }

    /**
     * The dataset queried directly (no view) under CPS: only the dataset shadow is in play, and the
     * single-survivor union must still collapse to the external relation and return the file's rows.
     */
    public void testDatasetDirectlyUnderCpsWithNoRemoteNamesakes() throws Exception {
        registerDataset(DATASET, csvFixture.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM " + DATASET + " | SORT emp_no | KEEP emp_no, first_name"), TIMEOUT)) {
            assertThat(
                response.columns(),
                equalTo(List.of(new ColumnInfoImpl("emp_no", "integer", null), new ColumnInfoImpl("first_name", "keyword", null)))
            );
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"))));
        }
    }

    /**
     * A single-dataset {@code FROM} under CPS is a two-child union until the empty shadow is
     * stripped. {@code FORK} above that union must still run.
     */
    public void testForkOverDatasetWithNoRemoteNamesakes() throws Exception {
        registerDataset(DATASET, csvFixture.toUri().toString(), Map.of("format", "csv"));

        String query = "FROM " + DATASET + """
            | FORK
                (WHERE emp_no < 3 | STATS count = COUNT(*))
                (WHERE emp_no == 3 | STATS count = COUNT(*))
            | KEEP _fork, count
            | SORT _fork
            """;
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), 1).build()));
        try (var response = run(request, TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0).toString(), equalTo("fork1"));
            assertThat(rows.get(0).get(1), equalTo(2L));
            assertThat(rows.get(1).get(0).toString(), equalTo("fork2"));
            assertThat(rows.get(1).get(1), equalTo(1L));
        }
    }
}
