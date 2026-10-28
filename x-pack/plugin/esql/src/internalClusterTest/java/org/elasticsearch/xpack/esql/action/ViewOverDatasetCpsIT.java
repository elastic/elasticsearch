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

/**
 * End-to-end regression coverage for querying a view whose body applies pipeline stages to a
 * dataset while cross-project search (CPS) is enabled and no linked project has a namesake index.
 * <p>
 * Under CPS, view resolution and the dataset rewrite each speculatively add a shadow relation for a
 * possible remote namesake, and the lenient linked-index lookup for a name that matches nothing
 * returns a valid-but-empty {@code IndexResolution}. Both shadows must be treated as unmatched and
 * stripped so both speculative unions collapse; resolving them into empty {@code EsRelation}s
 * instead keeps the inner union alive underneath the view body's pipeline stage, where view
 * compaction cannot flatten it, and the whole query fails post-optimization with
 * "Nested subqueries are not supported".
 * <p>
 * CPS is a serverless deployment mode, so {@code serverless.cross_project.enabled} is not a
 * registered node setting in this distribution; {@link CpsSettingPlugin} registers it for the test
 * cluster the same way {@code TermsEnumCpsIT} does, and {@code CrossProjectModeDecider} picks it up
 * from node settings. With no linked projects configured, the lenient shadow lookups run against
 * names that match no index, reproducing the exact no-namesake scenario.
 */
public class ViewOverDatasetCpsIT extends AbstractExternalDataSourceIT {

    private static final String DATASET = "employees_cps";
    private static final String VIEW = "employees_cps_view";
    private static final String NESTED_VIEW_1 = "employees_cps_nested_view_1";
    private static final String NESTED_VIEW_2 = "employees_cps_nested_view_2";
    private static final String NESTED_VIEW_3 = "employees_cps_nested_view_3";

    private Path csvFixture;
    private final List<String> createdViews = new ArrayList<>();

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
        csvFixture = createTempFile("cps-view-dataset-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
    }

    @After
    public void cleanupViews() throws Exception {
        for (String view : createdViews.reversed()) {
            client().execute(DeleteViewAction.INSTANCE, new DeleteViewAction.Request(TIMEOUT, TIMEOUT, new String[] { view }))
                .get(30, SECONDS);
        }
        createdViews.clear();
    }

    /**
     * A view body with a pipeline stage ({@code EVAL}) over the dataset places the dataset's
     * speculative union underneath a node that view compaction cannot lift a union through, so the
     * query only succeeds if both no-match shadows are stripped and both unions collapse.
     */
    public void testPipelineViewOverDatasetWithNoRemoteNamesakes() throws Exception {
        registerDataset(DATASET, csvFixture.toUri().toString(), Map.of("format", "csv"));
        putView(VIEW, "FROM " + DATASET + " | EVAL marker = 1");

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

    public void testNestedViewsOverEmptyIndexWithNoRemoteNamesakes() throws Exception {
        String emptyIndex = "employees_cps_empty";
        String existingIndex = "employees_cps_existing";
        createIndex(emptyIndex);
        prepareIndex(existingIndex).setSource("emp_no", 4, "first_name", "Dave").get();
        refresh(existingIndex);
        putNestedViews(emptyIndex);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + existingIndex + "," + NESTED_VIEW_3 + " | SORT emp_no | KEEP emp_no, first_name"),
                TIMEOUT
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(4L, "Dave"))));
        }
    }

    public void testNestedViewsOverDatasetWithNoRemoteNamesakes() throws Exception {
        String existingIndex = "employees_cps_existing";
        createIndex(existingIndex);
        registerDataset(DATASET, csvFixture.toUri().toString(), Map.of("format", "csv"));
        putNestedViews(DATASET);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM " + existingIndex + "," + NESTED_VIEW_3 + " | SORT emp_no | KEEP emp_no, first_name"),
                TIMEOUT
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1, "Alice"), List.of(2, "Bob"), List.of(3, "Carol"))));
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

    private void putNestedViews(String source) {
        putView(NESTED_VIEW_1, "FROM " + source);
        putView(NESTED_VIEW_2, "FROM " + NESTED_VIEW_1);
        putView(NESTED_VIEW_3, "FROM " + NESTED_VIEW_2);
    }

    private void putView(String name, String query) {
        assertAcked(client().execute(PutViewAction.INSTANCE, new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(name, query))));
        createdViews.add(name);
    }
}
