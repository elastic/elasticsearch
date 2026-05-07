/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end integration for {@code FROM <dataset>}: creates a data source and a dataset via the
 * CRUD API, both pointing at a local CSV fixture, and runs a {@code FROM <name>} query against
 * them. Proves that the parser → dataset rewriter → external-source resolver → analyzer →
 * execution pipeline wires up the way the PR description claims.
 *
 * <p>Single-node by design: multi-node dataset publication trips an unrelated
 * {@code ProjectMetadata.Builder} assertion already on {@code main}; coverage of that path is out
 * of scope for this PR.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private Path csvFixture;
    private Path csvFixtureAlt;

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme. */
    public static final class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    private static final class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), e.getKey().startsWith("secret_")));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return datasetSettings == null ? Map.of() : new HashMap<>(datasetSettings);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    /** Determinism over planner-regression diversity here — these tests pin specific plan shapes. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DataSourceMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("dataset-fixture-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        csvFixtureAlt = createTempFile("dataset-fixture-alt-", ".csv");
        Files.writeString(csvFixtureAlt, String.join("\n", "emp_no:integer,first_name:keyword", "10,Diana", "11,Eve") + "\n");
    }

    /**
     * Names every {@code testXxx} body PUTs. New tests must register their dataset name here so the
     * SUITE-scoped cluster doesn't carry state across methods.
     */
    private static final Set<String> CREATED_DATASETS = Set.of("employees", "employees_alt", "logs_dataset");

    @After
    public void cleanupRegistry() throws Exception {
        for (String ds : CREATED_DATASETS) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(ds)).get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted by the test itself
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", ds, e);
            }
        }
        try {
            client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest("local_ds"))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already deleted by the test itself
        } catch (Exception e) {
            logger.warn("data source cleanup [local_ds] failed", e);
        }
    }

    public void testFromDatasetReadsCsvFixture() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(2).get(0), equalTo(3));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testFromMixedIndexAndDatasetRejected() throws Exception {
        // Real ES index alongside the dataset so the resolver finds both abstractions and the
        // mixed-FROM rejection actually fires (rather than the resolver silently filtering an unknown name).
        createIndex("some_real_index");
        ensureGreen("some_real_index");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM some_real_index, employees | LIMIT 1"), TIMEOUT));
        assertCauseMessageContains(ex, "mixing datasets and non-datasets");
    }

    public void testTSCommandRejectedOnDataset() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("TS employees | LIMIT 1"), TIMEOUT));
        assertCauseMessageContains(ex, "TS command is not supported for datasets");
    }

    public void testLookupJoinRejectedAgainstDataset() throws Exception {
        // Create some_real_index locally so the test exercises the LOOKUP JOIN rejection rather
        // than failing earlier with "unknown index" under order-dependent runs.
        createIndex("some_real_index");
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM some_real_index | LOOKUP JOIN employees ON emp_no | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "LOOKUP JOIN against a dataset is not supported");
    }

    public void testFromDatasetWithWhere() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees | WHERE emp_no > 1 | SORT emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0), equalTo(3));
            assertThat(rows.get(1).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testFromDatasetWithKeep() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees | KEEP first_name | SORT first_name"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("first_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(0).toString(), equalTo("Bob"));
            assertThat(rows.get(2).get(0).toString(), equalTo("Carol"));
        }
    }

    public void testFromDatasetWithStatsCount() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testFromDatasetWithEval() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees | EVAL doubled = emp_no * 2 | SORT emp_no | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // emp_no=1, doubled=2
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            int doubledIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("doubled");
            assertThat(rows.get(0).get(empNoIdx), equalTo(1));
            assertThat(((Number) rows.get(0).get(doubledIdx)).intValue(), equalTo(2));
        }
    }

    public void testFromMultipleDatasets() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees, employees_alt | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 3 rows from employees + 2 from employees_alt
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testFromDatasetWildcardExpansion() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // employees + employees_alt = 3 + 2 = 5
        try (var response = run(syncEsqlQueryRequest("FROM employees* | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testFromDatasetWildcardWithExclusion() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // employees* matches both, exclusion of employees_alt leaves only employees (3 rows)
        try (var response = run(syncEsqlQueryRequest("FROM employees*,-employees_alt | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testFromDatasetWithMetadataFieldsRejected() throws Exception {
        // Phase 1 rejects METADATA fields on datasets at the rewriter rather than silently dropping.
        // _index synthesis on datasets is tracked separately; _id / _source / _score have no agreed
        // semantics yet on external sources.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees METADATA _index | KEEP _index | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "METADATA fields are not supported on datasets");
        assertCauseMessageContains(ex, "employees");
    }

    public void testWildcardSpanningIndexAndDatasetRejected() throws Exception {
        // Real index plus a dataset, both matching the same wildcard. The resolver expands the
        // pattern through IndexAbstractionResolver and finds both abstractions; the rewriter buckets
        // them and fires the mixed-FROM rejection — same path as a literal `FROM idx, ds` mix.
        createIndex("logs_index");
        ensureGreen("logs_index");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("logs_dataset", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM logs_* | LIMIT 1"), TIMEOUT));
        assertCauseMessageContains(ex, "mixing datasets and non-datasets");
    }

    public void testFromUnknownNameFallsThroughToIndexResolution() throws Exception {
        // Register a dataset so the rewriter is active, but the FROM target is neither index nor dataset.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM no_such_thing | LIMIT 1"), TIMEOUT));
        // The rewriter leaves the relation unchanged when the name isn't a known dataset; the analyzer
        // then attempts to resolve it as an index, which fails. Confirms fall-through to index resolution
        // (regression guard against future "treat unknowns as datasets").
        assertCauseMessageContains(ex, "no_such_thing");
    }

    /** Walks the cause chain and asserts a message fragment appears somewhere in it. */
    private static void assertCauseMessageContains(Throwable throwable, String fragment) {
        Throwable cause = throwable;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains(fragment) == false)) {
            cause = cause.getCause();
        }
        assertThat("error chain should contain message fragment [" + fragment + "]", cause, org.hamcrest.Matchers.notNullValue());
    }

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }
}
