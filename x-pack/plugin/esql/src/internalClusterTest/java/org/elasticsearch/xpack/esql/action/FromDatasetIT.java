/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end integration for {@code FROM <dataset>}: creates a data source and a dataset via the
 * CRUD API, both pointing at a local CSV fixture, and runs a {@code FROM <name>} query against
 * them. Proves that the parser → dataset rewriter → external-source resolver → analyzer →
 * execution pipeline wires up the way the PR description claims.
 *
 * <p>Single-node by design; this exercises the {@code FROM <dataset>} pipeline, not cluster-state propagation across
 * nodes (covered by {@code ProjectMetadataTests#testDatasetChangeViaDiffRebuildsIndicesLookup}).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private Path csvFixture;
    private Path csvFixtureAlt;
    private Path ndjsonFixture;

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
        plugins.add(NdJsonDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
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
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("dataset-fixture-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        csvFixtureAlt = createTempFile("dataset-fixture-alt-", ".csv");
        Files.writeString(csvFixtureAlt, String.join("\n", "emp_no:integer,first_name:keyword", "10,Diana", "11,Eve") + "\n");
        // NDJSON fixture with the SAME physical field names (emp_no/first_name) as the CSV fixture, so rename tests can
        // exercise the by-name (JSON key) read path: a declared `source` maps the logical column to its JSON field.
        ndjsonFixture = createTempFile("dataset-fixture-", ".ndjson");
        Files.writeString(
            ndjsonFixture,
            String.join(
                "\n",
                "{\"emp_no\":1,\"first_name\":\"Alice\"}",
                "{\"emp_no\":2,\"first_name\":\"Bob\"}",
                "{\"emp_no\":3,\"first_name\":\"Carol\"}"
            ) + "\n"
        );
    }

    /**
     * Names every {@code testXxx} body PUTs. New tests must register their dataset name here so the
     * SUITE-scoped cluster doesn't carry state across methods.
     */
    private static final Set<String> CREATED_DATASETS = Set.of(
        "employees",
        "employees_alt",
        "logs_dataset",
        "events_hive",
        "employees_external",
        "employees_mixed",
        "stats_ds",
        "employees_strict",
        "employees_nonstrict",
        "employees_strict_multi",
        "employees_nonstrict_multi",
        "employees_rename_strict",
        "employees_rename_nonstrict",
        "employees_rename_keep",
        "employees_ndjson_rename_strict",
        "employees_ndjson_rename_nonstrict",
        "employees_parquet_rename",
        "employees_source_disabled",
        "employees_source_enabled",
        "employees_copy_nonstrict",
        "employees_parquet_copy",
        "employees_copy_collide",
        "employees_copy_multi",
        "employees_swap",
        "employees_id_from_col"
    );

    /**
     * Names every {@code testXxx} body creates via {@link PutViewAction}. As with datasets, the SUITE-scoped
     * cluster requires explicit teardown so views don't leak across methods.
     */
    private static final Set<String> CREATED_VIEWS = Set.of("employees_view", "employees_filtered_view");

    @After
    public void cleanupRegistry() throws Exception {
        for (String view : CREATED_VIEWS) {
            try {
                client().execute(DeleteViewAction.INSTANCE, deleteViewRequest(view)).get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted by the test itself
            } catch (Exception e) {
                logger.warn("view cleanup [{}] failed", view, e);
            }
        }
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

    public void testStrictDeclaredSchemaUsesDeclaredNamesAndTypesSkippingInference() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Strict (dynamic:false) declaration over the CSV fixture whose physical header is emp_no:integer,first_name:keyword.
        // The declaration relabels the columns and pins emp_no's type to LONG (inference would have produced INTEGER),
        // proving the declared mapping is used and inference is skipped.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_strict | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            // declared LONG => values are Long, not the Integer inference would have produced
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(2).get(0), equalTo(3L));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testNonStrictDeclaredSchemaOverridesDeclaredColumnsAndKeepsInferredRest() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict (dynamic:true) declaration over the CSV fixture (emp_no:integer, first_name:keyword). We declare
        // only emp_no, pinning it to LONG (no rename); first_name is left to inference. The result must show the
        // declared type override AND the inferred remainder.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("long", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_nonstrict",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_nonstrict | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("emp_no"));      // declared (retyped), not renamed
            assertThat(columns.get(1).name(), equalTo("first_name"));  // inferred, passed through

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));               // declared LONG, not inferred Integer
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(2).get(0), equalTo(3L));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testStrictDeclaredSchemaRenamesColumnsViaSource() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Strict declaration that RENAMES via `source`: physical emp_no/first_name are exposed as id/name. CSV is read
        // positionally, so the declared order must match the file order; the logical names id/name are what the query sees.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_rename_strict",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_rename_strict | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));     // physical emp_no exposed as id
            assertThat(columns.get(1).name(), equalTo("name"));   // physical first_name exposed as name

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));          // emp_no's value under the id name, retyped LONG
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(2).get(0), equalTo(3L));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testNonStrictDeclaredSchemaRenamesViaSource() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict declaration that renames emp_no -> id (and retypes to LONG); first_name is left to inference.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_rename_nonstrict",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_rename_nonstrict | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));          // renamed from emp_no
            assertThat(columns.get(1).name(), equalTo("first_name"));  // inferred, passed through

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
        }
    }

    public void testCopyToDuplicatesColumnNonStrict() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict copy: emp_no is kept AND copied to emp_copy. The copy materializes as EVAL emp_copy = emp_no above
        // the base relation, so both columns carry the same value and the read path is untouched.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("long", null, "emp_copy"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_copy_nonstrict",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_copy_nonstrict | KEEP emp_no, emp_copy | SORT emp_no | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("emp_copy")); // the copy target
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("copy carries the source value", row.get(1), equalTo(row.get(0)));
            }
            assertThat(rows.get(0).get(0), equalTo(1L));
        }
    }

    public void testRenameWithKeepSubsetProjectsRenamedColumn() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // KEEP a subset down to just the renamed column: stresses the projection path under a rename (the column the
        // reader must read is the physical emp_no, exposed as id).
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_rename_keep",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_rename_keep | KEEP id | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("id"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(2).get(0), equalTo(3L));
        }
    }

    public void testNdJsonRenameStrictReadsByPhysicalJsonKey() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // NDJSON is read BY NAME (JSON key), so rename exercises the reader's logical->physical key resolution: the
        // declared id/name columns must be read from the JSON fields emp_no/first_name.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_ndjson_rename_strict",
                    "local_ds",
                    ndjsonFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_ndjson_rename_strict | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));            // read from JSON field emp_no
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice")); // read from JSON field first_name
            assertThat(rows.get(2).get(0), equalTo(3L));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testNdJsonRenameNonStrictReadsByPhysicalJsonKey() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict over NDJSON: emp_no renamed to id (retyped LONG); first_name inferred and read by its own JSON key.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_ndjson_rename_nonstrict",
                    "local_ds",
                    ndjsonFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_ndjson_rename_nonstrict | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));          // renamed from emp_no
            assertThat(columns.get(1).name(), equalTo("first_name"));  // inferred

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
        }
    }

    public void testSourceDisabledRejectsMetadataSource() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("integer", null));
        // _source.enabled: false -> METADATA _source must be rejected, not returned as a silently-null column.
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties, false));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_source_disabled",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees_source_disabled METADATA _source | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "[_source] is not available");
    }

    public void testSourceEnabledByDefaultAllowsMetadataSource() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // No _source knob -> available by default; METADATA _source resolves to a column (not rejected).
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_source_enabled", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM employees_source_enabled METADATA _source | KEEP _source | LIMIT 1"), TIMEOUT)) {
            assertThat(response.columns().get(0).name(), equalTo("_source"));
        }
    }

    public void testParquetRenameProjectsRenamedColumns() throws Exception {
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_rename | KEEP id, name | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L));                 // physical emp_no under logical id
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice")); // physical first_name under logical name
        }
    }

    public void testParquetRenameFilterPushdownOnRenamedColumn() throws Exception {
        // WHERE on the renamed `comp` (physical salary) exercises the pushed-filter surface — a mistranslated predicate
        // would silently drop/keep the wrong rows (parquet pushes the predicate down to row-group/stats).
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_rename | WHERE comp > 150 | SORT id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2)); // Bob (200), Carol (300)
            assertThat(rows.get(0).get(0), equalTo(2L));
            assertThat(rows.get(1).get(0), equalTo(3L));
        }
    }

    public void testParquetRenameAggregateOnRenamedColumn() throws Exception {
        // MAX/MIN on the renamed `comp` exercise the aggregate-stats surface (parquet answers these from footer stats
        // keyed by the physical column name).
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_rename | STATS mx = MAX(comp), mn = MIN(comp)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).intValue(), equalTo(300));
            assertThat(((Number) rows.get(0).get(1)).intValue(), equalTo(100));
        }
    }

    public void testParquetRenameTopNOnRenamedSortKey() throws Exception {
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_rename | SORT comp DESC | KEEP id | LIMIT 2"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(3L)); // Carol comp=300
            assertThat(rows.get(1).get(0), equalTo(2L)); // Bob comp=200
        }
    }

    public void testParquetRenameFromDottedFlattenedPath() throws Exception {
        // The physical column is the flattened nested path `dept.code`; the mapping renames it to the flat logical
        // `dept_code`. Proves a dotted (nested-flattened) physical name rides the same rename as an opaque whole string.
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_rename | KEEP id, dept_code | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(1).name(), equalTo("dept_code"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(1).toString(), equalTo("ENG"));   // read from nested dept.code
            assertThat(rows.get(2).get(1).toString(), equalTo("OPS"));
        }
    }

    public void testRenameSwapsTwoColumns() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Swap: emp_no reads first_name, first_name reads emp_no. Each output name is unique and each physical is used
        // once (a bijection), so it should be allowed and the values should cross over.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("keyword", "first_name")); // emp_no <- physical first_name (text)
        properties.put("first_name", new DatasetFieldMapping("long", "emp_no"));     // first_name <- physical emp_no (num)
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_swap",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );
        try (
            var response = run(syncEsqlQueryRequest("FROM employees_swap | SORT first_name | KEEP emp_no, first_name | LIMIT 10"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0).toString(), equalTo("Alice")); // emp_no now holds the text
            assertThat(rows.get(0).get(1), equalTo(1L));                 // first_name now holds the number
            assertThat(rows.get(2).get(0).toString(), equalTo("Carol"));
            assertThat(rows.get(2).get(1), equalTo(3L));
        }
    }

    public void testCopyToMultipleTargets() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // copy_to a LIST of targets: emp_no is copied to both emp_a and emp_b (one EVAL alias per target).
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("long", null, java.util.List.of("emp_a", "emp_b")));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_copy_multi",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_copy_multi | SORT emp_no | KEEP emp_no, emp_a, emp_b | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(3));
            assertThat(columns.get(1).name(), equalTo("emp_a"));
            assertThat(columns.get(2).name(), equalTo("emp_b"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(1), equalTo(row.get(0))); // emp_a == emp_no
                assertThat(row.get(2), equalTo(row.get(0))); // emp_b == emp_no
            }
        }
    }

    public void testCopyToTargetCollidingWithInferredColumnRejected() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Non-strict copy whose target collides with a REAL (inferred) file column. PUT can't see inferred columns, so
        // it passes; the query must reject rather than let the EVAL silently overwrite first_name with a copy of emp_no.
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("long", null, "first_name")); // target == an inferred column
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_copy_collide",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );
        Exception e = expectThrows(Exception.class, () -> {
            try (var ignored = run(syncEsqlQueryRequest("FROM employees_copy_collide | KEEP emp_no, first_name"), TIMEOUT)) {
                // must not reach here — the copy target collides with the inferred first_name
            }
        });
        assertThat(e.getMessage(), containsString("collides with an existing column"));
    }

    public void testParquetStrictCopyToAndFilterPushdown() throws Exception {
        // Strict + columnar + copy: comp (physical salary) is moved AND copied to comp2. The copy is an EVAL above the
        // relation, so it works for parquet exactly as for CSV, and a WHERE on the copy target substitutes back to the
        // source and pushes down — the trickiest combination (strict, columnar pushdown, copy alias-substitution).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("comp", new DatasetFieldMapping("integer", "salary", "comp2")); // move salary->comp, copy to comp2
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_parquet_copy",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );

        // 1) the copy carries the source value
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_copy | SORT id | KEEP comp, comp2 | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("copy carries the source value", row.get(1), equalTo(row.get(0)));
            }
        }
        // 2) WHERE on the copy target pushes down (substituted to salary): Bob (200), Carol (300)
        try (var response = run(syncEsqlQueryRequest("FROM employees_parquet_copy | WHERE comp2 > 150 | SORT id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(2L));
            assertThat(rows.get(1).get(0), equalTo(3L));
        }
    }

    private void putParquetRenameDataset(String name, Path parquet) {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
        properties.put("comp", new DatasetFieldMapping("integer", "salary"));
        properties.put("dept_code", new DatasetFieldMapping("keyword", "dept.code")); // flattened nested path
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    name,
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
    }

    private Path writeParquetRenameFixture() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message employees { required int64 emp_no; required binary first_name (UTF8); required int32 salary;"
                + " required group dept { required binary code (UTF8); } }"
        );
        String[] names = { "Alice", "Bob", "Carol" };
        int[] salaries = { 100, 200, 300 };
        String[] deptCodes = { "ENG", "SAL", "OPS" };
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < names.length; i++) {
                Group g = factory.newGroup();
                g.add("emp_no", (long) (i + 1));
                g.add("first_name", names[i]);
                g.add("salary", salaries[i]);
                g.addGroup("dept").add("code", deptCodes[i]);
                writer.write(g);
            }
        }
        Path tempFile = createTempDir().resolve("employees.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    public void testStrictDeclaredSchemaOverMultiFileGlob() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");
        Files.writeString(root.resolve("part2.csv"), "emp_no:integer,first_name:keyword\n3,Carol\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_multi",
                    "local_ds",
                    root.toUri() + "*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_strict_multi | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3)); // 2 rows from part1 + 1 from part2, no per-file schema reads
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(2).get(0), equalTo(3L));
        }
    }

    public void testNonStrictDeclaredSchemaOverMultiFileGlob() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");
        Files.writeString(root.resolve("part2.csv"), "emp_no:integer,first_name:keyword\n3,Carol\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        java.util.Map<String, DatasetFieldMapping> properties = new java.util.LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("long", null)); // retype only; first_name inferred
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_nonstrict_multi",
                    "local_ds",
                    root.toUri() + "*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_nonstrict_multi | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("first_name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1L)); // declared LONG override across files
            assertThat(rows.get(2).get(0), equalTo(3L));
        }
    }

    public void testViewOverExternalDatasetIsQueryable() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_external", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        // The view body targets the dataset. View resolution runs before the dataset rewrite, so it inlines the body
        // while it is still a plain index-shaped relation; the rewriter then turns the inlined leaf into an external
        // relation over the CSV fixture. This is the end-to-end realisation of a view "containing" an external source.
        // There is no index named "employees_external", so the query can only succeed via the external dataset pipeline —
        // if the inlined leaf were treated as an index it would fail with "unknown index" (see testFromUnknownName...).
        assertAcked(client().execute(PutViewAction.INSTANCE, putViewRequest("employees_view", "FROM employees_external")));

        try (var response = run(syncEsqlQueryRequest("FROM employees_view | SORT emp_no | LIMIT 10"), TIMEOUT)) {
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

    public void testViewOverExternalDatasetWithTransformInBody() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_external", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );
        // A non-trivial view body (a WHERE on top of the dataset) proves the external leaf resolves and executes when
        // it is nested below other commands inside a resolved view, not just as a bare top-level relation.
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                putViewRequest("employees_filtered_view", "FROM employees_external | WHERE emp_no > 1")
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_filtered_view | SORT emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0), equalTo(3));
            assertThat(rows.get(1).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testFromMixedIndexAndDatasetSucceeds() throws Exception {
        // Heterogeneous FROM (index + dataset) should succeed rather than reject.
        // some_real_index has no documents; employees dataset has 3 rows (Alice, Bob, Carol).
        createIndex("some_real_index");
        ensureGreen("some_real_index");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // Empty index + 3-row dataset = 3 rows total
        try (var response = run(syncEsqlQueryRequest("FROM some_real_index, employees | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testFromMixedWithWhere() throws Exception {
        // Heterogeneous FROM + WHERE: filter pushed into each UnionAll branch.
        // employees_mixed dataset (CSV): emp_no 1,2,3. ES index "employees_idx_where" is empty.
        createIndex("employees_idx_where");
        ensureGreen("employees_idx_where");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees_mixed", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_idx_where, employees_mixed | WHERE emp_no > 1 | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 0 from empty index + 2 from dataset (emp_no 2 and 3)
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(2L));
        }
    }

    public void testFromMixedWithStatsCount() throws Exception {
        // Heterogeneous FROM + STATS COUNT: aggregate pushed into each UnionAll branch.
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

        // Two datasets (3 + 2 = 5 rows) plus an empty index = 5 total.
        createIndex("employees_idx_stats");
        ensureGreen("employees_idx_stats");

        try (var response = run(syncEsqlQueryRequest("FROM employees_idx_stats, employees, employees_alt | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
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

    public void testFromDatasetIndexMetadataReturnsDatasetName() throws Exception {
        // Standard metadata fields are accepted on datasets. For the FROM <dataset> path, _index
        // resolves to the user-facing dataset name (not the underlying resource path) for every
        // row, matching the "_index is the dataset name" contract.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // METADATA surfaces _index with no KEEP; it resolves to the dataset name.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_index must surface without KEEP; got " + names, names, hasItem("_index"));
            int idx = names.indexOf("_index");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(idx).toString(), equalTo("employees"));
            }
        }
    }

    public void testHivePartitionClaimingReservedNameIsRenamedAndSpecWins() throws Exception {
        // Standard metadata names are dedicated: a Hive layout claiming /_index=.../ cannot
        // redefine METADATA _index. End-to-end pin for the rename: _index carries the dataset
        // name for every row, while the layout's value stays queryable under _partition._index.
        Path root = createTempDir();
        Path alpha = Files.createDirectories(root.resolve("_index=alpha"));
        Files.writeString(alpha.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");
        Path beta = Files.createDirectories(root.resolve("_index=beta"));
        Files.writeString(beta.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n3,Carol\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("events_hive", "local_ds", root.toUri() + "**/*.csv", Map.of("format", "csv"))
            )
        );

        // No KEEP: METADATA _index surfaces _index on its own, and the renamed partition column
        // _partition._index surfaces as an ordinary data column. Both are found by name, not position.
        try (var response = run(syncEsqlQueryRequest("FROM events_hive METADATA _index | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_index must surface without KEEP, got " + names, names, hasItem("_index"));
            assertThat("renamed partition column must stay queryable, got " + names, names, hasItem("_partition._index"));
            int indexIdx = names.indexOf("_index");
            int partitionIdx = names.indexOf("_partition._index");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            String[] expectedPartition = { "alpha", "alpha", "beta" };
            for (int i = 0; i < rows.size(); i++) {
                assertThat("spec-defined _index must carry the dataset name", rows.get(i).get(indexIdx).toString(), equalTo("events_hive"));
                assertThat(
                    "layout value stays queryable under the rename",
                    rows.get(i).get(partitionIdx).toString(),
                    equalTo(expectedPartition[i])
                );
            }
        }
    }

    public void testFromDatasetIdMetadataIsOpaqueAndRecordRefCarriesByteOffset() throws Exception {
        // End-to-end proof of the _id composition path on a non-Parquet format (CSV). The CSV reader
        // emits each record's file-global byte offset on the _rowPosition channel (splitStartByte +
        // bytes consumed up to the record's first character), matching NDJSON's shape so the value
        // is identical regardless of split layout. The raw token stays observable through
        // _file.record_ref; _id itself is the opaque (location, mtime, token) hash via
        // ExternalRowIdentity — fixed 32-char base64url, no path leak. The fixture writes
        // "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n3,Carol\n", so the three sorted rows
        // sit at byte offsets 34, 42, 48 (header 34 bytes; "1,Alice\n" 8 bytes; "2,Bob\n" 6 bytes).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // No KEEP: METADATA _id, _file.record_ref surfaces both on their own; columns found by name.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _id, _file.record_ref | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_id must surface without KEEP, got " + names, names, hasItem("_id"));
            assertThat("_file.record_ref must surface without KEEP, got " + names, names, hasItem("_file.record_ref"));
            int idIdx = names.indexOf("_id");
            int refIdx = names.indexOf("_file.record_ref");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));

            long[] expectedOffsets = { 34, 42, 48 };
            Set<String> distinctIds = new HashSet<>();
            for (int i = 0; i < rows.size(); i++) {
                String id = rows.get(i).get(idIdx).toString();
                assertTrue("rendered _id [" + id + "] must be fixed-length base64url", id.matches("[A-Za-z0-9_-]{32}"));
                assertThat("storage location must not leak into _id [" + id + "]", id, not(containsString("employees")));
                distinctIds.add(id);
                assertThat(
                    "file-global byte offset for row " + i,
                    ((Number) rows.get(i).get(refIdx)).longValue(),
                    equalTo(expectedOffsets[i])
                );
            }
            assertThat("all _id values are distinct", distinctIds, hasSize(3));
        }
    }

    public void testFromDatasetStandardMetadataNeverFails() throws Exception {
        // Standing contract: every standard metadata name is accepted, returning a value or SQL NULL,
        // but never an error. _index carries the dataset name; _version carries the file mtime; the
        // rest (no relevance scoring, no per-row _ignored, etc.) come back as NULL columns. None may
        // be dropped and none may crash the query.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // _tier (DataTierFieldMapper.NAME) is snapshot-only in MetadataAttribute.ATTRIBUTES_MAP;
        // omit it so the query is valid in non-snapshot builds. _score, _tsid, _size, _ignored,
        // _index_mode have no value on external rows and must render as NULL columns rather than
        // being dropped or erroring.
        String query = "FROM employees METADATA _index, _version, _ignored, _index_mode, _tsid, _size, _score "
            + "| SORT emp_no "
            + "| KEEP emp_no, _index, _version, _ignored, _index_mode, _tsid, _size, _score "
            + "| LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat(names, equalTo(List.of("emp_no", "_index", "_version", "_ignored", "_index_mode", "_tsid", "_size", "_score")));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_index is the dataset name", row.get(1).toString(), equalTo("employees"));
                // _ignored, _index_mode, _tsid, _size, _score have no external value: NULL.
                assertThat("_ignored is null on external rows", row.get(3), org.hamcrest.Matchers.nullValue());
                assertThat("_index_mode is null on external rows", row.get(4), org.hamcrest.Matchers.nullValue());
                assertThat("_tsid is null on external rows", row.get(5), org.hamcrest.Matchers.nullValue());
                assertThat("_size is null on external rows", row.get(6), org.hamcrest.Matchers.nullValue());
                assertThat("_score is null on external rows", row.get(7), org.hamcrest.Matchers.nullValue());
            }
        }
    }

    public void testWildcardSpanningIndexAndDatasetSucceeds() throws Exception {
        // A wildcard matching both a real index and a dataset should succeed, producing a
        // heterogeneous UnionAll. logs_index is empty; logs_dataset has 3 rows from the CSV fixture.
        createIndex("logs_index");
        ensureGreen("logs_index");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("logs_dataset", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // logs_* expands to logs_index (empty) + logs_dataset (3 rows) = 3 total
        try (var response = run(syncEsqlQueryRequest("FROM logs_* | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testFromDatasetExplainDoesNotLeakSecrets() throws Exception {
        assumeTrue("EXPLAIN requires the capability to be enabled", Cap.EXPLAIN.isEnabled());

        // Register a DataSource whose settings include a secret key with a recognisable sentinel value.
        // TestValidator marks any key starting with "secret_" as a secret, which causes DatasetRewriter
        // to wrap the value in a SecureString when building the config map for UnresolvedExternalRelation.
        final String sentinel = "SENTINEL_DO_NOT_LEAK_aBcD1234";
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                putDataSourceRequest("local_ds", Map.of("secret_access_key", sentinel, "region", "us-east-1"))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("EXPLAIN (FROM employees | LIMIT 1)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            for (List<Object> row : rows) {
                for (Object value : row) {
                    String rendered = String.valueOf(value);
                    assertFalse("EXPLAIN output must not contain the secret sentinel — found in: " + rendered, rendered.contains(sentinel));
                }
            }
        }
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

    /**
     * Grouped STATS (SUM BY) over a heterogeneous FROM — exercises {@code PushAggregateThroughUnionAll}.
     *
     * <p>Setup: ES index {@code mixed_grp_idx} has dept=1,salary=200 and dept=2,salary=300.
     * Dataset {@code stats_ds} has dept=1,salary=100 and dept=3,salary=400.
     *
     * <p>Expected: SUM(salary) BY dept across both sources =
     * {dept=1 → 300}, {dept=2 → 300}, {dept=3 → 400}.
     */
    public void testFromMixedGroupedStats() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("mixed_grp_idx").setMapping("dept", "type=integer", "salary", "type=integer"));
        prepareIndex("mixed_grp_idx").setSource(Map.of("dept", 1, "salary", 200)).get();
        prepareIndex("mixed_grp_idx").setSource(Map.of("dept", 2, "salary", 300)).get();
        client().admin().indices().prepareRefresh("mixed_grp_idx").get();

        Path salaryFixture = createTempFile("salary-fixture-", ".csv");
        Files.writeString(salaryFixture, "dept:integer,salary:integer\n1,100\n3,400\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("stats_ds", "local_ds", salaryFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM mixed_grp_idx, stats_ds | STATS total = SUM(salary) BY dept | SORT dept"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            int totalIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("total");
            int deptIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("dept");

            // dept=1: 200 (ES) + 100 (CSV) = 300
            assertThat(((Number) rows.get(0).get(deptIdx)).intValue(), equalTo(1));
            assertThat(((Number) rows.get(0).get(totalIdx)).longValue(), equalTo(300L));
            // dept=2: 300 (ES only)
            assertThat(((Number) rows.get(1).get(deptIdx)).intValue(), equalTo(2));
            assertThat(((Number) rows.get(1).get(totalIdx)).longValue(), equalTo(300L));
            // dept=3: 400 (CSV only)
            assertThat(((Number) rows.get(2).get(deptIdx)).intValue(), equalTo(3));
            assertThat(((Number) rows.get(2).get(totalIdx)).longValue(), equalTo(400L));
        }
    }

    /**
     * MAX and MIN across a heterogeneous FROM — exercises {@code PushAggregateThroughUnionAll} for
     * Min/Max decomposition.
     *
     * <p>Setup: ES index {@code maxmin_idx} has emp_no=10 and emp_no=20.
     * Dataset {@code employees} (csvFixture) has emp_no=1, 2, 3.
     *
     * <p>Expected: MAX(emp_no)=20, MIN(emp_no)=1.
     */
    public void testFromMixedMaxMin() throws Exception {
        assertAcked(
            client().admin().indices().prepareCreate("maxmin_idx").setMapping("emp_no", "type=integer", "first_name", "type=keyword")
        );
        prepareIndex("maxmin_idx").setSource(Map.of("emp_no", 10, "first_name", "Dave")).get();
        prepareIndex("maxmin_idx").setSource(Map.of("emp_no", 20, "first_name", "Eve")).get();
        client().admin().indices().prepareRefresh("maxmin_idx").get();

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM maxmin_idx, employees | STATS max_emp = MAX(emp_no), min_emp = MIN(emp_no)"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int maxIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("max_emp");
            int minIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("min_emp");
            assertThat(((Number) rows.get(0).get(maxIdx)).longValue(), equalTo(20L));
            assertThat(((Number) rows.get(0).get(minIdx)).longValue(), equalTo(1L));
        }
    }

    /**
     * SORT + LIMIT over a heterogeneous FROM where both sources contain rows —
     * verifies that global TopN across an ES index and a CSV dataset produces the correct top-N rows.
     *
     * <p>Setup: ES index {@code sort_idx} has emp_no=4, emp_no=5.
     * Dataset {@code employees} (csvFixture) has emp_no=1, 2, 3.
     *
     * <p>With {@code | SORT emp_no | LIMIT 3}, the result must be the three smallest values: 1, 2, 3.
     */
    public void testFromMixedSortLimit() throws Exception {
        assertAcked(
            client().admin().indices().prepareCreate("sort_idx").setMapping("emp_no", "type=integer", "first_name", "type=keyword")
        );
        prepareIndex("sort_idx").setSource(Map.of("emp_no", 4, "first_name", "Dave")).get();
        prepareIndex("sort_idx").setSource(Map.of("emp_no", 5, "first_name", "Eve")).get();
        client().admin().indices().prepareRefresh("sort_idx").get();

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM sort_idx, employees | SORT emp_no | LIMIT 3"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            int empNoIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("emp_no");
            // The 3 smallest emp_no values (1,2,3) all come from the dataset
            assertThat(((Number) rows.get(0).get(empNoIdx)).intValue(), equalTo(1));
            assertThat(((Number) rows.get(1).get(empNoIdx)).intValue(), equalTo(2));
            assertThat(((Number) rows.get(2).get(empNoIdx)).intValue(), equalTo(3));
        }
    }

    /**
     * A dataset that declares {@code mappings._id.path = first_name} stamps each row's {@code _id} from the
     * {@code first_name} column's value. Asserted three ways against the same dataset: (1) {@code _id} equals the
     * column's value when the id column IS projected via {@code KEEP}; (2) {@code _id} still equals the column's value
     * when the id column is NOT projected (the reader pins it into its projection, then the top-level Project drops it
     * from the user's output); (3) a query WITHOUT {@code METADATA _id} returns the plain rows unchanged (the synthetic
     * path is untouched).
     */
    public void testIdFromColumn() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict declaration whose only knob is _id.path = first_name (a keyword column). The columns keep their
        // inferred names/types; _id is stamped from first_name's value.
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, new java.util.LinkedHashMap<>(), null, "first_name")
        );

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_id_from_col",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        // (1) _id equals first_name when the id column IS projected.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_id_from_col METADATA _id | KEEP _id, first_name | SORT first_name | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            int idIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("_id");
            int nameIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("first_name");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_id is stamped from the first_name column", row.get(idIdx), equalTo(row.get(nameIdx)));
            }
            assertThat(rows.get(0).get(nameIdx).toString(), equalTo("Alice"));
        }

        // (2) _id STILL equals first_name when the id column is NOT projected — the reader pins first_name into its
        // read projection, and the top-level Project drops it from the user's output.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_id_from_col METADATA _id | KEEP _id, emp_no | SORT emp_no | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.stream().map(ColumnInfo::name).toList(), equalTo(List.of("_id", "emp_no")));
            int idIdx = columns.stream().map(ColumnInfo::name).toList().indexOf("_id");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            // emp_no 1,2,3 map to Alice,Bob,Carol per the fixture.
            assertThat(rows.get(0).get(idIdx).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(idIdx).toString(), equalTo("Bob"));
            assertThat(rows.get(2).get(idIdx).toString(), equalTo("Carol"));
        }

        // (3) WITHOUT METADATA _id the plain rows come back unchanged — the synthetic-_id machinery never runs.
        try (var response = run(syncEsqlQueryRequest("FROM employees_id_from_col | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.stream().map(ColumnInfo::name).toList(), equalTo(List.of("emp_no", "first_name")));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(2).get(0), equalTo(3));
            assertThat(rows.get(2).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testFromMixedIndexAndDatasetMetadataBindsOnBothHalves() throws Exception {
        // METADATA on a heterogeneous FROM must bind on BOTH branches and strip neither. The plan-global metadata
        // strip in Analyzer.planWithoutSyntheticAttributes fires only on the legacy EXTERNAL command's nameless leaf
        // (an ExternalRelation whose datasetName() == null — the gate added in #149796); a FROM <dataset> leaf always
        // carries a dataset name and the index leaf is a regular relation, so neither half matches the gate. This
        // asserts the dataset's _index survives the union (resolving to the dataset name) alongside the index's own.
        assertAcked(
            client().admin().indices().prepareCreate("metadata_idx").setMapping("emp_no", "type=integer", "first_name", "type=keyword")
        );
        prepareIndex("metadata_idx").setSource(Map.of("emp_no", 100, "first_name", "Zoe")).get();
        client().admin().indices().prepareRefresh("metadata_idx").get();

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            )
        );

        // 3 dataset rows (emp_no 1,2,3) + 1 index row (emp_no 100); SORT makes the per-row _index assertion deterministic.
        // No explicit KEEP _index: METADATA surfaces unconditionally on the FROM path, so a regression that broadened the
        // strip gate to fire when a FROM <dataset> leaf is present would drop _index from the union output entirely and
        // fail hasItem("_index"). An explicit KEEP _index would mask exactly that regression — it lands in Analyzer's
        // explicitlyKept set and is never stripped.
        try (var response = run(syncEsqlQueryRequest("FROM metadata_idx, employees METADATA _index | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("_index must bind on the heterogeneous union; got " + names, names, hasItem("_index"));
            int indexCol = names.indexOf("_index");
            int empNoCol = names.indexOf("emp_no");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(4));
            // Dataset rows (emp_no 1,2,3) sort ahead of the index row (100). The dataset half is not stripped: its rows
            // carry _index = the dataset name; the index row carries its own _index. emp_no is asserted per row so the
            // ordering this relies on stays self-evident if the fixtures ever change.
            assertThat(((Number) rows.get(0).get(empNoCol)).intValue(), equalTo(1));
            assertThat(rows.get(0).get(indexCol).toString(), equalTo("employees"));
            assertThat(((Number) rows.get(1).get(empNoCol)).intValue(), equalTo(2));
            assertThat(rows.get(1).get(indexCol).toString(), equalTo("employees"));
            assertThat(((Number) rows.get(2).get(empNoCol)).intValue(), equalTo(3));
            assertThat(rows.get(2).get(indexCol).toString(), equalTo("employees"));
            assertThat(((Number) rows.get(3).get(empNoCol)).intValue(), equalTo(100));
            assertThat(rows.get(3).get(indexCol).toString(), equalTo("metadata_idx"));
        }
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

    private static PutViewAction.Request putViewRequest(String name, String query) {
        return new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(name, query));
    }

    private static DeleteViewAction.Request deleteViewRequest(String name) {
        return new DeleteViewAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }
}
