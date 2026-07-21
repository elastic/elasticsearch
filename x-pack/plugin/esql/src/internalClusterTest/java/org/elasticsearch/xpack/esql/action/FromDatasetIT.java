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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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
public class FromDatasetIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;
    private Path csvFixtureAlt;
    private Path csvUnsignedLongFixture;
    private Path ndjsonUnsignedLongFixture;
    private Path ndjsonFixture;
    private Path csvDateFixture;
    private Path ndjsonDateFixture;

    // 10/Oct/2000:13:55:36 -0700 == 2000-10-10T20:55:36Z. The literal epoch pins the zone-aware parse: a formatter that
    // discarded the -0700 offset would land at 13:55:36Z (971204136000), 7h earlier — the exact zone-dropping bug the
    // declared-format read path must not have.
    private static final long ACCESS_LOG_EPOCH_MILLIS = 971211336000L;
    private static final String ACCESS_LOG_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
    // A numeric epoch-second token and the instant it names, shared across the cross-format parity test so a single
    // declared `format: epoch_second` reads the same instant from every carrier (Parquet int64, CSV/TSV/NDJSON token).
    private static final long EPOCH_SECOND_TOKEN = 1704067200L; // 2024-01-01T00:00:00Z, in seconds
    private static final long EPOCH_SECOND_MILLIS = 1704067200000L;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        // The base already registers HttpDataSourcePlugin + TestDataSourcePlugin + the enterprise license plugin;
        // this adds the format readers our tests exercise beyond CSV.
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    /** Determinism over planner-regression diversity here — these tests pin specific plan shapes. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixture() throws IOException {
        csvFixture = createTempFile("dataset-fixture-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");
        csvFixtureAlt = createTempFile("dataset-fixture-alt-", ".csv");
        Files.writeString(csvFixtureAlt, String.join("\n", "emp_no:integer,first_name:keyword", "10,Diana", "11,Eve") + "\n");
        // unsigned_long fixtures: the interesting values live in (2^63, 2^64) — they overflow a signed long, so a
        // reader that quietly routes them through the long path corrupts them rather than failing.
        csvUnsignedLongFixture = createTempFile("dataset-ul-fixture-", ".csv");
        Files.writeString(
            csvUnsignedLongFixture,
            String.join("\n", "id,v", "1,0", "2,9223372036854775808", "3,18446744073709551615") + "\n"
        );
        ndjsonUnsignedLongFixture = createTempFile("dataset-ul-fixture-", ".ndjson");
        Files.writeString(
            ndjsonUnsignedLongFixture,
            String.join("\n", "{\"id\":1,\"v\":0}", "{\"id\":2,\"v\":9223372036854775808}", "{\"id\":3,\"v\":18446744073709551615}") + "\n"
        );
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
        // Access-log-style timestamps carrying an explicit -0700 zone; the physical column is text (keyword) and a
        // declared `date` + `format` reparses it. `note` stays a plain keyword.
        csvDateFixture = createTempFile("dataset-date-fixture-", ".csv");
        Files.writeString(
            csvDateFixture,
            String.join("\n", "ts:keyword,note:keyword", "10/Oct/2000:13:55:36 -0700,alpha", "11/Oct/2000:09:00:00 -0700,beta") + "\n"
        );
        ndjsonDateFixture = createTempFile("dataset-date-fixture-", ".ndjson");
        Files.writeString(
            ndjsonDateFixture,
            String.join(
                "\n",
                "{\"ts\":\"10/Oct/2000:13:55:36 -0700\",\"note\":\"alpha\"}",
                "{\"ts\":\"11/Oct/2000:09:00:00 -0700\",\"note\":\"beta\"}"
            ) + "\n"
        );
    }

    /**
     * Names every dataset a {@code testXxx} body PUTs via the raw {@link PutDatasetAction} (our tests carry declared
     * mappings, which the base {@code registerDataset} helper does not model). New tests must register their dataset
     * name here so the SUITE-scoped cluster doesn't carry state across methods — {@link #cleanupRawDatasets} deletes
     * them (the base {@code cleanupRegistry} only tears down datasets created through its own helpers).
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
        "employees_headerless_strict",
        "employees_headerless_dynamic",
        "employees_absent_warn",
        "employees_parity_strict",
        "employees_parity_dynamic",
        "employees_order_strict",
        "employees_order_dynamic",
        "employees_rename_nonstrict",
        "employees_rename_keep",
        "employees_ndjson_rename_strict",
        "employees_ndjson_rename_nonstrict",
        "employees_parquet_rename",
        "employees_rename_multi",
        "employees_swap",
        "employees_id_from_col",
        "employees_id_bad_path",
        "employees_id_renamed",
        "employees_strict_hive",
        "employees_strict_hive_collide",
        "employees_parquet_type_conflict",
        "employees_strict_wrong_order",
        "logs_csv_strict",
        "logs_csv_nonstrict",
        "logs_csv_filelevel",
        "logs_csv_iso",
        "logs_parquet_format",
        "logs_ndjson",
        "logs_csv_rename",
        "logs_csv_gz",
        "logs_csv_gz_strict",
        "logs_tsv_gz_strict",
        "logs_ndjson_gz_strict",
        "logs_csv_gz_strict_multi",
        "logs_parquet_strict_format",
        "logs_noext_strict",
        "employees_extensionless",
        "logs_id_partition",
        "logs_partition_collide_nonstrict",
        "logs_partition_collide_path",
        "employees_strict_coerce",
        "employees_strict_uncoercible",
        "employees_strict_coerce_multi",
        "employees_int_to_long",
        "employees_declared_narrow",
        "logs_parquet_string_date",
        "coerced_long_to_double",
        "logs_csv_equiv",
        "logs_parquet_equiv",
        "long_csv_equiv",
        "long_parquet_equiv",
        "typed_strings_parquet",
        "empty_string_double",
        "logs_deferred_coerce",
        "logs_bad_date_token",
        "logs_bad_date_failfast",
        "logs_csv_bad_date_failfast",
        "logs_ndjson_bad_date_failfast",
        "logs_ndjson_bad_date_permissive",
        "employees_divergent_multi",
        "tsv_declared_type",
        "tsv_declared_date",
        "tsv_declared_rename",
        "mapped_ds_for_view",
        "mapped_ds_for_subquery",
        "ndjson_mv_coerce",
        "logs_ts_declared_long",
        "logs_date_inferred",
        "rp_micros",
        "rp_nanos",
        "rp_millis",
        "rp_bare_s",
        "rp_bare_ms",
        "rp_date",
        "rpn_bare_ns",
        "rpn_bare_s",
        "rpn_micros",
        "rp_prune_s",
        "rpn_prune_s",
        "scale_diff_a",
        "scale_diff_b",
        "scale_diff_c",
        "scale_diff_d",
        "scale_diff_e",
        "scale_diff_f",
        "scale_diff_g",
        "scale_xdecl_seconds",
        "scale_xdecl_millis",
        "cb_inferred",
        "cb_nonstrict",
        "cb_strict",
        "cb_ndjson_inferred",
        "cb_ndjson_nonstrict",
        "cb_ndjson_strict",
        "epoch_ovf_pq_null",
        "epoch_ovf_pq_skip",
        "epoch_ovf_pq_fail",
        "epoch_ovf_csv_null",
        "epoch_ovf_csv_skip",
        "epoch_ovf_csv_fail",
        "epoch_ovf_nj_null",
        "epoch_ovf_nj_skip",
        "epoch_ovf_nj_fail"
    );

    /**
     * Names every {@code testXxx} body creates via {@link PutViewAction}. As with datasets, the SUITE-scoped
     * cluster requires explicit teardown so views don't leak across methods.
     */
    private static final Set<String> CREATED_VIEWS = Set.of("employees_view", "employees_filtered_view", "mapped_dataset_view");

    @After
    public void cleanupViews() throws Exception {
        for (String view : CREATED_VIEWS) {
            try {
                client().execute(DeleteViewAction.INSTANCE, deleteViewRequest(view)).get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted by the test itself
            } catch (Exception e) {
                logger.warn("view cleanup [{}] failed", view, e);
            }
        }
    }

    /**
     * Tears down the datasets our tests create through the raw {@link PutDatasetAction} (they carry declared mappings,
     * which the base {@code registerDataset} helper does not model, so they are not in the base registry). The base
     * {@code cleanupRegistry} runs alongside this and clears anything created through its own helpers; a distinct name
     * keeps both from overriding each other.
     */
    @After
    public void cleanupRawDatasets() throws Exception {
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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

    public void testFromDatasetWhereMvInRange() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        // mv_in_range is source-agnostic: over a dataset there is no Lucene pushdown, so it runs in the compute engine
        // over the blocks the CSV reader produces. emp_no in [2,3] keeps Bob(2) and Carol(3), drops Alice(1).
        try (var response = run(syncEsqlQueryRequest("FROM employees | WHERE mv_in_range(emp_no, 2, 3) | SORT emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(1).get(0), equalTo(3));
            assertThat(rows.get(1).get(1).toString(), equalTo("Carol"));
        }
    }

    public void testFromExtensionlessResourceDrivenByExplicitFormat() throws Exception {
        // The headline fix: a resource with no file extension carries no inferable format, so the dataset's
        // explicit `format` setting is the only thing that can select the reader. The fixture is pipe-delimited
        // (header included) and the dataset also carries the csv-specific `delimiter`, so the rows parse into
        // the expected columns only if both settings reach the CsvFormatReader at query time. DataSourceCrudRestIT
        // asserts these settings round-trip into cluster state; this confirms they drive the actual read.
        Path noExtFixture = createTempDir().resolve("employees_no_ext");
        Files.writeString(noExtFixture, String.join("\n", "emp_no:integer|first_name:keyword", "1|Alice", "2|Bob", "3|Carol") + "\n");

        registerDataSource("local_ds", Map.of());
        registerDataset("employees_extensionless", "local_ds", noExtFixture.toUri().toString(), Map.of("format", "csv", "delimiter", "|"));

        try (var response = run(syncEsqlQueryRequest("FROM employees_extensionless | SORT emp_no | LIMIT 10"), TIMEOUT)) {
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

    public void testStrictDeclaredNamesAbsentFromHeaderReadNull() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // A declared schema binds by name. The declaration names id/name, which the file (header emp_no,first_name)
        // does not have and no `path` maps, so both read null with a warning. To rename emp_no->id the supported
        // mechanism is `path` — see testStrictDeclaredSchemaRenamesColumnsViaSource.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

        // Both declared names are absent from the file, so every row reads null — the declaration over-claims the file.
        try (var response = run(syncEsqlQueryRequest("FROM employees_strict | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(0), nullValue());
                assertThat(row.get(1), nullValue());
            }
        }
    }

    public void testStrictDeclaredDateNanosReadsWholeNumberAsEpochNanos() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // The headline user story: a lake column holding a raw epoch-nanoseconds whole number can now be pinned to
        // date_nanos. Inference would have produced LONG (a bare number carries no unit), so declaring the column is
        // the only way to present it at its real precision — and the declared type is what names the unit: under
        // `date_nanos` the number IS epoch-nanos (under `datetime` it would be epoch-millis).
        Path nanosFixture = createTempDir().resolve("events.csv");
        Files.writeString(
            nanosFixture,
            String.join(
                "\n",
                "id,event_time",
                "1,1700000000123456789",
                "2,1700000000000000000",
                // sub-millisecond digits survive: the whole point of the type
                "3,1700000000999999999"
            ) + "\n"
        );

        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("event_time", new DatasetFieldMapping("date_nanos", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "events_nanos",
                    "local_ds",
                    nanosFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM events_nanos | SORT id | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(1).name(), equalTo("event_time"));
            assertThat(
                "the column presents as date_nanos, not the long inference would have given",
                response.columns().get(1).type().typeName(),
                equalTo("date_nanos")
            );

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            // Rendered at nanosecond precision — the identity epoch-nanos reinterpret, no rescaling. (The renderer
            // trims trailing zeros, so a whole-second instant shows as .000Z; the sub-millisecond digits below are
            // the ones that would have been lost had the number been read as epoch-millis.)
            assertThat(rows.get(0).get(1).toString(), equalTo("2023-11-14T22:13:20.123456789Z"));
            assertThat(rows.get(1).get(1).toString(), equalTo("2023-11-14T22:13:20.000Z"));
            assertThat(rows.get(2).get(1).toString(), equalTo("2023-11-14T22:13:20.999999999Z"));
        }
    }

    public void testNonStrictDeclaredSchemaOverridesDeclaredColumnsAndKeepsInferredRest() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict (dynamic:true) declaration over the CSV fixture (emp_no:integer, first_name:keyword). We declare
        // only emp_no, pinning it to LONG (no rename); first_name is left to inference. The result must show the
        // declared type override AND the inferred remainder.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

        // Strict declaration that RENAMES via `source`: physical emp_no/first_name are exposed as id/name. A declared
        // path binds by name, so the declared order need not match the file's; the logical names id/name are what the
        // query sees. This declaration happens to be in file order, which is why it passed even when binding was
        // positional — see testStrictAndDynamicAgreeOnFullyDeclaredHeaderlessCsv for the case that pins the binding.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    /**
     * The #1307 invariant, in product terms: strict and dynamic are orthogonal to every other dimension. Over the same
     * file, with a declaration that names every physical column, the two must return identical results — a declaration
     * that covers the whole file leaves inference nothing to decide, so the dynamic knob cannot matter.
     * <p>
     * The declaration is deliberately NOT in file order: {@code dept} is declared first but names {@code col2}. Binding
     * a declared path by its declaration position — the bug — serves col0's value under {@code dept} on the strict read
     * while dynamic reads it correctly, so the two disagree. Binding by name makes the knob a no-op, as it must be.
     */
    public void testStrictAndDynamicAgreeOnFullyDeclaredHeaderlessCsv() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path headerless = createTempFile("dataset-headerless-", ".csv");
        Files.writeString(headerless, String.join("\n", "1,Alice,Engineering", "2,Bob,Sales", "3,Carol,Support") + "\n");

        // Every physical column of the file is declared, in an order that does not match the file's.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("dept", new DatasetFieldMapping("keyword", "col2"));
        properties.put("id", new DatasetFieldMapping("long", "col0"));
        properties.put("name", new DatasetFieldMapping("keyword", "col1"));

        registerHeaderlessCsv("employees_headerless_strict", headerless, DatasetMapping.Dynamic.FALSE, properties);
        registerHeaderlessCsv("employees_headerless_dynamic", headerless, DatasetMapping.Dynamic.TRUE, properties);

        List<List<Object>> strictRows = queryRows("FROM employees_headerless_strict | KEEP id, name, dept | SORT id | LIMIT 10");
        List<List<Object>> dynamicRows = queryRows("FROM employees_headerless_dynamic | KEEP id, name, dept | SORT id | LIMIT 10");

        // Both must be right, not merely equal: agreement alone would also hold if the two broke the same way.
        assertThat(strictRows, hasSize(3));
        assertThat(strictRows.get(0).get(0), equalTo(1L));                          // col0, retyped LONG
        assertThat(strictRows.get(0).get(1).toString(), equalTo("Alice"));          // col1
        assertThat(strictRows.get(0).get(2).toString(), equalTo("Engineering"));    // col2, NOT col0's value
        assertThat(strictRows.get(2).get(2).toString(), equalTo("Support"));

        assertThat(strictRows, equalTo(dynamicRows));
    }

    /**
     * The #1307 invariant in its strongest form: a declaration that covers the whole file IN FILE ORDER reads
     * IDENTICALLY under strict and dynamic — same column names, same column ORDER, same values — with NO {@code KEEP}
     * to normalize. This is the case where the two modes must not diverge on any dimension.
     */
    public void testStrictAndDynamicFullyAgreeNoKeepInFileOrder() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path headerless = createTempFile("dataset-parity-", ".csv");
        Files.writeString(headerless, String.join("\n", "1,Alice,Engineering", "2,Bob,Sales", "3,Carol,Support") + "\n");

        // Declared in file order: col0 -> id, col1 -> name, col2 -> dept. Declaration order == file order, so column
        // order cannot diverge and the two modes must agree on everything.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "col0"));
        properties.put("name", new DatasetFieldMapping("keyword", "col1"));
        properties.put("dept", new DatasetFieldMapping("keyword", "col2"));

        registerHeaderlessCsv("employees_parity_strict", headerless, DatasetMapping.Dynamic.FALSE, properties);
        registerHeaderlessCsv("employees_parity_dynamic", headerless, DatasetMapping.Dynamic.TRUE, properties);

        try (
            var strict = run(syncEsqlQueryRequest("FROM employees_parity_strict | SORT id | LIMIT 10"), TIMEOUT);
            var dynamic = run(syncEsqlQueryRequest("FROM employees_parity_dynamic | SORT id | LIMIT 10"), TIMEOUT)
        ) {
            assertThat(columnNames(strict), equalTo(List.of("id", "name", "dept")));
            assertThat(columnNames(strict), equalTo(columnNames(dynamic)));   // names + ORDER agree, no KEEP
            assertThat(getValuesList(strict), equalTo(getValuesList(dynamic))); // values agree
        }
    }

    /**
     * Column ORDER is the one dimension strict and dynamic still diverge on for an OUT-of-file-order declaration:
     * strict emits declaration order, dynamic emits file order. Pinned here as the known divergence tracked by the
     * column-order convergence follow-up (a dynamic-side change, out of #1307's binding scope).
     */
    public void testStrictDynamicColumnOrderDivergesOutOfFileOrder() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path headerless = createTempFile("dataset-order-", ".csv");
        Files.writeString(headerless, "1,Alice,Engineering\n");

        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("dept", new DatasetFieldMapping("keyword", "col2"));
        properties.put("id", new DatasetFieldMapping("long", "col0"));
        properties.put("name", new DatasetFieldMapping("keyword", "col1"));

        registerHeaderlessCsv("employees_order_strict", headerless, DatasetMapping.Dynamic.FALSE, properties);
        registerHeaderlessCsv("employees_order_dynamic", headerless, DatasetMapping.Dynamic.TRUE, properties);

        try (
            var strict = run(syncEsqlQueryRequest("FROM employees_order_strict | LIMIT 1"), TIMEOUT);
            var dynamic = run(syncEsqlQueryRequest("FROM employees_order_dynamic | LIMIT 1"), TIMEOUT)
        ) {
            assertThat(columnNames(strict), equalTo(List.of("dept", "id", "name")));   // declaration order
            assertThat(columnNames(dynamic), equalTo(List.of("id", "name", "dept")));  // file order
        }
    }

    private static List<String> columnNames(EsqlQueryResponse response) {
        return response.columns().stream().map(ColumnInfo::name).toList();
    }

    /** Registers {@code file} as a headerless CSV dataset, so a declared path names a physical column as {@code col<N>}. */
    private void registerHeaderlessCsv(String name, Path file, DatasetMapping.Dynamic dynamic, Map<String, DatasetFieldMapping> properties)
        throws Exception {
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    name,
                    "local_ds",
                    file.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv", "header_row", false)),
                    new DatasetMapping(new DatasetMapping.Mappings(dynamic, properties))
                )
            )
        );
    }

    private List<List<Object>> queryRows(String query) {
        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            return getValuesList(response);
        }
    }

    public void testNonStrictDeclaredSchemaRenamesViaSource() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict declaration that renames emp_no -> id (and retypes to LONG); first_name is left to inference.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    public void testRenameWithKeepSubsetProjectsRenamedColumn() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // KEEP a subset down to just the renamed column: stresses the projection path under a rename (the column the
        // reader must read is the physical emp_no, exposed as id).
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    public void testDeclaredDateFormatCsvStrictParsesZoneAware() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Strict declaration: `ts` is a date parsed with the access-log pattern (which carries an explicit zone).
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        properties.put("note", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_strict",
                    "local_ds",
                    csvDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        // ts::long is the parsed epoch-millis; SORT ts ASC puts the 10/Oct/2000:13:55:36 -0700 row first. The exact
        // literal proves the -0700 offset was honored (a zone-dropping parse would be 7h off).
        try (var response = run(syncEsqlQueryRequest("FROM logs_csv_strict | SORT ts | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testDeclaredDateFormatCsvNonStrictDateMath() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Non-strict overlay retypes the inferred keyword `ts` to a date with the declared format; date comparison then
        // works on the parsed instants. Only the 11/Oct row (2000-10-11T16:00:00Z) is >= the 2000-10-11T00:00:00Z bound.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_nonstrict",
                    "local_ds",
                    csvDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_csv_nonstrict | WHERE ts >= \"2000-10-11T00:00:00Z\"::datetime | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(1L));
        }
    }

    public void testDeclaredDateFormatWinsOverFileLevelDatetimeFormat() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // File-level datetime_format is a pattern that CANNOT parse the access-log text; the column's own declared
        // format must win for that column, so the value still parses to the exact zone-aware epoch.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        properties.put("note", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_filelevel",
                    "local_ds",
                    csvDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv", "datetime_format", "yyyy-MM-dd")),
                    mapping
                )
            )
        );

        try (
            var response = run(syncEsqlQueryRequest("FROM logs_csv_filelevel | SORT ts | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testNoDeclaredFormatDateColumnParsesIso() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Regression: a declared date column with NO format keeps the ISO/default parse path unchanged.
        Path isoFixture = createTempFile("dataset-iso-fixture-", ".csv");
        Files.writeString(isoFixture, String.join("\n", "ts:keyword", "2000-10-10T20:55:36Z") + "\n");
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("date", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_iso",
                    "local_ds",
                    isoFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM logs_csv_iso | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testDeclaredDateFormatOnParquetRejected() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // A declared date `format` applies as a string parse pattern, or as the epoch unit / parse dialect of a numeric
        // column — but never on an already-temporal physical (an annotated TIMESTAMP infers as datetime), where it
        // could never apply. Such a declaration is rejected loudly at query resolution rather than silently ignored.
        Path parquet = createTempDir().resolve("ts.parquet");
        Files.write(parquet, timestampMillisFixtureBytes(1_000L, 2_000L, 3_000L));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_parquet_format",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );

        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM logs_parquet_format | LIMIT 5"), TIMEOUT).close());
        assertThat(e.getMessage(), containsString("[format] on column [ts]"));
        assertThat(e.getMessage(), containsString("parquet"));
        assertThat(e.getMessage(), containsString("datetime"));
    }

    public void testDeclaredDateFormatNdjsonParsesZoneAware() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // NDJSON already parses dates via the ES DateFormatter; a per-column declared format reparses that column with
        // its own pattern (same zone-aware semantics).
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_ndjson",
                    "local_ds",
                    ndjsonDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM logs_ndjson | SORT ts | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testDeclaredDateFormatFollowsPathRename() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // The physical column `ts` is exposed as logical `event_time` via a `path` rename AND carries a declared format.
        // The format must follow the column through the rename (the spec is logical-keyed; FileSourceFactory physicalizes
        // the key back to `ts` for the reader). If the logical->physical re-keying were inverted, the reader would look
        // up the formatter under the wrong name and the value would fall to the undeclared path.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("event_time", DatasetFieldMapping.withFormat("date", "ts", ACCESS_LOG_FORMAT));
        properties.put("note", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_rename",
                    "local_ds",
                    csvDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_csv_rename | SORT event_time | EVAL ms = event_time::long | KEEP ms | LIMIT 1"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testDeclaredDateFormatOverGzipCsvParsesZoneAware() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // Compressed .csv.gz goes through CompressionDelegatingFormatReader; the declared format must reach the wrapped
        // CSV reader (a missing wrapper override would silently drop it and diverge from the uncompressed result). Format
        // here is inferred from the compound `.csv.gz` extension; testExplicitFormatOverGzipCsvReads covers the sibling
        // case of an explicit `format` setting composing with the same compression suffix.
        Path gz = createTempFile("dataset-date-fixture-", ".csv.gz");
        byte[] csv = ("ts:keyword,note:keyword\n10/Oct/2000:13:55:36 -0700,alpha\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (var out = new java.util.zip.GZIPOutputStream(Files.newOutputStream(gz))) {
            out.write(csv);
        }
        // Non-strict here so this test stays focused on the declared date format; the strict path over a compound
        // `.csv.gz` is covered separately by testStrictOverGzipCsvReads (and its tsv/ndjson twins). The overlay retypes
        // the inferred `ts` to a date with the format.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_gz",
                    "local_ds",
                    gz.toUri().toString(),
                    null,
                    new HashMap<>(),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM logs_csv_gz | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testStrictOverGzipCsvReads() throws Exception {
        // A strict (dynamic:false) mapping over compound-compressed .csv.gz. The strict path derives the reader
        // sourceType from the file name; before the compound-extension fix it last-dotted to "gz" and the query failed
        // with "No operator factory for sourceType: gz". It must now resolve to "csv" (through the compression-unwrapping
        // registry) and read the row. Regression guard for the strict compound-extension read fix.
        assertStrictGzippedTextReads("logs_csv_gz_strict", ".csv.gz", "some_ts,alpha\n", Map.of("header_row", false), "col0", "col1");
    }

    public void testExplicitFormatOverGzipCsvReads() throws Exception {
        // Regression guard for the compressed-read-under-explicit-format fix: an explicit `format` setting used
        // to bypass compression detection entirely (FormatNameResolver.resolveReader looked the format up by
        // name with no regard for the resource's outer compression suffix), so this query used to fail trying
        // to parse raw gzip bytes as CSV. `format: csv` must now compose with the `.csv.gz` suffix exactly like
        // the extension-inferred path does.
        assertStrictGzippedTextReads(
            "logs_csv_gz_explicit_format",
            ".csv.gz",
            "some_ts,alpha\n",
            Map.of("header_row", false, "format", "csv"),
            "col0",
            "col1"
        );
    }

    public void testStrictOverGzipTsvReads() throws Exception {
        assertStrictGzippedTextReads("logs_tsv_gz_strict", ".tsv.gz", "some_ts\talpha\n", Map.of("header_row", false), "col0", "col1");
    }

    public void testStrictOverGzipNdjsonReads() throws Exception {
        // NDJSON is read by JSON key, so no header_row setting applies; the compound `.ndjson.gz` must still resolve
        // through the compression-unwrapping registry to the "ndjson" reader rather than last-dotting to "gz".
        assertStrictGzippedTextReads(
            "logs_ndjson_gz_strict",
            ".ndjson.gz",
            "{\"ts\":\"some_ts\",\"note\":\"alpha\"}\n",
            Map.of(),
            null,
            null
        );
    }

    public void testStrictOverGzipCsvGlobReads() throws Exception {
        // The multi-file half of the fix: resolveStrictMultiFile must derive the reader from a CONCRETE listed file name
        // (listing.path(0)) through the compression-unwrapping registry — not last-dot the raw `*.csv.gz` glob string to
        // "gz" (which would fail with "No operator factory for sourceType: gz"). Regression guard for the multi-file
        // portion of the strict compound-extension read fix.
        Path root = createTempDir();
        writeGzip(root.resolve("part1.csv.gz"), "ts_a,alpha\nts_b,beta\n");
        writeGzip(root.resolve("part2.csv.gz"), "ts_c,gamma\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Headerless text binds a DECLARED schema by name against col<N>, so ts/note bind via `path` to col0/col1.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("keyword", "col0"));
        properties.put("note", new DatasetFieldMapping("keyword", "col1"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_gz_strict_multi",
                    "local_ds",
                    root.toUri() + "*.csv.gz",
                    null,
                    new HashMap<>(Map.of("header_row", false)),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM logs_csv_gz_strict_multi | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L)); // 2 rows from part1 + 1 from part2
        }
    }

    private static void writeGzip(Path target, String content) throws Exception {
        try (var out = new java.util.zip.GZIPOutputStream(Files.newOutputStream(target))) {
            out.write(content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /**
     * Registers a strict (dynamic:false) two-column {@code ts,note} dataset over a gzipped text file of the given
     * compound extension and asserts {@code note} reads back as {@code alpha} — i.e. the strict path resolved the
     * reader through the compound extension (not the "gz" codec suffix).
     */
    private void assertStrictGzippedTextReads(
        String datasetName,
        String ext,
        String content,
        Map<String, Object> settings,
        String tsPath,
        String notePath
    ) throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path gz = createTempFile("dataset-strict-", ext);
        writeGzip(gz, content);
        // Under a strict (DECLARED) schema, columns bind by name against the file's physical names. A headerless text
        // file names its columns col<N> by position, so the declaration must bind ts/note to col0/col1 via `path`;
        // NDJSON binds by JSON key, where the logical names ts/note already match, so it needs no path.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("keyword", tsPath));
        properties.put("note", new DatasetFieldMapping("keyword", notePath));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "local_ds",
                    gz.toUri().toString(),
                    null,
                    new HashMap<>(settings),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM " + datasetName + " | KEEP note | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0).toString(), equalTo("alpha"));
        }
    }

    public void testDeclaredDateFormatOnStrictParquetRejected() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // A declared date format on a never-applicable physical (an annotated TIMESTAMP infers as datetime) is rejected
        // in STRICT mode too, not just non-strict — the strict resolution path bypasses the non-strict overlay's
        // reject, so it must guard the case itself.
        Path parquet = createTempDir().resolve("ts.parquet");
        Files.write(parquet, timestampMillisFixtureBytes(1_000L, 2_000L, 3_000L));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_parquet_strict_format",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_parquet_strict_format | LIMIT 5"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("[format] on column [ts]"));
        assertThat(e.getMessage(), containsString("parquet"));
        assertThat(e.getMessage(), containsString("datetime"));
    }

    public void testStrictDatasetWithUnknowableFormatFailsCleanlyNotNpe() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // A strict dataset over an extensionless path with no `format` setting cannot resolve a reader. Strict now
        // derives the sourceType through the registry (FormatNameResolver.resolveFormatName -> byExtension), which fails
        // loud at resolution with a clean IllegalArgumentException ("Cannot infer format from object name without
        // extension") — propagated unwrapped as a 4xx, never an NPE-wrapped 500.
        Path noExt = createTempFile("dataset-noext-", "");
        Files.writeString(noExt, "id\n1\n");
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_noext_strict",
                    "local_ds",
                    noExt.toUri().toString(),
                    null,
                    new HashMap<>(),
                    mapping
                )
            )
        );

        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM logs_noext_strict | LIMIT 1"), TIMEOUT).close());
        assertThat(e.getMessage(), not(containsString("NullPointerException")));
        assertThat(e.getMessage(), containsString("without extension"));
    }

    public void testNdJsonRenameStrictReadsByPhysicalJsonKey() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        // NDJSON is read BY NAME (JSON key), so rename exercises the reader's logical->physical key resolution: the
        // declared id/name columns must be read from the JSON fields emp_no/first_name.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    public void testParquetRenameOverMultiFileGlobReconciles() throws Exception {
        // Composition of the two axes no single-file rename test crosses: a declared RENAME over a MULTI-FILE parquet
        // glob, so the per-file reconciliation path (main #152847 pins the reader to the per-file projection/attributes)
        // runs WITH our logical->physical rename translation applied at the reader boundary. A wrong merge of that seam
        // (dropping the translate, or feeding the query-unified cols) would read a renamed column from the wrong
        // physical column or mis-cast a per-file page — and every existing rename test is single-file, so it would pass
        // silently. Two identical files => each logical id (physical emp_no) 1,2,3 appears twice.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path dir = createTempDir();
        Files.write(dir.resolve("part1.parquet"), parquetRenameFixtureBytes());
        Files.write(dir.resolve("part2.parquet"), parquetRenameFixtureBytes());
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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
                    "employees_rename_multi",
                    "local_ds",
                    dir.toUri() + "*.parquet",
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        // Full read of all four renamed columns (incl. the flattened nested path) across both files, sorted on the
        // renamed sort key: projection + TopN reconciliation with renames, per file.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_rename_multi | KEEP id, name, comp, dept_code | SORT id | LIMIT 20"),
                TIMEOUT
            )
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(3).name(), equalTo("dept_code"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(6)); // 3 rows x 2 files
            assertThat(rows.get(0).get(0), equalTo(1L)); // physical emp_no under logical id, from either file
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice")); // physical first_name under logical name
            assertThat(rows.get(5).get(0), equalTo(3L));
        }
        // WHERE on a renamed column over the glob: the pushed filter must resolve to the physical column per file.
        try (
            var response = run(syncEsqlQueryRequest("FROM employees_rename_multi | WHERE name == \"Alice\" | KEEP id | SORT id"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("Alice appears once per file, two files", rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(1L));
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

    public void testParquetRenameDeferredExtractionOnRenamedColumns() throws Exception {
        // A TopN keeping >= DEFERRED_COLUMN_MIN (3) non-sort columns over a ColumnExtractorAware parquet source defers
        // their extraction until after the top rows are chosen. Deferred columns are pulled from the file by name, so a
        // `path` rename must physicalize them too — otherwise the extractor looks up the logical names (id/name/dept_code)
        // which the file doesn't have (emp_no/first_name/dept.code) and fails "column [id] is missing".
        putParquetRenameDataset("employees_parquet_rename", writeParquetRenameFixture());
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees_parquet_rename | SORT comp DESC | KEEP id, name, dept_code | LIMIT 2"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(3L));                 // Carol, emp_no=3, comp=300
            assertThat(rows.get(0).get(1).toString(), equalTo("Carol"));
            assertThat(rows.get(0).get(2).toString(), equalTo("OPS"));   // dept_code from dotted dept.code
            assertThat(rows.get(1).get(0), equalTo(2L));                 // Bob, emp_no=2, comp=200
            assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
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
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    public void testStrictColumnarLongToDatetimeCoerces() throws Exception {
        // Strict + columnar: declaring the physical int64 `emp_no` as DATETIME COERCES at read time (Hive/Trino style) —
        // the reader reinterprets the epoch-millis long as a datetime, no reject and no silent null. emp_no 1,2,3 read
        // as the datetimes at epoch millis 1,2,3 (1970-01-01T00:00:00.001/.002/.003Z).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("datetime", "emp_no")); // physical int64 -> coerce to datetime
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_coerce",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM employees_strict_coerce | SORT id | KEEP id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo("1970-01-01T00:00:00.001Z"));
            assertThat(rows.get(1).get(0), equalTo("1970-01-01T00:00:00.002Z"));
            assertThat(rows.get(2).get(0), equalTo("1970-01-01T00:00:00.003Z"));
        }
    }

    public void testDeclaredCoercionsUnderDeferredExtraction() throws Exception {
        // The deferred-extraction twin of the eager coercion tests: a TopN keeping >= DEFERRED_COLUMN_MIN (3) non-sort
        // columns defers their extraction until after the top rows are chosen, so the coerced columns are materialized by
        // ParquetColumnExtractor rather than the eager scan. Both routes run the one DeclaredTypeCoercions.castBlock, so
        // the deferred read must produce identical values. The pairs are chosen to be block-representation-changing so a
        // skipped coercion cannot pass by accident: string->datetime with a declared NON-ISO format (BytesRef -> long;
        // the ISO default cannot parse the token, so falling back to it nulls the cell) and int64->keyword
        // (long -> BytesRef).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetDeferredCoerceFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", "event_ts", ACCESS_LOG_FORMAT));
        properties.put("id_str", new DatasetFieldMapping("keyword", "id")); // physical int64 -> stringify
        properties.put("msg", new DatasetFieldMapping("keyword", null));
        properties.put("pri", new DatasetFieldMapping("integer", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_deferred_coerce",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        // Same query shape as testParquetRenameDeferredExtractionOnRenamedColumns — the shape proven to cross
        // DEFERRED_COLUMN_MIN and defer the three kept non-sort columns.
        try (
            var response = run(syncEsqlQueryRequest("FROM logs_deferred_coerce | SORT pri DESC | KEEP ts, id_str, msg | LIMIT 2"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            // pri 30 -> id 3, second 38 of the access-log minute; pri 20 -> id 2, second 37 (13:55 -0700 == 20:55Z).
            assertThat(rows.get(0).get(0), equalTo("2000-10-10T20:55:38.000Z"));
            assertThat(rows.get(0).get(1).toString(), equalTo("3"));
            assertThat(rows.get(0).get(2).toString(), equalTo("gamma"));
            assertThat(rows.get(1).get(0), equalTo("2000-10-10T20:55:37.000Z"));
            assertThat(rows.get(1).get(1).toString(), equalTo("2"));
            assertThat(rows.get(1).get(2).toString(), equalTo("beta"));
        }
    }

    public void testNumericEpochFormatCoercionUnderDeferredExtraction() throws Exception {
        // The deferred-extraction twin of testDeclaredEpochSecondFormatOnParquetLongCoerces: ParquetColumnExtractor
        // never consults fusedInDecode — it always decodes at the file type and coerces through castBlock with the
        // column's declared formatter — so the deferred (TopN-materialized) read of an int64 + epoch_second column must
        // produce the SAME instant as the eager scan. Pins extractor/eager parity for the new numeric-format arm.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetDeferredCoerceFixture(); // physical int64 id = 1,2,3
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts_epoch", DatasetFieldMapping.withFormat("date", "id", "epoch_second")); // int64 -> date via epoch unit
        properties.put("event_ts", new DatasetFieldMapping("keyword", null));
        properties.put("msg", new DatasetFieldMapping("keyword", null));
        properties.put("pri", new DatasetFieldMapping("integer", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "epoch_deferred",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        // Three kept non-sort columns crosses DEFERRED_COLUMN_MIN, so ts_epoch is materialized by the extractor.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM epoch_deferred | SORT pri DESC | KEEP ts_epoch, event_ts, msg | LIMIT 2"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            // pri 30 -> id 3 -> 3 epoch SECONDS = 3000ms; pri 20 -> id 2 -> 2000ms. An epoch-millis reinterpret
            // (the un-defused fused path) would instead yield 1970-01-01T00:00:00.003Z / .002Z.
            assertThat(rows.get(0).get(0), equalTo("1970-01-01T00:00:03.000Z"));
            assertThat(rows.get(0).get(2).toString(), equalTo("gamma"));
            assertThat(rows.get(1).get(0), equalTo("1970-01-01T00:00:02.000Z"));
            assertThat(rows.get(1).get(2).toString(), equalTo("beta"));
        }
    }

    /**
     * Fixture for {@link #testDeclaredCoercionsUnderDeferredExtraction}: four columns so a TopN keeping three non-sort
     * columns crosses the deferred-extraction threshold, with an access-log date string (declared-format coerce) and an
     * int64 id (stringify coerce) among the deferred columns. Rows are seconds 36/37/38 of the shared access-log minute.
     */
    private Path writeParquetDeferredCoerceFixture() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message logs { required int64 id; required int32 pri; required binary event_ts (UTF8); required binary msg (UTF8); }"
        );
        String[] timestamps = { "10/Oct/2000:13:55:36 -0700", "10/Oct/2000:13:55:37 -0700", "10/Oct/2000:13:55:38 -0700" };
        int[] pris = { 10, 20, 30 };
        String[] msgs = { "alpha", "beta", "gamma" };
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < timestamps.length; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) (i + 1));
                g.add("pri", pris[i]);
                g.add("event_ts", timestamps[i]);
                g.add("msg", msgs[i]);
                writer.write(g);
            }
        }
        Path tempFile = createTempDir().resolve("logs_deferred.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    /**
     * Fixture for the bad-date-token tests: same 4-column shape as
     * {@link #writeParquetDeferredCoerceFixture} (so the wide-projection TopN crosses the
     * deferred-extraction threshold), but the pri=20 row's timestamp is an unparseable token.
     */
    private Path writeParquetBadDateTokenFixture() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message logs { required int64 id; required int32 pri; required binary event_ts (UTF8); required binary msg (UTF8); }"
        );
        String[] timestamps = { "10/Oct/2000:13:55:36 -0700", "definitely-not-a-date", "10/Oct/2000:13:55:38 -0700" };
        int[] pris = { 10, 20, 30 };
        String[] msgs = { "alpha", "beta", "gamma" };
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < timestamps.length; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) (i + 1));
                g.add("pri", pris[i]);
                g.add("event_ts", timestamps[i]);
                g.add("msg", msgs[i]);
                writer.write(g);
            }
        }
        Path tempFile = createTempDir().resolve("logs_bad_token.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private void putBadDateTokenDataset(String name, Map<String, Object> extraSettings) throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetBadDateTokenFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", "event_ts", ACCESS_LOG_FORMAT));
        properties.put("id_str", new DatasetFieldMapping("keyword", "id"));
        properties.put("msg", new DatasetFieldMapping("keyword", null));
        properties.put("pri", new DatasetFieldMapping("integer", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        HashMap<String, Object> settings = new HashMap<>(Map.of("format", "parquet"));
        settings.putAll(extraSettings);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, "local_ds", parquet.toUri().toString(), null, settings, mapping)
            )
        );
    }

    public void testColumnarBadDateTokenNullsCellEagerAndDeferred() throws Exception {
        // The fused string->datetime arm under an explicit null_field policy: an unparseable token
        // nulls THAT cell and the query SUCCEEDS — for BOTH plan shapes over the same cell. The
        // eager KEEP decodes the coerced column in the forward scan; the wide-projection TopN
        // defers it to ParquetColumnExtractor. Pre-fix the eager read hard-failed while the
        // deferred one warned+nulled — same cell, opposite outcome by plan shape. error_mode is
        // set explicitly: the default is fail_fast (columnar readers no longer default to permissive).
        putBadDateTokenDataset("logs_bad_date_token", Map.of("error_mode", "null_field"));

        // Eager: one kept non-sort column stays under the deferred-extraction threshold.
        try (var response = run(syncEsqlQueryRequest("FROM logs_bad_date_token | SORT pri | KEEP ts | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo("2000-10-10T20:55:36.000Z"));
            assertThat("the bad token reads as null, not a thrown read", rows.get(1).get(0), equalTo(null));
            assertThat(rows.get(2).get(0), equalTo("2000-10-10T20:55:38.000Z"));
        }

        // Deferred: same shape as testDeclaredCoercionsUnderDeferredExtraction — three kept
        // non-sort columns cross DEFERRED_COLUMN_MIN, so ts materializes via the extractor.
        try (
            var response = run(syncEsqlQueryRequest("FROM logs_bad_date_token | SORT pri DESC | KEEP ts, id_str, msg | LIMIT 2"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo("2000-10-10T20:55:38.000Z")); // pri 30
            assertThat(rows.get(0).get(2).toString(), equalTo("gamma"));
            assertThat("the bad token reads as null under deferred extraction too", rows.get(1).get(0), equalTo(null)); // pri 20
            assertThat(rows.get(1).get(2).toString(), equalTo("beta"));
        }

        // The nulled cell must be announced: re-run the eager query and read the coordinator's
        // accumulated response Warning headers at completion (same probe as
        // ExternalCsvHivePartitionedIT — the transport client owns the response ref-count, so the
        // listener only inspects headers).
        List<String> coercionWarnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM logs_bad_date_token | SORT pri | KEEP ts | LIMIT 10"),
            ActionListener.running(() -> {
                try {
                    internalCluster().getInstance(TransportService.class)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .filter(w -> w.contains("could not be coerced to the declared column type"))
                        .forEach(coercionWarnings::add);
                } finally {
                    latch.countDown();
                }
            })
        );
        assertTrue("query did not complete within timeout", latch.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertThat("the nulled cell must emit a response Warning header", coercionWarnings, not(empty()));
    }

    public void testColumnarBadDateTokenFailFastFailsQueryMatchingCsv() throws Exception {
        // error_mode: fail_fast routes the same coercion failure to a query abort — on parquet
        // exactly as on CSV, where the same declared date + bad token has always failed the read.
        putBadDateTokenDataset("logs_bad_date_failfast", Map.of("error_mode", "fail_fast"));
        Exception parquetFailure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_bad_date_failfast | SORT pri | KEEP ts | LIMIT 10"), TIMEOUT).close()
        );
        assertThat(parquetFailure.getMessage(), containsString("definitely-not-a-date"));

        // CSV leg: same declared coercion, same bad token, same explicit policy -> same outcome.
        Path csv = createTempFile("dataset-bad-date-", ".csv");
        Files.writeString(csv, "ts:keyword,note:keyword\ndefinitely-not-a-date,alpha\n");
        Map<String, DatasetFieldMapping> csvProps = new LinkedHashMap<>();
        csvProps.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        csvProps.put("note", new DatasetFieldMapping("keyword", null));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_bad_date_failfast",
                    "local_ds",
                    csv.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv", "error_mode", "fail_fast")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, csvProps))
                )
            )
        );
        Exception csvFailure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_csv_bad_date_failfast | KEEP ts | LIMIT 10"), TIMEOUT).close()
        );
        assertThat(csvFailure.getMessage(), containsString("definitely-not-a-date"));
    }

    public void testNdjsonBadDateTokenHonorsErrorPolicyMatchingColumnar() throws Exception {
        // NDJSON's declared-datetime parse failure must obey the SAME ErrorPolicy contract as the columnar and CSV
        // readers: under fail_fast it fails the query (like parquet/CSV); under a non-strict policy (null_field) it
        // nulls THAT cell + emits a response Warning while the query succeeds. Before the fix the NDJSON path
        // null-filled policy-blind — fail_fast silently succeeded and no Warning header was emitted. Both policies are
        // set explicitly here: the default error_mode is fail_fast for every format (columnar included), so the
        // null_field leg is pinned rather than left to the default.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        String content = """
            {"ts":"10/Oct/2000:13:55:36 -0700","note":"alpha"}
            {"ts":"definitely-not-a-date","note":"beta"}
            """;
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        props.put("note", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props));

        // fail_fast -> query aborts on the bad token, matching parquet/CSV.
        Path failFast = createTempFile("dataset-bad-date-ndjson-ff-", ".ndjson");
        Files.writeString(failFast, content);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_ndjson_bad_date_failfast",
                    "local_ds",
                    failFast.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson", "error_mode", "fail_fast")),
                    mapping
                )
            )
        );
        Exception ndjsonFailure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_ndjson_bad_date_failfast | KEEP ts, note | LIMIT 10"), TIMEOUT).close()
        );
        assertThat(ndjsonFailure.getMessage(), containsString("definitely-not-a-date"));

        // null_field policy -> the bad cell nulls, the query succeeds.
        Path permissive = createTempFile("dataset-bad-date-ndjson-perm-", ".ndjson");
        Files.writeString(permissive, content);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_ndjson_bad_date_permissive",
                    "local_ds",
                    permissive.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson", "error_mode", "null_field")),
                    mapping
                )
            )
        );
        try (
            var response = run(syncEsqlQueryRequest("FROM logs_ndjson_bad_date_permissive | SORT note | KEEP ts, note | LIMIT 10"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo("2000-10-10T20:55:36.000Z")); // alpha, good token
            assertThat("the bad token reads as null, not a thrown read", rows.get(1).get(0), equalTo(null)); // beta, bad token
        }

        // The nulled cell must be announced as a response Warning header, same probe as the columnar test.
        List<String> coercionWarnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM logs_ndjson_bad_date_permissive | SORT note | KEEP ts | LIMIT 10"),
            ActionListener.running(() -> {
                try {
                    internalCluster().getInstance(TransportService.class)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .filter(w -> w.contains("could not be coerced to type"))
                        .forEach(coercionWarnings::add);
                } finally {
                    latch.countDown();
                }
            })
        );
        assertTrue("query did not complete within timeout", latch.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertThat("the nulled NDJSON cell must emit a response Warning header", coercionWarnings, not(empty()));
    }

    public void testStringToDatetimeEquivalentAcrossTextAndColumnar() throws Exception {
        // DIRECT text<->columnar consistency: the SAME date string + declared format, read as a CSV token (text parse)
        // and as a Parquet BINARY value (columnar string->date coerce), produces the IDENTICAL instant — because both
        // route through the one DeclaredTypeCoercions.parseDatetimeMillis. Assert the two reads equal EACH OTHER.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        Map<String, DatasetFieldMapping> csvProps = new LinkedHashMap<>();
        csvProps.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        csvProps.put("note", new DatasetFieldMapping("keyword", null));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_csv_equiv",
                    "local_ds",
                    csvDateFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, csvProps))
                )
            )
        );

        Path parquet = writeParquetStringDateFixture();
        Map<String, DatasetFieldMapping> pqProps = new LinkedHashMap<>();
        pqProps.put("id", new DatasetFieldMapping("long", null));
        pqProps.put("ts", DatasetFieldMapping.withFormat("date", "event_ts", ACCESS_LOG_FORMAT));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_parquet_equiv",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, pqProps))
                )
            )
        );

        long csvMs = coerceStringDateToEpoch("logs_csv_equiv");
        long parquetMs = coerceStringDateToEpoch("logs_parquet_equiv");
        assertThat("text and columnar coerce the same string+format to the same instant", parquetMs, equalTo(csvMs));
        assertThat(csvMs, equalTo(ACCESS_LOG_EPOCH_MILLIS));
    }

    public void testStringToLongEquivalentAcrossTextAndColumnar() throws Exception {
        // Same numeric token, read as a CSV token (text parse into the declared type) and as a Parquet BINARY value
        // (columnar string->long coercion), produces the IDENTICAL long: text and columnar are the same idea — the
        // file yields a string, the declaration says what it means.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        Path csv = createTempFile("dataset-string-long-", ".csv");
        Files.writeString(csv, "n:keyword\n42\n");
        Map<String, DatasetFieldMapping> csvProps = new LinkedHashMap<>();
        csvProps.put("n", new DatasetFieldMapping("long", null));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "long_csv_equiv",
                    "local_ds",
                    csv.toUri().toString(),
                    null,
                    // The shared fixture's second row holds an uncoercible token; this test reads only the good row,
                    // so tolerate the bad one with null_field (the default is fail_fast) — keeps both legs symmetric.
                    new HashMap<>(Map.of("format", "csv", "error_mode", "null_field")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, csvProps))
                )
            )
        );

        Path parquet = writeParquetTypedStringsFixture();
        Map<String, DatasetFieldMapping> pqProps = new LinkedHashMap<>();
        pqProps.put("id", new DatasetFieldMapping("long", null));
        pqProps.put("n", new DatasetFieldMapping("long", "s_long"));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "long_parquet_equiv",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet", "error_mode", "null_field")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, pqProps))
                )
            )
        );

        long csvValue = readSingleLong("FROM long_csv_equiv | KEEP n | LIMIT 1");
        long parquetValue = readSingleLong("FROM long_parquet_equiv | SORT id | KEEP n | LIMIT 1");
        assertThat("text and columnar coerce the same token to the same long", parquetValue, equalTo(csvValue));
        assertThat(csvValue, equalTo(42L));
    }

    public void testEmptyStringDeclaredDoubleReproducesReportedFailureWithContext() throws Exception {
        // Reproduces the exact reported failure end to end: a string column of EMPTY strings declared `double`.
        // Double.parseDouble("") threw a bare `NumberFormatException: empty String` with no column or type, on the
        // query that materialized the column. The read still fails under fail_fast, but the client-visible message
        // now names the column and the declared type and points at error_mode -- the original `empty String` detail
        // survives as context, no longer the whole story.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetEmptyStringFixture();
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("id", new DatasetFieldMapping("long", null));
        props.put("d", new DatasetFieldMapping("double", "s")); // s is the empty string ""
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "empty_string_double",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet", "error_mode", "fail_fast")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, props))
                )
            )
        );
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM empty_string_double | KEEP d | LIMIT 10"), TIMEOUT).close()
        );
        String message = e.getMessage();
        assertThat("the original symptom is retained as context", message, containsString("empty String"));
        assertThat("the failing column is named", message, containsString("Column ["));
        assertThat("the declared type is named", message, containsString("declared type [double]"));
        assertThat("the tolerance path is pointed at", message, containsString("error_mode=null_field"));
    }

    private Path writeParquetEmptyStringFixture() throws IOException {
        // One required UTF8 string column holding the empty string -- the exact value from the reported incident.
        MessageType schema = MessageTypeParser.parseMessageType("message empties { required int64 id; required binary s (UTF8); }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            Group g = factory.newGroup();
            g.add("id", 1L);
            g.add("s", "");
            writer.write(g);
        }
        Path tempFile = createTempDir().resolve("empty_string.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    private long readSingleLong(String query) {
        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            return (Long) rows.get(0).get(0);
        }
    }

    private long coerceStringDateToEpoch(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT ts | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            return (Long) rows.get(0).get(0);
        }
    }

    public void testStrictColumnarInt32ToLongCoerces() throws Exception {
        // int32 (salary) declared `long` widens losslessly at read time — the same int->long coercion the ORC unit test
        // exercises, via the same DeclaredTypeCoercions matrix both columnar readers consult (parquet/orc consistency).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("comp", new DatasetFieldMapping("long", "salary")); // int32 -> long coerce
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_int_to_long",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM employees_int_to_long | SORT comp | KEEP comp | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(100L));
            assertThat(rows.get(1).get(0), equalTo(200L));
            assertThat(rows.get(2).get(0), equalTo(300L));
        }
    }

    public void testDeclaredNumericNarrowingCoercesWhereInferredClashWouldNull() throws Exception {
        // The end-to-end contrast to an INFERRED first_file_wins clash (parquet-multifile.parquetFfwAllRows null-fills an
        // int64-vs-INTEGER divergence). Here the INTEGER target for the same physical int64 column comes from an explicit
        // DECLARATION: the coordinator marks it a declared-type column, FileSourceFactory physicalizes that to `emp_no`,
        // and the reader keeps the coercion escape — narrowing int64 -> integer per value instead of null-filling the
        // whole column. Guards the declared-vs-inferred null-fill gate split through the non-strict overlay. The
        // Integer-valued (not Long) non-null results prove both the coercion happened AND the target type is INTEGER.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture(); // physical emp_no int64 = 1,2,3 (all fit in integer)
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", "emp_no")); // int64 -> integer NARROWING (declared)
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_declared_narrow",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM employees_declared_narrow | SORT id | KEEP id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1)); // Integer, coerced from int64 — NOT null (an inferred clash nulls)
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(2).get(0), equalTo(3));
        }
    }

    public void testStrictColumnarUncoercibleTypeRejected() throws Exception {
        // The no-silent-null guarantee holds: a declared type with NO read-time conversion from the physical type
        // (a number has no ip form — even bulk ingest rejects it) is rejected at resolution, not silently nulled.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("ip", "emp_no")); // physical int64 -> ip: no coercion
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_uncoercible",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees_strict_uncoercible | KEEP id | LIMIT 10"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("no read-time conversion exists for this pair"));
        assertThat(e.getMessage(), containsString("id"));
    }

    public void testStrictColumnarLongToDoubleCoerces() throws Exception {
        // "The user declared it double; they told us what they want": the physical int64 coerces long->double at read
        // time, exactly like bulk ingest of a long into a double field. Pre-widening this pair was rejected.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("double", "emp_no")); // physical int64 -> double coerce
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "coerced_long_to_double",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM coerced_long_to_double | SORT id | KEEP id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1.0));
            assertThat(rows.get(1).get(0), equalTo(2.0));
            assertThat(rows.get(2).get(0), equalTo(3.0));
        }
    }

    public void testStrictColumnarStringColumnsCoerceToDeclaredTypes() throws Exception {
        // Columnar string columns parse into any declared type — the same thing text formats do — and a token the
        // declared type cannot coerce nulls THAT cell (bulk leniency), never failing the read or reading garbage.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetTypedStringsFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("v_long", new DatasetFieldMapping("long", "s_long"));
        properties.put("v_double", new DatasetFieldMapping("double", "s_double"));
        properties.put("v_bool", new DatasetFieldMapping("boolean", "s_bool"));
        properties.put("v_ip", new DatasetFieldMapping("ip", "s_ip"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "typed_strings_parquet",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    // Row 2 carries strings that cannot coerce to the declared long/double/ip; the test asserts they
                    // null and the query succeeds, which is null_field (opt-in) behavior — the default is fail_fast.
                    new HashMap<>(Map.of("format", "parquet", "error_mode", "null_field")),
                    mapping
                )
            )
        );
        try (
            var response = run(
                syncEsqlQueryRequest("FROM typed_strings_parquet | SORT id | KEEP v_long, v_double, v_bool, v_ip | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            // row 1: every token parses into its declared type
            assertThat(rows.get(0).get(0), equalTo(42L));
            assertThat(rows.get(0).get(1), equalTo(2.5));
            assertThat(rows.get(0).get(2), equalTo(true));
            assertThat(rows.get(0).get(3), equalTo("10.20.30.40"));
            // row 2: unparseable tokens null their cells only; the parseable boolean still reads
            assertNull(rows.get(1).get(0));
            assertNull(rows.get(1).get(1));
            assertThat(rows.get(1).get(2), equalTo(false));
            assertNull(rows.get(1).get(3));
        }
    }

    private Path writeParquetTypedStringsFixture() throws IOException {
        // String columns carrying typed tokens (row 1) and unparseable tokens (row 2, except the boolean).
        MessageType schema = MessageTypeParser.parseMessageType(
            "message typed { required int64 id; required binary s_long (UTF8); required binary s_double (UTF8);"
                + " required binary s_bool (UTF8); required binary s_ip (UTF8); }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            Group ok = factory.newGroup();
            ok.add("id", 1L);
            ok.add("s_long", "42");
            ok.add("s_double", "2.5");
            ok.add("s_bool", "true");
            ok.add("s_ip", "10.20.30.40");
            writer.write(ok);
            Group bad = factory.newGroup();
            bad.add("id", 2L);
            bad.add("s_long", "not-a-long");
            bad.add("s_double", "not-a-double");
            bad.add("s_bool", "false");
            bad.add("s_ip", "999.999.999.999");
            writer.write(bad);
        }
        Path tempFile = createTempDir().resolve("typed_strings.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    public void testStrictColumnarLongToDatetimeCoercesMultiFile() throws Exception {
        // Same coercion as above, but over a MULTI-FILE glob: the multi-file strict path reads ONE anchor file's footer
        // to validate the pair, then every file's reader coerces int64 -> datetime. Two identical files => emp_no
        // 1,2,3 twice, all coerced.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path dir = createTempDir();
        Files.write(dir.resolve("part1.parquet"), parquetRenameFixtureBytes());
        Files.write(dir.resolve("part2.parquet"), parquetRenameFixtureBytes());
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("datetime", "emp_no")); // physical int64 -> coerce to datetime
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_coerce_multi",
                    "local_ds",
                    dir.toUri() + "*.parquet",
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM employees_strict_coerce_multi | SORT id | KEEP id | LIMIT 20"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(6));
            assertThat(rows.get(0).get(0), equalTo("1970-01-01T00:00:00.001Z"));
            assertThat(rows.get(5).get(0), equalTo("1970-01-01T00:00:00.003Z"));
        }
    }

    private void putParquetRenameDataset(String name, Path parquet) {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    public void testStrictCsvDeclaredColumnAbsentReadsNullPartialMatch() throws Exception {
        // A declared schema binds by name. emp_no/first_name are in the 2-column file and bind; the extra declared
        // `department` is absent, so it reads null with one per-dataset warning while the other two read real data.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> tooWide = new LinkedHashMap<>();
        tooWide.put("emp_no", new DatasetFieldMapping("integer", null));
        tooWide.put("first_name", new DatasetFieldMapping("keyword", null));
        tooWide.put("department", new DatasetFieldMapping("keyword", null)); // fixture has only 2 columns
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, tooWide));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_wrong_order",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );
        // The partial-match case: emp_no/first_name are present and read real data; the extra declared `department`
        // is absent from the 2-column file, so it reads null (with a per-dataset warning) rather than failing.
        try (var response = run(syncEsqlQueryRequest("FROM employees_strict_wrong_order | SORT emp_no | LIMIT 5"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(3));
            assertThat(columns.get(2).name(), equalTo("department"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(1)); // emp_no present (declared integer)
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice")); // first_name present
            for (List<Object> row : rows) {
                assertThat(row.get(2), nullValue()); // department absent -> null
            }
        }
    }

    /** End-to-end: the absent-declared-column warning reaches the client as a response Warning header. */
    public void testAbsentDeclaredColumnEmitsResponseWarning() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("integer", null));
        properties.put("first_name", new DatasetFieldMapping("keyword", null));
        properties.put("department", new DatasetFieldMapping("keyword", null)); // absent from the 2-column fixture
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_absent_warn",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        // Read the coordinator's accumulated response Warning headers at completion (same probe as the coercion tests).
        List<String> warnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM employees_absent_warn | SORT emp_no | LIMIT 5"),
            ActionListener.running(() -> {
                try {
                    internalCluster().getInstance(TransportService.class)
                        .getThreadPool()
                        .getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .filter(w -> w.contains("declared column [department] is not present"))
                        .forEach(warnings::add);
                } finally {
                    latch.countDown();
                }
            })
        );
        assertTrue("query did not complete within timeout", latch.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertThat("the absent declared column must emit a response Warning header", warnings, not(empty()));
    }

    public void testDeclaredTypeConflictingWithPhysicalParquetTypeRejected() throws Exception {
        // Parquet columns carry their own type. A declared type with a defined read-time coercion (e.g. long->datetime,
        // long->keyword) is coerced; one with NO coercion (long->ip here — the ip mapper only ingests string tokens) is
        // rejected at resolution with an actionable message rather than dying deep in the engine or reading as silent null.
        Path parquet = writeParquetRenameFixture();
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("emp_no", new DatasetFieldMapping("ip", null)); // physical int64!
        properties.put("first_name", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_parquet_type_conflict",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees_parquet_type_conflict | SORT first_name | LIMIT 5"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("no read-time conversion exists for this pair"));
        assertThat(e.getMessage(), containsString("emp_no"));
        assertThat(e.getMessage(), containsString("long"));
    }

    /** Annotated TIMESTAMP(MILLIS) fixture — infers as {@code datetime} (MICROS/NANOS infer as {@code date_nanos}). */
    private byte[] timestampMillisFixtureBytes(long... millisValues) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message logs { required int64 ts (TIMESTAMP(MILLIS,true)); }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (long millis : millisValues) {
                Group g = factory.newGroup();
                g.add("ts", millis);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    private byte[] timestampMicrosFixtureBytes(long... microsValues) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message logs { required int64 ts (TIMESTAMP(MICROS,true)); }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (long micros : microsValues) {
                Group g = factory.newGroup();
                g.add("ts", micros);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    private byte[] dateFixtureBytes(int... days) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message logs { required int32 d (DATE); }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int day : days) {
                Group g = factory.newGroup();
                g.add("d", day);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    /**
     * End-to-end regression pin for the declared-{@code long}-over-{@code TIMESTAMP(MICROS)} filter-pushdown bug:
     * such a column decodes to epoch-NANOS, but the file's row-group statistics are in MICROS. Before the fix a
     * pushed {@code WHERE ts == <nanos>} predicate was compared against micros stats and pruned the matching row
     * group, so the query returned zero rows — filtering for exactly the value the column reads back found nothing.
     */
    public void testWhereOnDeclaredLongTimestampMatchesThroughEngine() throws Exception {
        Path root = createTempDir();
        long microsA = 1_600_000_000_000_000L; // reads back as 1_600_000_000_000_000_000 nanos
        long microsB = 1_700_000_000_000_000L;
        Files.write(root.resolve("logs.parquet"), timestampMicrosFixtureBytes(microsA, microsB));

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("long", null)); // declare the timestamp column as long (epoch-nanos)
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_ts_declared_long",
                    "local_ds",
                    root.toUri() + "*.parquet",
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );

        // Filter for exactly the nanos value the first row reads back; it must be found (row group not wrongly pruned).
        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_ts_declared_long | WHERE ts == 1600000000000000000 | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat("the matching row must survive filter pushdown", getValuesList(response).get(0).get(0), equalTo(1L));
        }
        // The IN path had the same hazard (translateLongIn); the row must survive it too.
        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_ts_declared_long | WHERE ts IN (1600000000000000000) | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat("the matching row must survive IN pushdown", getValuesList(response).get(0).get(0), equalTo(1L));
        }
    }

    /**
     * End-to-end regression pin for the DATE-column filter-pushdown rounding bug (no declared mapping — a plain
     * inferred {@code date} column). A DATE stores whole days; the row group holds day 19723 (2024-01-01). Before the
     * fix the day bound was rounded with {@code floorDiv} for every operator, so {@code d < 2024-01-01T00:00:00.001Z}
     * pushed {@code day < 19723} and pruned the [19723,19723] row group, and {@code d != 2024-01-01T00:00:00.001Z}
     * pushed {@code notEq(19723)} and pruned it too — both dropped the row that genuinely matches.
     */
    public void testWhereLessThanAndNotEqualsOnDateColumnMatchThroughEngine() throws Exception {
        Path root = createTempDir();
        Files.write(root.resolve("dates.parquet"), dateFixtureBytes(19723)); // 2024-01-01, single row group

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_date_inferred",
                    "local_ds",
                    root.toUri() + "*.parquet",
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    null // inferred: no declared mapping, the column reads back as datetime
                )
            )
        );

        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_date_inferred | WHERE d < TO_DATETIME(\"2024-01-01T00:00:00.001Z\") | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat("the matching day must survive < pushdown", getValuesList(response).get(0).get(0), equalTo(1L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM logs_date_inferred | WHERE d != TO_DATETIME(\"2024-01-01T00:00:00.001Z\") | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat("the matching day must survive != pushdown", getValuesList(response).get(0).get(0), equalTo(1L));
        }
    }

    private Path writeParquetRenameFixture() throws IOException {
        Path tempFile = createTempDir().resolve("employees.parquet");
        Files.write(tempFile, parquetRenameFixtureBytes());
        return tempFile;
    }

    private Path writeParquetStringDateFixture() throws IOException {
        // A Parquet BINARY(UTF8) column holding an access-log date string — the source of a columnar string->date coerce.
        MessageType schema = MessageTypeParser.parseMessageType("message logs { required int64 id; required binary event_ts (UTF8); }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            Group g = factory.newGroup();
            g.add("id", 1L);
            g.add("event_ts", "10/Oct/2000:13:55:36 -0700");
            writer.write(g);
        }
        Path tempFile = createTempDir().resolve("logs.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    public void testStrictColumnarStringToDatetimeWithDeclaredFormatCoerces() throws Exception {
        // The second driving case: a Parquet BINARY(UTF8) column of access-log date strings, declared `date` with a
        // format, is coerced string->datetime at read time through the SAME DeclaredTypeCoercions.parseDatetimeMillis
        // the text readers use — so the parsed instant equals the CSV/NDJSON tests' ACCESS_LOG_EPOCH_MILLIS exactly.
        // Same token + same declared format => same instant across CSV, NDJSON, and Parquet: cross-format equivalence.
        // ts::long recovers the raw epoch millis, proving the -0700 offset was honored (a zone-dropping parse is 7h off).
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetStringDateFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("ts", DatasetFieldMapping.withFormat("date", "event_ts", ACCESS_LOG_FORMAT)); // string -> date
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_parquet_string_date",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM logs_parquet_string_date | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS));
        }
    }

    public void testDeclaredEpochSecondFormatOnParquetLongCoerces() throws Exception {
        // A whole-number Parquet column declared `date` WITH `format: epoch_second` reads the int64 as epoch SECONDS,
        // not the default epoch-millis reinterpret: emp_no 1 -> 1000ms. The format is the epoch unit, exactly as the
        // text readers already treat a numeric token. The column defuses off the fused long->datetime reinterpret onto
        // castBlock, which parses through the format. ts::long recovers the raw epoch millis.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture(); // emp_no int64 = 1,2,3
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", "emp_no", "epoch_second"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "epoch_parquet_nonstrict",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (
            var response = run(
                syncEsqlQueryRequest("FROM epoch_parquet_nonstrict | EVAL ms = ts::long | KEEP ms | SORT ms | LIMIT 1"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(1000L)); // emp_no=1 -> 1 second -> 1000 ms
        }
    }

    public void testDeclaredEpochSecondFormatOnStrictParquetLongCoerces() throws Exception {
        // Same as the non-strict case above, but a STRICT (Dynamic.FALSE) declaration — the strict resolution path
        // legalizes format-on-numeric identically. emp_no 1 -> 1000ms.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = writeParquetRenameFixture();
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", "emp_no", "epoch_second"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "epoch_parquet_strict",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (
            var response = run(
                syncEsqlQueryRequest("FROM epoch_parquet_strict | EVAL ms = ts::long | KEEP ms | SORT ms | LIMIT 1"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(1000L));
        }
    }

    /**
     * The acceptance test: ONE declaration — {@code {type: date, format: epoch_second}} — reads the SAME instant from a
     * numeric epoch-second token regardless of carrier. Parquet holds it as a physical int64 (defused off the fused
     * reinterpret, parsed through the format); CSV / TSV / NDJSON hold it as a numeric token (the text readers'
     * existing declared-format-over-numeric semantic). All four land on {@link #EPOCH_SECOND_MILLIS}. ORC shares the
     * same {@code castBlock} coercion arm but has its OWN routing code, pinned separately by
     * {@code OrcFormatReaderTests.testLongDeclaredDatetimeHonorsEpochSecondFormat}.
     */
    public void testDeclaredEpochSecondFormatSameInstantAcrossFormats() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        Path parquet = writeParquetEpochSecondFixture();
        assertEpochSecondMs("epoch_parquet", "parquet", parquet.toUri().toString());

        Path csv = createTempFile("dataset-epoch-", ".csv");
        Files.writeString(csv, "ts:long\n" + EPOCH_SECOND_TOKEN + "\n");
        assertEpochSecondMs("epoch_csv", "csv", csv.toUri().toString());

        Path tsv = createTempFile("dataset-epoch-", ".tsv");
        Files.writeString(tsv, "ts:long\n" + EPOCH_SECOND_TOKEN + "\n");
        assertEpochSecondMs("epoch_tsv", "tsv", tsv.toUri().toString());

        Path ndjson = createTempFile("dataset-epoch-", ".ndjson");
        Files.writeString(ndjson, "{\"ts\":" + EPOCH_SECOND_TOKEN + "}\n");
        assertEpochSecondMs("epoch_ndjson", "ndjson", ndjson.toUri().toString());
    }

    /**
     * The ClickBench shape. One COMPOSITE declaration — {@code "yyyy-MM-dd HH:mm:ss||epoch_second"} — serves a column
     * whose CARRIER differs per file format: ClickBench's {@code EventTime} is a bare int64 of Unix seconds in parquet
     * (the upstream file carries no logical-type annotation) but the string {@code "2013-07-14 20:38:47"} in NDJSON.
     * ES multi-format patterns try alternatives left-to-right, and string-vs-number is unambiguous, so neither
     * alternative can shadow the other. Both carriers must land on the identical instant under ONE mapping.
     * <p>
     * (Contrast {@code "epoch_second||epoch_millis"}, which would be a silent wrong-answer generator: every millis
     * value also parses as seconds, so the first alternative always wins. Composite alternatives must be mutually
     * unambiguous — this test pins the safe pairing.)
     */
    public void testCompositeFormatServesNumericAndStringCarriers() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        String composite = "yyyy-MM-dd HH:mm:ss||epoch_second";

        // parquet: bare int64 Unix seconds -> the epoch_second alternative fires
        Path parquet = writeParquetEpochSecondFixture();
        assertCompositeMs("composite_parquet", "parquet", parquet.toUri().toString(), composite);

        // ndjson: the same instant as a calendar STRING -> the calendar alternative fires
        Path ndjson = createTempFile("dataset-composite-", ".ndjson");
        Files.writeString(ndjson, "{\"ts\":\"2024-01-01 00:00:00\"}\n");
        assertCompositeMs("composite_ndjson", "ndjson", ndjson.toUri().toString(), composite);

        // csv: same calendar string token
        Path csv = createTempFile("dataset-composite-", ".csv");
        Files.writeString(csv, "ts:keyword\n2024-01-01 00:00:00\n");
        assertCompositeMs("composite_csv", "csv", csv.toUri().toString(), composite);
    }

    public void testAnnotatedTimestampMicrosDeclaredDateNarrows() throws Exception {
        // The Spark/Arrow/Iceberg shape: their parquet writers annotate a timestamp column as TIMESTAMP(MICROS), which
        // infers as date_nanos. Declaring it `date` — the conventional dashboard type — narrows nanos->millis, so the
        // column can be a Kibana time field. Before the narrowing this failed resolution outright ("declared type [date]
        // cannot be read from the file's type [date_nanos]"). The annotation carries the unit, so no format is involved.
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Path parquet = createTempDir().resolve("ts_micros.parquet");
        // 2024-01-01T00:00:00.123456Z in MICROS — sub-millisecond digits present, so truncation is observable
        Files.write(parquet, timestampMicrosFixtureBytes(1704067200_123_456L));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", new DatasetFieldMapping("date", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "annotated_ts_micros",
                    "local_ds",
                    parquet.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM annotated_ts_micros | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // micros -> millis: the .456 microsecond remainder truncates away
            assertThat(rows.get(0).get(0), equalTo(1704067200_123L));
        }
    }

    /** Declares {@code {ts: date, format: <the composite>}} over one dataset and asserts ts recovers EPOCH_SECOND_MILLIS. */
    private void assertCompositeMs(String datasetName, String format, String location, String composite) throws Exception {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, composite));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "local_ds",
                    location,
                    null,
                    new HashMap<>(Map.of("format", format)),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + datasetName + " | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("composite format on [" + format + "]", rows, hasSize(1));
            assertThat(
                "composite format must read the same instant from the [" + format + "] carrier",
                rows.get(0).get(0),
                equalTo(EPOCH_SECOND_MILLIS)
            );
        }
    }

    /** Declares {ts: date, format: epoch_second} over one dataset and asserts ts recovers EPOCH_SECOND_MILLIS. */
    private void assertEpochSecondMs(String datasetName, String format, String location) throws Exception {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, "epoch_second"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "local_ds",
                    location,
                    null,
                    new HashMap<>(Map.of("format", format)),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + datasetName + " | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("format [" + format + "]", rows, hasSize(1));
            assertThat("format [" + format + "] must read the same instant", rows.get(0).get(0), equalTo(EPOCH_SECOND_MILLIS));
        }
    }

    /** Single-column int64 fixture holding the shared epoch-second token, for the cross-format parity test. */
    private Path writeParquetEpochSecondFixture() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message e { required int64 ts; }");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            Group g = factory.newGroup();
            g.add("ts", EPOCH_SECOND_TOKEN);
            writer.write(g);
        }
        Path tempFile = createTempDir().resolve("epoch.parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    /**
     * Reads REAL parquet written by pyarrow (the writer pandas/Spark/Iceberg use), not by this test's own fixture
     * code, across every timestamp shape a lake file actually carries. Each file holds the same instant
     * (2024-01-01T00:00:00Z); each declaration must recover it. This is the end-to-end proof of the unit rule:
     * the annotation wins where present, else the format, else the type.
     * <p>
     * Files are generated by {@code realParquet(...)} below from bytes captured out of pyarrow, so the test is
     * hermetic but the bytes are genuinely third-party.
     */
    public void testRealParquetTimestampShapesAllReachTheSameInstant() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long expected = 1704067200000L; // 2024-01-01T00:00:00Z

        // annotated TIMESTAMP(MICROS) -> infers date_nanos -> declared `date` narrows
        assertRealParquet("rp_micros", "annotated_micros", new DatasetFieldMapping("date", null), expected);
        // annotated TIMESTAMP(NANOS) -> infers date_nanos -> declared `date` narrows
        assertRealParquet("rp_nanos", "annotated_nanos", new DatasetFieldMapping("date", null), expected);
        // annotated TIMESTAMP(MILLIS) -> infers date -> native
        assertRealParquet("rp_millis", "annotated_millis", new DatasetFieldMapping("date", null), expected);
        // bare int64 seconds (the ClickBench shape) -> the format names the unit
        assertRealParquet("rp_bare_s", "bare_seconds", DatasetFieldMapping.withFormat("date", null, "epoch_second"), expected);
        // bare int64 millis -> no format, the type names the unit
        assertRealParquet("rp_bare_ms", "bare_millis", new DatasetFieldMapping("date", null), expected);
        // annotated DATE (days) -> infers date, day-scaled
        assertRealParquet("rp_date", "annotated_date", new DatasetFieldMapping("date", null), expected);
    }

    /** The date_nanos half of the rule, over the same real files. */
    public void testRealParquetTimestampShapesReachDateNanos() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long expectedNanos = 1704067200000000000L;

        // bare int64 nanos -> no format, the type names the unit (nanos)
        assertRealParquetNanos("rpn_bare_ns", "bare_nanos", new DatasetFieldMapping("date_nanos", null), expectedNanos);
        // bare int64 seconds -> the format overrides the type's unit
        assertRealParquetNanos(
            "rpn_bare_s",
            "bare_seconds",
            DatasetFieldMapping.withFormat("date_nanos", null, "epoch_second"),
            expectedNanos
        );
        // annotated TIMESTAMP(MICROS) -> infers date_nanos -> native, exact
        assertRealParquetNanos("rpn_micros", "annotated_micros", new DatasetFieldMapping("date_nanos", null), expectedNanos);
    }

    /**
     * The differential invariant for every unit/scaling bug on this axis: <b>the engine's answer with its
     * optimizations enabled must equal the answer computed from fully-decoded data</b>. Filter pushdown, TopN
     * threshold skipping and stats-answered aggregates each decide what NOT to read by comparing against RAW file
     * statistics; a declared {@code format} (or a scaling annotation) makes the decoded value a different number
     * than the raw one, and every such decision then silently drops rows.
     *
     * <p>Ground truth is a plain {@code KEEP} with no filter, sort or aggregate — that engages no pruning at all, so
     * it observes only decode, which is tested separately. Everything else is asserted against it.
     *
     * <p>The fixture is deliberately 4 columns wide and multi-row-group: the TopN threshold rail only engages past
     * {@code InsertExternalFieldExtraction.DEFERRED_COLUMN_MIN} (3) deferred columns, and a skip decision needs a
     * second row group to skip. A narrow or single-group fixture would go green without executing the bug.
     */
    public void testScalingDifferentialAcrossFilterSortAndAggregate() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Raw epoch SECONDS across two row groups; the largest instant deliberately lives in the second group so a
        // unit-blind DESC threshold skips exactly the group holding the right answer. Here the rescale is driven by a
        // declared FORMAT over a bare int64; the annotation-driven twin below drives it from the file's own annotation.
        long[] rawSeconds = new long[2000];
        for (int i = 0; i < rawSeconds.length; i++) {
            rawSeconds[i] = 1704067200L + i * 60L; // ascending, so the LARGEST lives in the LAST row group
        }
        Path file = writeScalingFixture("scale_seconds", rawSeconds);

        // strict x dynamic is an axis, not a footnote: they take different resolution paths (strict pins the
        // declaration, dynamic overlays it onto inference) and must agree on every other dimension.
        runScalingDifferential(
            List.of(
                new ScalingCell(
                    "scale_diff_a",
                    "dynamic: date + epoch_second",
                    DatasetFieldMapping.withFormat("date", null, "epoch_second"),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "scale_diff_b",
                    "dynamic: date_nanos + epoch_second",
                    DatasetFieldMapping.withFormat("date_nanos", null, "epoch_second"),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "scale_diff_c",
                    "dynamic: date + epoch_millis (identity)",
                    DatasetFieldMapping.withFormat("date", null, "epoch_millis"),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "scale_diff_d",
                    "dynamic: long, no declaration (control)",
                    new DatasetFieldMapping("long", null),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "scale_diff_e",
                    "STRICT: date + epoch_second",
                    DatasetFieldMapping.withFormat("date", null, "epoch_second"),
                    DatasetMapping.Dynamic.FALSE
                ),
                new ScalingCell(
                    "scale_diff_f",
                    "STRICT: date_nanos + epoch_second",
                    DatasetFieldMapping.withFormat("date_nanos", null, "epoch_second"),
                    DatasetMapping.Dynamic.FALSE
                ),
                new ScalingCell(
                    "scale_diff_g",
                    "STRICT: date + epoch_millis (identity)",
                    DatasetFieldMapping.withFormat("date", null, "epoch_millis"),
                    DatasetMapping.Dynamic.FALSE
                )
            ),
            file,
            rawSeconds.length
        );
    }

    /**
     * Annotation-driven twin of {@link #testScalingDifferentialAcrossFilterSortAndAggregate}: the file itself carries a
     * {@code TIMESTAMP(MICROS)} annotation (the Spark / pandas / Iceberg default shape), so the decode scales relative
     * to the raw footer statistics with <b>no declared format at all</b>. Three sub-cells the format-driven twin can
     * never reach, because its fixture is a bare {@code int64}:
     * <ul>
     *   <li><b>inferred {@code date_nanos}</b> — decode is {@code ScaleUp x1000} (raw micros to nanos); the TopN rail
     *       must map the raw micros stat up before comparing, or DESC skips the group holding the true max;</li>
     *   <li><b>declared {@code date}</b> — decode is {@code ScaleDown /1000} (raw micros to millis); here ASC is the
     *       wrong-answer side, the raw stat reading 1000x larger than the decoded bound;</li>
     *   <li><b>declared {@code long}</b> — the sort type carries no relation, so only {@code sortColumnAnnotationScales}
     *       stops the raw passthrough; revert it and the wrong extremum returns silently.</li>
     * </ul>
     * Exercises both the row-group stats rail and the page-index rail over a rescaled annotation.
     */
    public void testScalingDifferentialOverAnnotatedSortColumns() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Raw MICROS across two row groups, ascending so the largest lives in the last group.
        long[] rawMicros = new long[2000];
        for (int i = 0; i < rawMicros.length; i++) {
            rawMicros[i] = 1704067200_000_000L + i * 60_000_000L;
        }
        Path file = writeScalingFixture("scale_micros", rawMicros, "TIMESTAMP(MICROS,true)");

        runScalingDifferential(
            List.of(
                new ScalingCell(
                    "ascale_a",
                    "dynamic: TIMESTAMP(MICROS) -> date_nanos (ScaleUp x1000)",
                    new DatasetFieldMapping("date_nanos", null),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "ascale_b",
                    "dynamic: TIMESTAMP(MICROS) declared date (ScaleDown /1000)",
                    new DatasetFieldMapping("date", null),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "ascale_c",
                    "dynamic: TIMESTAMP(MICROS) declared long (annotation still scales)",
                    new DatasetFieldMapping("long", null),
                    DatasetMapping.Dynamic.TRUE
                ),
                new ScalingCell(
                    "ascale_d",
                    "STRICT: TIMESTAMP(MICROS) -> date_nanos",
                    new DatasetFieldMapping("date_nanos", null),
                    DatasetMapping.Dynamic.FALSE
                ),
                new ScalingCell(
                    "ascale_e",
                    "STRICT: TIMESTAMP(MICROS) declared date",
                    new DatasetFieldMapping("date", null),
                    DatasetMapping.Dynamic.FALSE
                ),
                new ScalingCell(
                    "ascale_f",
                    "STRICT: TIMESTAMP(MICROS) declared long",
                    new DatasetFieldMapping("long", null),
                    DatasetMapping.Dynamic.FALSE
                )
            ),
            file,
            rawMicros.length
        );
    }

    /** A cell of the scaling differential: one dataset declaration over the shared fixture. */
    private record ScalingCell(String dataset, String name, DatasetFieldMapping mapping, DatasetMapping.Dynamic dynamic) {}

    /**
     * Drives every {@link ScalingCell} over {@code file} and asserts the filter / TopN / aggregate paths all agree with
     * fully-decoded ground truth (a plain {@code KEEP} that engages no pruning). Shared by the format-driven and
     * annotation-driven differentials so the two cannot drift on what "agree" means.
     */
    private void runScalingDifferential(List<ScalingCell> cells, Path file, int rowCount) throws Exception {
        List<String> failures = new ArrayList<>();
        for (ScalingCell cell : cells) {
            registerScalingDataset(cell.dataset(), file, cell.mapping(), cell.dynamic());
            List<Long> truth = scalingGroundTruth(cell.dataset());
            assertThat("[" + cell.name() + "] ground truth must see every row", truth, hasSize(rowCount));
            assertThat("ground truth is capped by the LIMIT below, keep the fixture under it", rowCount, lessThan(10000));
            long min = truth.stream().mapToLong(Long::longValue).min().getAsLong();
            long max = truth.stream().mapToLong(Long::longValue).max().getAsLong();

            // Filter pushdown: row-group stats, dictionary, bloom and page-index all ride the same predicate.
            record Probe(String what, String query, Object expected) {}
            List<Probe> probes = List.of(
                new Probe(
                    "WHERE == min",
                    "FROM " + cell.dataset() + " | WHERE ts == " + literalFor(cell.mapping(), min) + " | STATS c = COUNT(*)",
                    truth.stream().filter(v -> v == min).count()
                ),
                new Probe(
                    "WHERE >= max",
                    "FROM " + cell.dataset() + " | WHERE ts >= " + literalFor(cell.mapping(), max) + " | STATS c = COUNT(*)",
                    truth.stream().filter(v -> v >= max).count()
                ),
                // The TopN threshold rail only engages past InsertExternalFieldExtraction.DEFERRED_COLUMN_MIN (3)
                // DEFERRED columns, so these MUST project the filler columns too. Projecting ts alone defers
                // nothing, the rule never fires, and the probe passes without ever running the code it targets.
                new Probe(
                    "SORT ASC LIMIT 1 (wide)",
                    "FROM " + cell.dataset() + " | SORT ts ASC | LIMIT 1 | EVAL v = ts::long | KEEP v, id, pri, msg",
                    min
                ),
                new Probe(
                    "SORT DESC LIMIT 1 (wide)",
                    "FROM " + cell.dataset() + " | SORT ts DESC | LIMIT 1 | EVAL v = ts::long | KEEP v, id, pri, msg",
                    max
                ),
                new Probe("STATS MIN", "FROM " + cell.dataset() + " | STATS m = MIN(ts) | EVAL v = m::long | KEEP v", min),
                new Probe("STATS MAX", "FROM " + cell.dataset() + " | STATS m = MAX(ts) | EVAL v = m::long | KEEP v", max),
                new Probe("STATS COUNT", "FROM " + cell.dataset() + " | STATS c = COUNT(ts)", (long) rowCount)
            );
            for (Probe probe : probes) {
                try (var response = run(syncEsqlQueryRequest(probe.query()), TIMEOUT)) {
                    List<List<Object>> rows = getValuesList(response);
                    Object actual = rows.isEmpty() ? null : rows.get(0).get(0);
                    if (Objects.equals(probe.expected(), actual) == false) {
                        failures.add(
                            "["
                                + cell.name()
                                + "] "
                                + probe.what()
                                + ": expected "
                                + probe.expected()
                                + " but got "
                                + actual
                                + "  (query: "
                                + probe.query()
                                + ")"
                        );
                    }
                }
            }
        }
        assertTrue("the engine disagreed with fully-decoded ground truth:\n  " + String.join("\n  ", failures), failures.isEmpty());
    }

    /**
     * Two datasets over the SAME file with DIFFERENT declarations must each get their own answer. The warm stats
     * entry is keyed on path+mtime+config and carries no declared-schema component, so an extremum harvested under
     * one declaration can be served to the other — both are {@code Long}, so nothing downstream notices the x1000.
     */
    public void testCrossDeclarationWarmStatsDoNotContaminate() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long[] rawSeconds = { 1704067200L, 1704153600L };
        Path file = writeScalingFixture("scale_shared", rawSeconds);

        registerScalingDataset("scale_xdecl_seconds", file, DatasetFieldMapping.withFormat("date", null, "epoch_second"));
        registerScalingDataset("scale_xdecl_millis", file, new DatasetFieldMapping("long", null));

        // Warm the cache through the declared-format dataset first, then read the undeclared one.
        List<Long> declared = scalingGroundTruth("scale_xdecl_seconds");
        assertThat(declared, contains(1704067200000L, 1704153600000L));
        for (int i = 0; i < 2; i++) { // second pass reads warm
            try (var response = run(syncEsqlQueryRequest("FROM scale_xdecl_millis | STATS m = MIN(ts) | KEEP m"), TIMEOUT)) {
                assertThat(
                    "an undeclared read must see the file's RAW seconds, never the neighbouring dataset's rescaled millis",
                    getValuesList(response).get(0).get(0),
                    equalTo(1704067200L)
                );
            }
        }
    }

    /** The ESQL literal for a value in this declaration's decoded domain. */
    private static String literalFor(DatasetFieldMapping mapping, long decoded) {
        return switch (mapping.type()) {
            case "date" -> "TO_DATETIME(" + decoded + ")";
            case "date_nanos" -> "TO_DATE_NANOS(" + decoded + ")";
            default -> String.valueOf(decoded);
        };
    }

    /**
     * Decoded values with NO filter, NO sort and NO aggregate — the one query shape that engages no pruning at all,
     * so it observes decode only. Sorting happens in the test, never in the query: a SORT could engage the very
     * TopN rail whose correctness this is supposed to be the yardstick for.
     */
    private List<Long> scalingGroundTruth(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | EVAL v = ts::long | KEEP v | LIMIT 5000"), TIMEOUT)) {
            List<Long> out = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                out.add(((Number) row.get(0)).longValue());
            }
            out.sort(Long::compareTo);
            return out;
        }
    }

    private void registerScalingDataset(String dataset, Path file, DatasetFieldMapping ts) throws Exception {
        registerScalingDataset(dataset, file, ts, DatasetMapping.Dynamic.TRUE);
    }

    /**
     * @param dynamic {@code FALSE} pins the declaration AS the schema, so it must name every column the file has —
     *                a strict read does no inference to fall back on. That difference in path is exactly why both
     *                modes belong in the matrix: they must agree on every other axis.
     */
    private void registerScalingDataset(String dataset, Path file, DatasetFieldMapping ts, DatasetMapping.Dynamic dynamic)
        throws Exception {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", ts);
        if (dynamic == DatasetMapping.Dynamic.FALSE) {
            properties.put("id", new DatasetFieldMapping("long", null));
            properties.put("pri", new DatasetFieldMapping("integer", null));
            properties.put("msg", new DatasetFieldMapping("keyword", null));
        }
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    dataset,
                    "local_ds",
                    file.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    new DatasetMapping(new DatasetMapping.Mappings(dynamic, properties))
                )
            )
        );
    }

    /**
     * 4 columns (so the TopN deferred-extraction threshold engages) and one row group per row (so there is always a
     * later group for a unit-blind threshold to wrongly skip).
     */
    private Path writeScalingFixture(String name, long[] rawTs) throws IOException {
        return writeScalingFixture(name, rawTs, null);
    }

    /**
     * As {@link #writeScalingFixture(String, long[])} but optionally annotates the {@code ts} column with a parquet
     * logical type (e.g. {@code TIMESTAMP(MICROS,true)}), so the file itself — not a declared format — drives the
     * decode rescale. The raw values are written as-is; the annotation only changes how the reader interprets them.
     */
    private Path writeScalingFixture(String name, long[] rawTs, String tsAnnotation) throws IOException {
        String tsColumn = tsAnnotation == null ? "required int64 ts" : "required int64 ts (" + tsAnnotation + ")";
        MessageType schema = MessageTypeParser.parseMessageType(
            "message scaling { " + tsColumn + "; required int64 id; required int32 pri; required binary msg (UTF8); }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        PlainParquetConfiguration conf = new PlainParquetConfiguration();
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(baos))
                .withConf(conf)
                .withType(schema)
                .withRowGroupSize(256L)
                .withPageSize(64)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rawTs.length; i++) {
                Group g = factory.newGroup();
                g.add("ts", rawTs[i]);
                g.add("id", (long) i);
                g.add("pri", i);
                g.add("msg", "m" + i);
                writer.write(g);
            }
        }
        Path tempFile = createTempDir().resolve(name + ".parquet");
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }

    /**
     * Real-data validation against a genuine ClickBench parquet file (bare int64 {@code EventTime} of Unix seconds),
     * across all THREE declaration modes — inferred, non-strict, strict — with ground truth computed from the raw
     * seconds independently of the product. Skipped unless {@code -Dclickbench.real.parquet=<path>} points at a real
     * file, so CI does not carry the bytes. Values pinned from the actual file used in this run.
     */
    private java.nio.file.Path realClickBenchLocal;

    public void testRealClickBenchEventTimeAcrossAllThreeModes() throws Exception {
        String realPath = System.getProperty("tests.clickbench.real.parquet");
        assumeTrue("set -Dtests.clickbench.real.parquet to a real hits.parquet", realPath != null);
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        long maxSec = 1375127873L;      // real EventTime max in this file
        long minSec = 1372795438L;      // real EventTime min
        String maxIso = "2013-07-29T19:57:53.000Z";

        // MODE 1 — INFERRED: no declaration; EventTime infers as long, unit is raw seconds.
        registerReal("cb_inferred", realPath, null);
        assertQ("cb_inferred INF ==", "FROM cb_inferred | WHERE EventTime == " + maxSec + " | STATS c = COUNT(*)", 1L);
        assertQ("cb_inferred INF MAX", "FROM cb_inferred | STATS m = MAX(EventTime)", maxSec);
        assertQ("cb_inferred INF MIN", "FROM cb_inferred | STATS m = MIN(EventTime)", minSec);
        assertQ(
            "cb_inferred INF SORT DESC",
            "FROM cb_inferred | SORT EventTime DESC | LIMIT 1 | EVAL v = EventTime::long | KEEP v",
            maxSec
        );

        // MODE 2 — NON-STRICT: dynamic:true, declare EventTime {date, epoch_second}; unit becomes millis.
        registerReal("cb_nonstrict", realPath, DatasetMapping.Dynamic.TRUE);
        assertQ("cb_nonstrict ==", "FROM cb_nonstrict | WHERE EventTime == TO_DATETIME(\"" + maxIso + "\") | STATS c = COUNT(*)", 1L);
        assertQ("cb_nonstrict >=", "FROM cb_nonstrict | WHERE EventTime >= TO_DATETIME(\"" + maxIso + "\") | STATS c = COUNT(*)", 1L);
        assertQ(
            "cb_nonstrict SORT DESC",
            "FROM cb_nonstrict | SORT EventTime DESC | LIMIT 1 | EVAL v = EventTime::long | KEEP v",
            maxSec * 1000L
        );
        assertQ("cb_nonstrict MAX", "FROM cb_nonstrict | STATS m = MAX(EventTime) | EVAL v = m::long | KEEP v", maxSec * 1000L);
        assertQ("cb_nonstrict MIN", "FROM cb_nonstrict | STATS m = MIN(EventTime) | EVAL v = m::long | KEEP v", minSec * 1000L);

        // MODE 3 — STRICT: dynamic:false, EventTime {date, epoch_second} pinned as the schema.
        registerReal("cb_strict", realPath, DatasetMapping.Dynamic.FALSE);
        assertQ("cb_strict ==", "FROM cb_strict | WHERE EventTime == TO_DATETIME(\"" + maxIso + "\") | STATS c = COUNT(*)", 1L);
        assertQ(
            "cb_strict SORT DESC",
            "FROM cb_strict | SORT EventTime DESC | LIMIT 1 | EVAL v = EventTime::long | KEEP v",
            maxSec * 1000L
        );
        assertQ("cb_strict MAX", "FROM cb_strict | STATS m = MAX(EventTime) | EVAL v = m::long | KEEP v", maxSec * 1000L);
    }

    private void assertQ(String label, String query, Object expected) {
        assertQ(label, query, expected, TIMEOUT);
    }

    private void assertQ(String label, String query, Object expected, org.elasticsearch.core.TimeValue timeout) {
        try (var response = run(syncEsqlQueryRequest(query), timeout)) {
            List<List<Object>> rows = getValuesList(response);
            Object actual = rows.isEmpty() ? null : rows.get(0).get(0);
            assertThat(label + " :: " + query, actual, equalTo(expected));
        }
    }

    private void registerReal(String dataset, String realPath, @Nullable DatasetMapping.Dynamic mode) throws Exception {
        // Copy into the allowlisted temp dir the harness permits (esql.datasource.local_allowed_paths); the real
        // download lives outside it. One copy is shared across the three registrations via a per-test cache.
        if (realClickBenchLocal == null) {
            realClickBenchLocal = createTempDir().resolve("hits.parquet");
            Files.copy(org.elasticsearch.core.PathUtils.get(realPath), realClickBenchLocal);
        }
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        DatasetMapping mapping = null;
        if (mode != null) {
            props.put("EventTime", DatasetFieldMapping.withFormat("date", null, "epoch_second"));
            if (mode == DatasetMapping.Dynamic.FALSE) {
                // strict pins the declaration, so name the columns the queries touch.
                props.put("EventDate", new DatasetFieldMapping("integer", null));
            }
            mapping = new DatasetMapping(new DatasetMapping.Mappings(mode, props));
        }
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    dataset,
                    "local_ds",
                    realClickBenchLocal.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    mapping
                )
            )
        );
    }

    /**
     * Real-data validation against a genuine ClickBench NDJSON file, whose {@code EventTime} is a calendar STRING
     * ({@code "2013-07-28 15:03:05"}) — the text-carrier shape — declared {@code {date, format: "yyyy-MM-dd HH:mm:ss"}}.
     * Mirrors {@link #testRealClickBenchEventTimeAcrossAllThreeModes} across all three declaration modes with ground
     * truth computed independently ({@code jq}/python over the raw strings), so the declared-format decode, filter and
     * aggregate carriers are exercised end-to-end on genuine third-party bytes. Skipped unless
     * {@code -Dtests.clickbench.real.ndjson=<path>} points at a decompressed {@code hits_*.ndjson}; values pinned from
     * {@code ndjson/industry-standard/.../hits_109.ndjson} (442,467 rows).
     */
    private java.nio.file.Path realClickBenchNdjsonLocal;

    public void testRealClickBenchNdjsonEventTimeAcrossAllThreeModes() throws Exception {
        String realPath = System.getProperty("tests.clickbench.real.ndjson");
        assumeTrue("set -Dtests.clickbench.real.ndjson to a decompressed hits_*.ndjson", realPath != null);
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));

        String minStr = "2013-07-27 20:00:00";
        String maxStr = "2013-07-28 19:59:59";
        long minMillis = 1374955200000L; // 2013-07-27T20:00:00.000Z
        long maxMillis = 1375041599000L; // 2013-07-28T19:59:59.000Z
        String maxIso = "2013-07-28T19:59:59.000Z";
        long count = 442467L;
        // A full scan of the ~1 GB decompressed file per query needs longer than the shared 30s wait.
        org.elasticsearch.core.TimeValue slow = org.elasticsearch.core.TimeValue.timeValueMinutes(10);

        // MODE 1 — INFERRED: no declaration; EventTime infers as keyword. The pattern is lexically monotone, so the
        // string extrema coincide with the chronological ones.
        registerRealNdjson("cb_ndjson_inferred", realPath, null);
        assertQ("cb_ndjson_inferred MAX", "FROM cb_ndjson_inferred | STATS m = MAX(EventTime)", maxStr, slow);
        assertQ("cb_ndjson_inferred MIN", "FROM cb_ndjson_inferred | STATS m = MIN(EventTime)", minStr, slow);
        assertQ("cb_ndjson_inferred COUNT", "FROM cb_ndjson_inferred | STATS c = COUNT(*)", count, slow);

        // MODE 2 — NON-STRICT: dynamic:true, declare EventTime {date, format}; the string decodes to epoch millis.
        registerRealNdjson("cb_ndjson_nonstrict", realPath, DatasetMapping.Dynamic.TRUE);
        assertQ(
            "cb_ndjson_nonstrict ==",
            "FROM cb_ndjson_nonstrict | WHERE EventTime == TO_DATETIME(\"" + maxIso + "\") | STATS c = COUNT(*)",
            4L,
            slow
        );
        assertQ(
            "cb_ndjson_nonstrict SORT DESC",
            "FROM cb_ndjson_nonstrict | SORT EventTime DESC | LIMIT 1 | EVAL v = EventTime::long | KEEP v",
            maxMillis,
            slow
        );
        assertQ(
            "cb_ndjson_nonstrict MAX",
            "FROM cb_ndjson_nonstrict | STATS m = MAX(EventTime) | EVAL v = m::long | KEEP v",
            maxMillis,
            slow
        );
        assertQ(
            "cb_ndjson_nonstrict MIN",
            "FROM cb_ndjson_nonstrict | STATS m = MIN(EventTime) | EVAL v = m::long | KEEP v",
            minMillis,
            slow
        );

        // MODE 3 — STRICT: dynamic:false, EventTime {date, format} pinned as the schema.
        registerRealNdjson("cb_ndjson_strict", realPath, DatasetMapping.Dynamic.FALSE);
        assertQ(
            "cb_ndjson_strict ==",
            "FROM cb_ndjson_strict | WHERE EventTime == TO_DATETIME(\"" + maxIso + "\") | STATS c = COUNT(*)",
            4L,
            slow
        );
        assertQ("cb_ndjson_strict MAX", "FROM cb_ndjson_strict | STATS m = MAX(EventTime) | EVAL v = m::long | KEEP v", maxMillis, slow);
    }

    private void registerRealNdjson(String dataset, String realPath, @Nullable DatasetMapping.Dynamic mode) throws Exception {
        if (realClickBenchNdjsonLocal == null) {
            realClickBenchNdjsonLocal = createTempDir().resolve("hits.ndjson");
            Files.copy(org.elasticsearch.core.PathUtils.get(realPath), realClickBenchNdjsonLocal);
        }
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        DatasetMapping mapping = null;
        if (mode != null) {
            props.put("EventTime", DatasetFieldMapping.withFormat("date", null, "yyyy-MM-dd HH:mm:ss"));
            mapping = new DatasetMapping(new DatasetMapping.Mappings(mode, props));
        }
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    dataset,
                    "local_ds",
                    realClickBenchNdjsonLocal.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "ndjson")),
                    mapping
                )
            )
        );
    }

    /**
     * Epoch-scaling overflow on the COLUMNAR (parquet) path: an int64 declared {@code {date, format: epoch_second}}
     * whose value cannot scale to millis ({@code Long.MAX_VALUE} seconds × 1000) must fail PER CELL — never abort the
     * whole read on a bare {@code ArithmeticException}, never emit a wrong value. A columnar batch cannot drop a single
     * row, so {@code skip_row} degrades to the same null+warn as {@code null_field} (see {@code ErrorPolicy}); only
     * {@code fail_fast} aborts. This is the overflow leg of the error-mode matrix the string-token tests do not reach.
     */
    public void testParquetDeclaredEpochSecondOverflowHonorsErrorPolicy() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long good0 = 1704067200L, good2 = 1704067201L, overflow = Long.MAX_VALUE;
        Path parquet = writeScalingFixture("epoch_ovf", new long[] { good0, overflow, good2 });

        // null_field and skip_row: the bad cell nulls, both good rows survive (skip_row cannot drop a columnar row).
        for (String mode : List.of("null_field", "skip_row")) {
            String ds = mode.equals("null_field") ? "epoch_ovf_pq_null" : "epoch_ovf_pq_skip";
            putEpochOverflowDataset(ds, "parquet", parquet.toUri().toString(), mode, false);
            try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat("columnar " + mode + " keeps every position", rows, hasSize(3));
                assertThat(rows.get(0).get(0), equalTo(good0 * 1000L));
                assertThat("the overflowing epoch-second cell nulls under " + mode, rows.get(1).get(0), equalTo(null));
                assertThat(rows.get(2).get(0), equalTo(good2 * 1000L));
            }
        }
        assertLenientWarning("FROM epoch_ovf_pq_null | SORT pri | EVAL v = ts::long | KEEP v");

        // fail_fast: the read aborts with a sensible per-cell error, not a bare ArithmeticException.
        putEpochOverflowDataset("epoch_ovf_pq_fail", "parquet", parquet.toUri().toString(), "fail_fast", false);
        Exception failure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM epoch_ovf_pq_fail | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT).close()
        );
        assertFalse("fail_fast overflow must not surface as a bare ArithmeticException", failure instanceof ArithmeticException);
    }

    /**
     * Epoch-scaling overflow on the CSV (text) path: same malformed value, same declaration. Text readers ARE
     * row-oriented, so {@code skip_row} genuinely drops the bad record (two rows survive), while {@code null_field}
     * keeps it with a null cell — the distinction columnar readers cannot make. {@code fail_fast} aborts.
     */
    public void testCsvDeclaredEpochSecondOverflowHonorsErrorPolicy() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long good0 = 1704067200L, good2 = 1704067201L;
        Path csv = createTempFile("epoch-ovf-", ".csv");
        Files.writeString(csv, "pri:integer,ts:long\n1," + good0 + "\n2," + Long.MAX_VALUE + "\n3," + good2 + "\n");

        // null_field: bad cell nulls, all three rows survive.
        putEpochOverflowDataset("epoch_ovf_csv_null", "csv", csv.toUri().toString(), "null_field", true);
        try (var response = run(syncEsqlQueryRequest("FROM epoch_ovf_csv_null | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(good0 * 1000L));
            assertThat("the overflowing cell nulls under null_field", rows.get(1).get(0), equalTo(null));
            assertThat(rows.get(2).get(0), equalTo(good2 * 1000L));
        }
        assertLenientWarning("FROM epoch_ovf_csv_null | SORT pri | EVAL v = ts::long | KEEP v");

        // skip_row: the bad record is dropped whole — only the two good rows survive.
        putEpochOverflowDataset("epoch_ovf_csv_skip", "csv", csv.toUri().toString(), "skip_row", true);
        try (var response = run(syncEsqlQueryRequest("FROM epoch_ovf_csv_skip | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("text skip_row drops the malformed record", rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(good0 * 1000L));
            assertThat(rows.get(1).get(0), equalTo(good2 * 1000L));
        }

        // fail_fast: aborts.
        putEpochOverflowDataset("epoch_ovf_csv_fail", "csv", csv.toUri().toString(), "fail_fast", true);
        Exception failure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM epoch_ovf_csv_fail | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT).close()
        );
        assertFalse("fail_fast overflow must not surface as a bare ArithmeticException", failure instanceof ArithmeticException);
    }

    /**
     * Epoch-scaling overflow on the NDJSON (text) path: the row-oriented twin of the CSV test — {@code skip_row} drops
     * the malformed record, {@code null_field} nulls the cell, {@code fail_fast} aborts.
     */
    public void testNdjsonDeclaredEpochSecondOverflowHonorsErrorPolicy() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        long good0 = 1704067200L, good2 = 1704067201L;
        String content = "{\"pri\":1,\"ts\":" + good0 + "}\n{\"pri\":2,\"ts\":" + Long.MAX_VALUE + "}\n{\"pri\":3,\"ts\":" + good2 + "}\n";
        Path ndjson = createTempFile("epoch-ovf-", ".ndjson");
        Files.writeString(ndjson, content);

        // null_field: bad cell nulls, all three rows survive.
        putEpochOverflowDataset("epoch_ovf_nj_null", "ndjson", ndjson.toUri().toString(), "null_field", true);
        try (var response = run(syncEsqlQueryRequest("FROM epoch_ovf_nj_null | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(good0 * 1000L));
            assertThat("the overflowing cell nulls under null_field", rows.get(1).get(0), equalTo(null));
            assertThat(rows.get(2).get(0), equalTo(good2 * 1000L));
        }
        assertLenientWarning("FROM epoch_ovf_nj_null | SORT pri | EVAL v = ts::long | KEEP v");

        // skip_row: the bad record is dropped whole.
        putEpochOverflowDataset("epoch_ovf_nj_skip", "ndjson", ndjson.toUri().toString(), "skip_row", true);
        try (var response = run(syncEsqlQueryRequest("FROM epoch_ovf_nj_skip | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("text skip_row drops the malformed record", rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(good0 * 1000L));
            assertThat(rows.get(1).get(0), equalTo(good2 * 1000L));
        }

        // fail_fast: aborts.
        putEpochOverflowDataset("epoch_ovf_nj_fail", "ndjson", ndjson.toUri().toString(), "fail_fast", true);
        Exception failure = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM epoch_ovf_nj_fail | SORT pri | EVAL v = ts::long | KEEP v"), TIMEOUT).close()
        );
        assertFalse("fail_fast overflow must not surface as a bare ArithmeticException", failure instanceof ArithmeticException);
    }

    /**
     * Registers a {@code {ts: date, format: epoch_second}} dataset over {@code location} with the given {@code error_mode}.
     * {@code declarePri} names {@code pri} in the mapping too, for the text fixtures whose ordering column the query sorts on.
     */
    private void putEpochOverflowDataset(String name, String format, String location, String errorMode, boolean declarePri)
        throws Exception {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date", null, "epoch_second"));
        if (declarePri) {
            props.put("pri", new DatasetFieldMapping("integer", null));
        }
        HashMap<String, Object> settings = new HashMap<>(Map.of("format", format, "error_mode", errorMode));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    name,
                    "local_ds",
                    location,
                    null,
                    settings,
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props))
                )
            )
        );
    }

    /** Runs {@code query} and asserts at least one response {@code Warning} header surfaced (the lenient malformed-cell announcement). */
    private void assertLenientWarning(String query) throws Exception {
        List<String> warnings = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.running(() -> {
            try {
                internalCluster().getInstance(TransportService.class)
                    .getThreadPool()
                    .getThreadContext()
                    .getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .forEach(warnings::add);
            } finally {
                latch.countDown();
            }
        }));
        assertTrue("query did not complete within timeout", latch.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertThat("a lenient malformed-cell read must emit a response Warning header", warnings, not(empty()));
    }

    /**
     * A declared {@code format} names the epoch unit, so decode rescales the raw value — but the parquet footer
     * statistics stay in the file's RAW unit. A filter whose bound is pushed in the DECODED unit gets compared against
     * those raw stats and prunes the row group that genuinely matches, returning nothing.
     *
     * <p>Pruning is not RECHECK-recoverable: RECHECK re-applies exact semantics to rows that were READ, and a pruned
     * row group is never decoded at all. The sibling shape-recovery tests never filter, so they cannot see this.
     */
    public void testDeclaredEpochSecondFormatDoesNotPruneTheMatchingRowGroup() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // bare int64 holding 1704067200 (seconds); decode scales it to 1704067200000 millis.
        List<List<Object>> rows = runRealParquetFiltered(
            "rp_prune_s",
            "bare_seconds",
            DatasetFieldMapping.withFormat("date", null, "epoch_second"),
            "ts == TO_DATETIME(\"2024-01-01T00:00:00.000Z\")"
        );
        assertThat("the row matches the filter and must not be pruned by a raw-unit stats comparison", rows, hasSize(1));
    }

    /** The date_nanos half: decode scales seconds by 1e9 while the footer stats stay raw seconds. */
    public void testDeclaredEpochSecondFormatDoesNotPruneTheMatchingRowGroupForDateNanos() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        List<List<Object>> rows = runRealParquetFiltered(
            "rpn_prune_s",
            "bare_seconds",
            DatasetFieldMapping.withFormat("date_nanos", null, "epoch_second"),
            "ts == TO_DATE_NANOS(\"2024-01-01T00:00:00.000Z\")"
        );
        assertThat("the row matches the filter and must not be pruned by a raw-unit stats comparison", rows, hasSize(1));
    }

    /** Like {@link #runRealParquet} but applies a WHERE, so the filter reaches parquet pushdown and row-group pruning. */
    private List<List<Object>> runRealParquetFiltered(String dataset, String fixture, DatasetFieldMapping ts, String where)
        throws Exception {
        Path file = createTempDir().resolve(fixture + ".parquet");
        Files.write(file, RealParquetFixtures.bytes(fixture));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", ts);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    dataset,
                    "local_ds",
                    file.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties))
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | WHERE " + where + " | KEEP ts"), TIMEOUT)) {
            return getValuesList(response);
        }
    }

    private void assertRealParquet(String dataset, String fixture, DatasetFieldMapping ts, long expectedMillis) throws Exception {
        try (var response = runRealParquet(dataset, fixture, ts)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(fixture, rows, hasSize(greaterThanOrEqualTo(1)));
            assertThat("[" + fixture + "] must recover the instant", rows.get(0).get(0), equalTo(expectedMillis));
        }
    }

    private void assertRealParquetNanos(String dataset, String fixture, DatasetFieldMapping ts, long expectedNanos) throws Exception {
        try (var response = runRealParquet(dataset, fixture, ts)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(fixture, rows, hasSize(greaterThanOrEqualTo(1)));
            assertThat("[" + fixture + "] must recover the nanos instant", rows.get(0).get(0), equalTo(expectedNanos));
        }
    }

    private EsqlQueryResponse runRealParquet(String dataset, String fixture, DatasetFieldMapping ts) throws Exception {
        Path file = createTempDir().resolve(fixture + ".parquet");
        Files.write(file, RealParquetFixtures.bytes(fixture));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", ts);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    dataset,
                    "local_ds",
                    file.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "parquet")),
                    new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties))
                )
            )
        );
        return run(syncEsqlQueryRequest("FROM " + dataset + " | EVAL v = ts::long | KEEP v | SORT v | LIMIT 1"), TIMEOUT);
    }

    private byte[] parquetRenameFixtureBytes() throws IOException {
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
        return baos.toByteArray();
    }

    public void testStrictDeclaredSchemaOverMultiFileGlob() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");
        Files.writeString(root.resolve("part2.csv"), "emp_no:integer,first_name:keyword\n3,Carol\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Strict declaration RENAMES emp_no/first_name to id/name via `path`, per file, by name (each file carries the
        // same header). This is the strict multi-file rail: the declared schema is pinned once and bound to every
        // file's own header without a per-file schema inference read.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
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
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
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

    /**
     * The declared schema must win uniformly across a multi-file glob whose files DISAGREE on a column's physical
     * type — every existing glob test uses identical files, so the reconcile-under-declaration path never runs here.
     * part1 stores {@code val} as integer, part2 as long with a value that overflows int; declared {@code val: long}
     * reads both as long, and part2's large value survives (an inferred integer reconcile would have clashed/nulled it).
     */
    public void testDeclaredSchemaOverDivergentTypeMultiFileGlob() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("part1.csv"), "val:integer\n100\n");
        Files.writeString(root.resolve("part2.csv"), "val:long\n5000000000\n"); // 5e9 > Integer.MAX_VALUE

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_divergent_multi",
                    "local_ds",
                    root.toUri() + "*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM employees_divergent_multi | SORT val | LIMIT 10"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(1));
            assertThat(columns.get(0).name(), equalTo("val"));
            assertThat(columns.get(0).outputType(), equalTo("long"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo(100L));          // integer-file value read as long
            assertThat(rows.get(1).get(0), equalTo(5000000000L));   // long-file value that would overflow an int
        }
    }

    /** TSV (shares the CSV reader via the tsv preset): a declared type coerces the file value like CSV/Parquet. */
    public void testTsvDeclaredTypeCoercionReadsAsDeclared() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("data.tsv"), "val:integer\tname:keyword\n100\tAlice\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", null)); // retype integer file column to long
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "tsv_declared_type",
                    "local_ds",
                    root.toUri() + "*.tsv",
                    null,
                    new HashMap<>(Map.of("format", "tsv")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM tsv_declared_type | KEEP val | LIMIT 1"), TIMEOUT)) {
            assertThat(response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response).get(0).get(0), equalTo(100L));
        }
    }

    /** TSV declared per-column date {@code format}: a string field declared date parses zone-aware, like the CSV path. */
    public void testTsvDeclaredDateFormatParsesZoneAware() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("logs.tsv"), "ts:keyword\tnote:keyword\n10/Oct/2000:13:55:36 -0700\thello\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("ts", DatasetFieldMapping.withFormat("date", null, ACCESS_LOG_FORMAT));
        properties.put("note", new DatasetFieldMapping("keyword", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "tsv_declared_date",
                    "local_ds",
                    root.toUri() + "*.tsv",
                    null,
                    new HashMap<>(Map.of("format", "tsv")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM tsv_declared_date | EVAL ms = ts::long | KEEP ms | LIMIT 1"), TIMEOUT)) {
            assertThat(getValuesList(response).get(0).get(0), equalTo(ACCESS_LOG_EPOCH_MILLIS)); // -0700 offset honored
        }
    }

    /** TSV declared rename (`path`): a logical column reads from a differently-named physical TSV column. */
    public void testTsvDeclaredRenameReadsByPhysicalColumn() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("e.tsv"), "emp_no:integer\tfirst_name:keyword\n1\tAlice\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", "emp_no"));      // logical id <- physical emp_no
        properties.put("name", new DatasetFieldMapping("keyword", "first_name"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "tsv_declared_rename",
                    "local_ds",
                    root.toUri() + "*.tsv",
                    null,
                    new HashMap<>(Map.of("format", "tsv")),
                    mapping
                )
            )
        );
        try (var response = run(syncEsqlQueryRequest("FROM tsv_declared_rename | KEEP id, name | LIMIT 1"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.get(0).name(), equalTo("id"));
            assertThat(columns.get(1).name(), equalTo("name"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(0).get(1), equalTo("Alice"));
        }
    }

    /**
     * A multivalue (array) column declared a coerced type coerces element-by-element — every element of the
     * position is converted, not just the first, and the position keeps its multivalue shape.
     */
    public void testNdjsonMultiValueColumnCoercesElementwise() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("mv.ndjson"), "{\"vals\":[\"10\",\"20\",\"30\"]}\n{\"vals\":[\"40\"]}\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("vals", new DatasetFieldMapping("long", null)); // declare the string array as long
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "ndjson_mv_coerce",
                    "local_ds",
                    root.toUri() + "*.ndjson",
                    null,
                    new HashMap<>(Map.of("format", "ndjson")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM ndjson_mv_coerce | KEEP vals | LIMIT 10"), TIMEOUT)) {
            assertThat(response.columns().get(0).outputType(), equalTo("long"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            // The 3-element position keeps all three elements, each coerced to long.
            assertThat(rows.get(0).get(0), equalTo(List.of(10L, 20L, 30L)));
            assertThat(rows.get(1).get(0), equalTo(40L)); // single-element position renders as a scalar
        }
    }

    /**
     * A view whose body targets a dataset carrying a DECLARED mapping: the declared coercion must survive view
     * inlining (view resolution rewrites the inlined leaf into the external relation, which must keep the mapping —
     * a dropped mapping here would silently read the file's physical type instead of the declared one).
     */
    public void testViewOverMappedDatasetPreservesCoercion() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("d.csv"), "val:integer\n100\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", null)); // declare integer file column as long
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "mapped_ds_for_view",
                    "local_ds",
                    root.toUri() + "*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );
        assertAcked(client().execute(PutViewAction.INSTANCE, putViewRequest("mapped_dataset_view", "FROM mapped_ds_for_view")));

        try (var response = run(syncEsqlQueryRequest("FROM mapped_dataset_view | KEEP val | LIMIT 1"), TIMEOUT)) {
            assertThat("declared type must survive view inlining", response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response).get(0).get(0), equalTo(100L));
        }
    }

    /**
     * A dataset carrying a DECLARED mapping used inside a subquery ({@code FROM (FROM mapped)}): the declared
     * coercion must survive the subquery planning rewrite, not just a top-level {@code FROM}.
     */
    public void testSubqueryOverMappedDatasetPreservesCoercion() throws Exception {
        Path root = createTempDir();
        Files.writeString(root.resolve("d.csv"), "val:integer\n100\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("val", new DatasetFieldMapping("long", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "mapped_ds_for_subquery",
                    "local_ds",
                    root.toUri() + "*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM (FROM mapped_ds_for_subquery) | KEEP val | LIMIT 1"), TIMEOUT)) {
            assertThat("declared type must survive the subquery rewrite", response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response).get(0).get(0), equalTo(100L));
        }
    }

    public void testViewOverExternalDatasetIsQueryable() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees_external", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees_external", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
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

        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

        registerDataSource("local_ds", Map.of());
        registerDataset("employees_mixed", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("TS employees | LIMIT 1"), TIMEOUT));
        assertCauseMessageContains(ex, "TS command is not supported for datasets");
    }

    public void testLookupJoinRejectedAgainstDataset() throws Exception {
        // Create some_real_index locally so the test exercises the LOOKUP JOIN rejection rather
        // than failing earlier with "unknown index" under order-dependent runs.
        createIndex("some_real_index");
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        Exception ex = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM some_real_index | LOOKUP JOIN employees ON emp_no | LIMIT 1"), TIMEOUT)
        );
        assertCauseMessageContains(ex, "LOOKUP JOIN against a dataset is not supported");
    }

    public void testFromDatasetWithWhere() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM employees | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testFromDatasetWithEval() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM employees, employees_alt | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            // 3 rows from employees + 2 from employees_alt
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testFromDatasetWildcardExpansion() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));

        // employees + employees_alt = 3 + 2 = 5
        try (var response = run(syncEsqlQueryRequest("FROM employees* | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    public void testFromDatasetWildcardWithExclusion() throws Exception {
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));
        registerDataset("employees_alt", "local_ds", csvFixtureAlt.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

        registerDataSource("local_ds", Map.of());
        registerDataset("events_hive", "local_ds", root.toUri() + "**/*.csv", Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

        registerDataSource("local_ds", Map.of());
        registerDataset("logs_dataset", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of("secret_access_key", sentinel, "region", "us-east-1"));
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

        registerDataSource("local_ds", Map.of());
        registerDataset("stats_ds", "local_ds", salaryFixture.toUri().toString(), Map.of("format", "csv"));

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

        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
     * COUNT_DISTINCT across a heterogeneous FROM: exercises the intermediate-state pushdown
     * ({@code ToPartial}/{@code FromPartial}) for the HLL sketch. The overlapping value (emp_no=3 in both
     * sources) proves the branches' sketches are <b>unioned</b>, not summed.
     *
     * <p>ES {@code cd_idx}: emp_no 1,2,3. Dataset {@code heavy_ds}: emp_no 3,4,5.
     * Distinct union = {1,2,3,4,5} = 5 (a naive sum of per-branch distincts would give 6).
     */
    public void testFromMixedCountDistinct() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("cd_idx").setMapping("emp_no", "type=integer"));
        prepareIndex("cd_idx").setSource(Map.of("emp_no", 1)).get();
        prepareIndex("cd_idx").setSource(Map.of("emp_no", 2)).get();
        prepareIndex("cd_idx").setSource(Map.of("emp_no", 3)).get();
        client().admin().indices().prepareRefresh("cd_idx").get();

        Path fixture = createTempFile("cd-fixture-", ".csv");
        Files.writeString(fixture, "emp_no:integer\n3\n4\n5\n");
        putHeavyDataset(fixture);

        try (var response = run(syncEsqlQueryRequest("FROM cd_idx, heavy_ds | STATS d = COUNT_DISTINCT(emp_no)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int dIdx = columnIndex(response, "d");
            assertThat(((Number) rows.get(0).get(dIdx)).longValue(), equalTo(5L));
        }
    }

    /**
     * Grouped COUNT_DISTINCT across a heterogeneous FROM: the grouped happy path for the intermediate-state
     * pushdown: each branch builds a per-group HLL sketch ({@code ToPartial} BY dept) and the coordinator
     * merges per group ({@code FromPartial}).
     *
     * <p>ES {@code cdgrp_idx}: (dept=1,emp_no=1),(dept=1,emp_no=2),(dept=2,emp_no=3).
     * Dataset: (dept=1,emp_no=2),(dept=2,emp_no=4). Distinct emp_no per dept: dept=1 {1,2}=2; dept=2 {3,4}=2.
     */
    public void testFromMixedCountDistinctGrouped() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("cdgrp_idx").setMapping("dept", "type=integer", "emp_no", "type=integer"));
        prepareIndex("cdgrp_idx").setSource(Map.of("dept", 1, "emp_no", 1)).get();
        prepareIndex("cdgrp_idx").setSource(Map.of("dept", 1, "emp_no", 2)).get();
        prepareIndex("cdgrp_idx").setSource(Map.of("dept", 2, "emp_no", 3)).get();
        client().admin().indices().prepareRefresh("cdgrp_idx").get();

        Path fixture = createTempFile("cdgrp-fixture-", ".csv");
        Files.writeString(fixture, "dept:integer,emp_no:integer\n1,2\n2,4\n");
        putHeavyDataset(fixture);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM cdgrp_idx, heavy_ds | STATS d = COUNT_DISTINCT(emp_no) BY dept | SORT dept"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            int dIdx = columnIndex(response, "d");
            int deptIdx = columnIndex(response, "dept");
            assertThat(((Number) rows.get(0).get(deptIdx)).intValue(), equalTo(1));
            assertThat(((Number) rows.get(0).get(dIdx)).longValue(), equalTo(2L));
            assertThat(((Number) rows.get(1).get(deptIdx)).intValue(), equalTo(2));
            assertThat(((Number) rows.get(1).get(dIdx)).longValue(), equalTo(2L));
        }
    }

    /**
     * Grouped + filtered COUNT_DISTINCT across a heterogeneous FROM. The per-aggregate filter is pushed down into
     * each branch on the {@code ToPartial} node, and the physical layer wraps the inner aggregator so each branch
     * folds only the matching rows into its per-group intermediate state. This shape previously <b>hung the query</b>
     * (the grouping path drives {@code ToPartial} through the mode-aware factory, which the plain filter wrapper does
     * not implement); this test is the end-to-end regression guard that it now completes and is correct.
     *
     * <p>ES {@code cdg_idx}: (dept=1,emp_no=1,salary=10),(dept=1,emp_no=2,salary=20),(dept=2,emp_no=9,salary=5).
     * Dataset: (dept=1,emp_no=2,salary=30),(dept=2,emp_no=8,salary=40).
     * With salary>15: dept=1 distinct emp_no {2} = 1; dept=2 distinct emp_no {8} = 1.
     */
    public void testFromMixedCountDistinctGroupedWithFilter() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("cdg_idx")
                .setMapping("dept", "type=integer", "emp_no", "type=integer", "salary", "type=integer")
        );
        prepareIndex("cdg_idx").setSource(Map.of("dept", 1, "emp_no", 1, "salary", 10)).get();
        prepareIndex("cdg_idx").setSource(Map.of("dept", 1, "emp_no", 2, "salary", 20)).get();
        prepareIndex("cdg_idx").setSource(Map.of("dept", 2, "emp_no", 9, "salary", 5)).get();
        client().admin().indices().prepareRefresh("cdg_idx").get();

        Path fixture = createTempFile("cdg-fixture-", ".csv");
        Files.writeString(fixture, "dept:integer,emp_no:integer,salary:integer\n1,2,30\n2,8,40\n");
        putHeavyDataset(fixture);

        try (
            var response = run(
                syncEsqlQueryRequest("FROM cdg_idx, heavy_ds | STATS d = COUNT_DISTINCT(emp_no) WHERE salary > 15 BY dept | SORT dept"),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            int dIdx = columnIndex(response, "d");
            int deptIdx = columnIndex(response, "dept");
            assertThat(((Number) rows.get(0).get(deptIdx)).intValue(), equalTo(1));
            assertThat(((Number) rows.get(0).get(dIdx)).longValue(), equalTo(1L));
            assertThat(((Number) rows.get(1).get(deptIdx)).intValue(), equalTo(2));
            assertThat(((Number) rows.get(1).get(dIdx)).longValue(), equalTo(1L));
        }
    }

    /**
     * AVG grouped across a heterogeneous FROM: AVG is rewritten to SUM/COUNT before the pushdown rule, so it
     * decomposes through the algebraic path. Verifies the end-to-end result is correct.
     *
     * <p>ES {@code avg_idx}: (dept=1,salary=100),(dept=2,salary=300). Dataset: (dept=1,salary=200),(dept=3,salary=400).
     * AVG(salary) BY dept = {1 → 150}, {2 → 300}, {3 → 400}.
     */
    public void testFromMixedAvgGrouped() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("avg_idx").setMapping("dept", "type=integer", "salary", "type=integer"));
        prepareIndex("avg_idx").setSource(Map.of("dept", 1, "salary", 100)).get();
        prepareIndex("avg_idx").setSource(Map.of("dept", 2, "salary", 300)).get();
        client().admin().indices().prepareRefresh("avg_idx").get();

        Path fixture = createTempFile("avg-fixture-", ".csv");
        Files.writeString(fixture, "dept:integer,salary:integer\n1,200\n3,400\n");
        putHeavyDataset(fixture);

        try (var response = run(syncEsqlQueryRequest("FROM avg_idx, heavy_ds | STATS a = AVG(salary) BY dept | SORT dept"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            int aIdx = columnIndex(response, "a");
            int deptIdx = columnIndex(response, "dept");
            assertThat(((Number) rows.get(0).get(deptIdx)).intValue(), equalTo(1));
            assertThat(((Number) rows.get(0).get(aIdx)).doubleValue(), closeTo(150.0, 1e-9));
            assertThat(((Number) rows.get(1).get(deptIdx)).intValue(), equalTo(2));
            assertThat(((Number) rows.get(1).get(aIdx)).doubleValue(), closeTo(300.0, 1e-9));
            assertThat(((Number) rows.get(2).get(deptIdx)).intValue(), equalTo(3));
            assertThat(((Number) rows.get(2).get(aIdx)).doubleValue(), closeTo(400.0, 1e-9));
        }
    }

    /** Registers the {@code local_ds} data source and a {@code heavy_ds} CSV dataset pointing at {@code fixture}. */
    private void putHeavyDataset(Path fixture) {
        registerDataSource("local_ds", Map.of());
        registerDataset("heavy_ds", "local_ds", fixture.toUri().toString(), Map.of("format", "csv"));
    }

    private static int columnIndex(EsqlQueryResponse response, String name) {
        return response.columns().stream().map(ColumnInfo::name).toList().indexOf(name);
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

        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, new LinkedHashMap<>(), "first_name")
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

    public void testIdFromRenamedColumn() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // The id-source is a LOGICAL name: declare uid as a rename of the physical first_name column and point
        // _id.path at the logical uid. The whole chain (pin, projection, reader translation, stamp) must stay in
        // logical space — _id equals the renamed column's values.
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("uid", new DatasetFieldMapping("keyword", "first_name"));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties, "uid"));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_id_renamed",
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
                syncEsqlQueryRequest("FROM employees_id_renamed METADATA _id | KEEP _id, uid | SORT uid | LIMIT 10"),
                TIMEOUT
            )
        ) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            int idIdx = names.indexOf("_id");
            int uidIdx = names.indexOf("uid");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_id is stamped from the renamed column", row.get(idIdx), equalTo(row.get(uidIdx)));
            }
            assertThat(rows.get(0).get(uidIdx).toString(), equalTo("Alice"));
        }
    }

    public void testIdPathOnPartitionColumnRejected() throws Exception {
        // _id.path naming a partition column is rejected loudly: a partition value is a path-derived constant surfaced
        // as a plain data-looking column, but the reader classifies it in the partition branch and never stamps _id from
        // it — pointing _id there would silently yield null ids for every row.
        Path root = createTempDir();
        Path east = Files.createDirectories(root.resolve("region=east"));
        Files.writeString(east.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("emp_no", new DatasetFieldMapping("integer", null));
        props.put("first_name", new DatasetFieldMapping("keyword", null));
        // Non-strict, _id.path = region (the partition key). PUT accepts (non-strict defers the id-column existence
        // check); the reject fires at query time when _id is actually requested.
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props, "region"));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_id_partition",
                    "local_ds",
                    root.toUri() + "**/*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv", "hive_partitioning", true)),
                    mapping
                )
            )
        );

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_id_partition METADATA _id | KEEP _id | LIMIT 1"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("[_id]"));
        assertThat(e.getMessage(), containsString("region"));
        // Pin the partition branch specifically, not the sibling "no such column exists" reject (both embed [_id]+path).
        assertThat(e.getMessage(), containsString("not a data column"));
    }

    public void testNonStrictPartitionKeyCollisionRejected() throws Exception {
        // Non-strict: a declared column whose name collides with a partition key is rejected, same as strict — otherwise
        // it would overlay/retype the path-derived partition attribute and misbind (block-type mismatch) at read time.
        Path root = createTempDir();
        Path east = Files.createDirectories(root.resolve("region=east"));
        Files.writeString(east.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("region", new DatasetFieldMapping("integer", null)); // collides with the partition key "region"
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_partition_collide_nonstrict",
                    "local_ds",
                    root.toUri() + "**/*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv", "hive_partitioning", true)),
                    mapping
                )
            )
        );

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_partition_collide_nonstrict | LIMIT 1"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("collides with a partition column"));
        assertThat(e.getMessage(), containsString("region"));

        // Same reject via the physical name: a declared column whose `path` points at the partition key
        // collides just as a name-match does — the read would map the physical to a shadowed path-derived column.
        Map<String, DatasetFieldMapping> pathProps = new LinkedHashMap<>();
        pathProps.put("region_alias", new DatasetFieldMapping("integer", "region")); // path physical collides with "region"
        DatasetMapping pathMapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, pathProps));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "logs_partition_collide_path",
                    "local_ds",
                    root.toUri() + "**/*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv", "hive_partitioning", true)),
                    pathMapping
                )
            )
        );
        Exception pe = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM logs_partition_collide_path | LIMIT 1"), TIMEOUT).close()
        );
        assertThat(pe.getMessage(), containsString("collides with a partition column"));
        assertThat(pe.getMessage(), containsString("region_alias"));
    }

    public void testStrictHivePartitionedGlob() throws Exception {
        // Strict mode over a hive-partitioned layout: partition columns are path-derived (no file I/O), so the
        // declaration-is-the-schema contract still surfaces them — typed and filterable — exactly like inference does.
        Path root = createTempDir();
        Path east = Files.createDirectories(root.resolve("region=east"));
        Files.writeString(east.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n1,Alice\n2,Bob\n");
        Path west = Files.createDirectories(root.resolve("region=west"));
        Files.writeString(west.resolve("part1.csv"), "emp_no:integer,first_name:keyword\n3,Carol\n");

        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        Map<String, DatasetFieldMapping> strictProps = new LinkedHashMap<>();
        strictProps.put("emp_no", new DatasetFieldMapping("integer", null));
        strictProps.put("first_name", new DatasetFieldMapping("keyword", null));
        DatasetMapping strictMapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, strictProps));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_hive",
                    "local_ds",
                    root.toUri() + "**/*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv", "hive_partitioning", true)),
                    strictMapping
                )
            )
        );

        // The partition column is queryable and filterable alongside the declared columns.
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM employees_strict_hive | WHERE region == \"east\" | SORT emp_no | KEEP emp_no, first_name, region"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(1).toString(), equalTo("Alice"));
            assertThat(rows.get(1).get(1).toString(), equalTo("Bob"));
            assertThat(rows.get(0).get(2).toString(), equalTo("east"));
        }

        // A declared column colliding with a partition key is rejected loudly (never silently shadowed — under
        // strict, shadowing would re-bind the positional text columns).
        Map<String, DatasetFieldMapping> colliding = new LinkedHashMap<>();
        colliding.put("emp_no", new DatasetFieldMapping("integer", null));
        colliding.put("region", new DatasetFieldMapping("keyword", null));
        DatasetMapping collidingMapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, colliding));
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_strict_hive_collide",
                    "local_ds",
                    root.toUri() + "**/*.csv",
                    null,
                    new HashMap<>(Map.of("format", "csv", "hive_partitioning", true)),
                    collidingMapping
                )
            )
        );
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees_strict_hive_collide | LIMIT 1"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("collides with a partition column"));
    }

    public void testIdFromMissingColumnRejectedOnlyWhenIdRequested() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        // Non-strict declaration whose _id.path names a column that exists in NO file — a typo, or the files lost it.
        // PUT accepts it (non-strict defers the existence check to query time; the files may not exist yet at PUT).
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, new LinkedHashMap<>(), "no_such_column")
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees_id_bad_path",
                    "local_ds",
                    csvFixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            )
        );

        // Asking for _id fails loudly at analysis — never a silently-null id column.
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM employees_id_bad_path METADATA _id | KEEP _id, emp_no | LIMIT 5"), TIMEOUT).close()
        );
        assertThat(e.getMessage(), containsString("no_such_column"));
        assertThat(e.getMessage(), containsString("_id"));

        // Not asking for _id is moot — the bad _id.path is like any other unread column; the query works.
        try (var response = run(syncEsqlQueryRequest("FROM employees_id_bad_path | SORT emp_no | LIMIT 5"), TIMEOUT)) {
            assertThat(getValuesList(response), hasSize(3));
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

        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", csvFixture.toUri().toString(), Map.of("format", "csv"));

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

    private static PutViewAction.Request putViewRequest(String name, String query) {
        return new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(name, query));
    }

    private static DeleteViewAction.Request deleteViewRequest(String name) {
        return new DeleteViewAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
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

    /**
     * A declared {@code unsigned_long} was accepted by PUT dataset and then failed every
     * {@code FROM} over it on the text formats — the readers re-derived the type→block-shape mapping locally and
     * omitted the type, so they threw at page setup, before {@code error_mode} could apply. End-to-end, the full
     * {@code [0, 2^64-1]} domain must now come back at full magnitude, identically from CSV and NDJSON, and equal
     * to what an explicit {@code ::unsigned_long} cast of the same token produces.
     */
    private void assertDeclaredUnsignedLongReadsFullMagnitude(String datasetName, Path fixture, String format) throws Exception {
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("long", null));
        properties.put("v", new DatasetFieldMapping("unsigned_long", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, properties));

        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    "local_ds",
                    fixture.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", format)),
                    mapping
                )
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM " + datasetName + " | SORT id | LIMIT 10"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            // Column type must survive as unsigned_long, not silently widen.
            assertThat(response.columns().get(1).type().typeName(), equalTo("unsigned_long"));
            assertThat(rows.get(0).get(1).toString(), equalTo("0"));
            assertThat(rows.get(1).get(1).toString(), equalTo("9223372036854775808"));   // 2^63
            assertThat(rows.get(2).get(1).toString(), equalTo("18446744073709551615"));  // 2^64-1
        }

        // Cast parity: the declared read agrees with an explicit ::unsigned_long over the same magnitudes.
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM " + datasetName + " | SORT id | EVAL same = (v == \"18446744073709551615\"::unsigned_long) | LIMIT 10"
                ),
                TIMEOUT
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.get(2).get(rows.get(2).size() - 1), equalTo(true));
            assertThat(rows.get(0).get(rows.get(0).size() - 1), equalTo(false));
        }
    }

    public void testDeclaredUnsignedLongReadsFromCsv() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertDeclaredUnsignedLongReadsFullMagnitude("ul_csv", csvUnsignedLongFixture, "csv");
    }

    public void testDeclaredUnsignedLongReadsFromNdjson() throws Exception {
        assertAcked(client().execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("local_ds", Map.of())));
        assertDeclaredUnsignedLongReadsFullMagnitude("ul_ndjson", ndjsonUnsignedLongFixture, "ndjson");
    }
}
