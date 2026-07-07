/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.closeTo;
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
public class FromDatasetIT extends AbstractExternalDataSourceIT {

    private Path csvFixture;
    private Path csvFixtureAlt;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
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
    }

    /**
     * Names every {@code testXxx} body creates via {@link PutViewAction}. Datasets and the {@code local_ds}
     * data source are registered through the base {@link AbstractExternalDataSourceIT#registerDataset}/
     * {@link AbstractExternalDataSourceIT#registerDataSource} helpers, so the base {@code cleanupRegistry()}
     * tears them down; only views need bespoke teardown here, one at a time so a missing view doesn't skip
     * deleting the others still present.
     */
    private static final Set<String> CREATED_VIEWS = Set.of("employees_view", "employees_filtered_view");

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
}
