/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasources.ExternalRowIdentity;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Per-format matrix for the standard metadata columns surfaced on {@code FROM <external-dataset>}.
 *
 * <p>The wiring that surfaces {@code _id} / {@code _source} / {@code _version} / {@code _index}
 * (and the always-null set {@code _score / _ignored / _index_mode / _tsid / _size}) reaches into
 * the per-format <em>reader</em>: every format reader emits a per-record token on the
 * {@code _rowPosition} channel (columnar formats — Parquet/ORC — a file-global row index; text
 * formats — CSV/NDJSON — a file-global byte offset), and
 * {@code VirtualColumnIterator} composes {@code _id} as the opaque (location, mtime, token) hash via
 * {@code ExternalRowIdentity}. {@code _source} is composed from the reader's data blocks by
 * {@code SynthesizeExternalSource}, rendering each value per its declared type the way the
 * response layer would.
 * A format-specific reader regression in any of those paths would otherwise pass with only the
 * CSV coverage in {@link FromDatasetIT}.
 *
 * <p>This base owns the {@code @Test} bodies; each concrete subclass binds them to one format
 * by supplying {@link #format()}, {@link #formatPlugins()} and a {@link #writeFixture(Path)} that
 * lays down the same canonical 3-row fixture (in file order {@code emp_no} 1,2,3) so the assertions
 * hold uniformly across formats.
 *
 * <p>Single-node by design, matching {@link FromDatasetIT}: this exercises the per-format reader/metadata surface,
 * not cluster-state propagation across nodes (covered by
 * {@code ProjectMetadataTests#testDatasetChangeViaDiffRebuildsIndicesLookup}).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractExternalMetadataMatrixIT extends AbstractExternalDataSourceIT {

    /** The format name passed as the dataset's {@code format} setting (e.g. {@code "csv"}). */
    protected abstract String format();

    /**
     * Write the canonical 3-row fixture into {@code dir} and return the resource URI string.
     * Row order in the file must be {@code emp_no} 1,2,3 so that, under {@code SORT emp_no}, the
     * file-local offsets {@code 0,1,2} line up with the sorted rows. The fixture carries three
     * columns: {@code emp_no} (int) 1,2,3; {@code first_name} (keyword) Alice,Bob,Carol; and
     * {@code host_ip} (keyword) "10.0.0.1","10.0.0.2","10.0.0.3" — the latter a non-{@code emp_no}
     * keyword column so {@code _source} synthesis exercises a typed KEYWORD-family render.
     */
    protected abstract String writeFixture(Path dir) throws Exception;

    private String fixtureUri;

    /** Determinism over planner-regression diversity here — these tests pin specific plan shapes. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        fixtureUri = writeFixture(createTempDir());
        registerDataSource("local_ds", Map.of());
        registerDataset("employees", "local_ds", fixtureUri, Map.of("format", format()));
    }

    /**
     * Index of metadata column {@code name} in the response columns, asserting it surfaced. On the
     * FROM path METADATA surfaces a column with no KEEP, so the data columns are present too and the
     * metadata column's position is not fixed — look it up by name.
     */
    private static int columnIndex(List<? extends ColumnInfo> columns, String name) {
        List<String> names = columns.stream().map(ColumnInfo::name).toList();
        int idx = names.indexOf(name);
        assertThat(name + " must surface without KEEP; got columns " + names, idx, greaterThanOrEqualTo(0));
        return idx;
    }

    public void testIdRendersOpaqueStableRowIdentity() throws Exception {
        // _id is base64url(hash(location) | mtime | recordToken) — opaque by design: the storage
        // path is hashed, never rendered, so dataset-layout details (bucket names, directory
        // structure) cannot leak through _id into Kibana row keys, alert hashes, or logs. The
        // record token is format-defined and intentionally NOT uniform across readers (columnar
        // formats emit a file-global row index, text formats a byte anchor), so we assert the
        // contract — fixed length, base64url charset, no path leak, per-row distinctness, and
        // determinism across two runs of the same query — not specific values.
        List<String> ids = collectIds();
        assertThat(ids, hasSize(3));
        for (String id : ids) {
            assertTrue(
                "rendered _id [" + id + "] must be fixed-length base64url",
                id.matches("[A-Za-z0-9_-]{" + ExternalRowIdentity.RENDERED_LENGTH + "}")
            );
            assertThat("storage location must not leak into _id [" + id + "]", id, not(containsString("employees")));
        }
        assertThat("all _id values are distinct", new java.util.HashSet<>(ids), hasSize(3));

        // Stability: the same physical rows compose the same ids on a second run.
        assertThat("_id must be deterministic across runs", collectIds(), equalTo(ids));
    }

    private List<String> collectIds() throws Exception {
        // METADATA surfaces _id with no KEEP.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _id | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_id");
            List<String> ids = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                ids.add(row.get(idx).toString());
            }
            return ids;
        }
    }

    public void testIndexIsDatasetName() throws Exception {
        // METADATA surfaces _index with no KEEP; it resolves to the dataset name.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_index");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(idx).toString(), equalTo("employees"));
            }
        }
    }

    public void testSourceRoundTripsRowColumnsWithDataKept() throws Exception {
        // Explicit-KEEP path: when the user lists the data columns in KEEP alongside _source, the
        // synthesizer renders every column. Explicit-projection baseline; the narrowed-projection
        // case is covered by testSourceSurvivesKeepNarrowingDataColumns.
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM employees METADATA _source | SORT emp_no | KEEP emp_no, first_name, host_ip, _source | LIMIT 10"
                ),
                TIMEOUT
            )
        ) {
            int sourceIdx = response.columns().stream().map(ColumnInfo::name).toList().indexOf("_source");
            assertThat("_source column present", sourceIdx, greaterThan(-1));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));

            // Expected per-row keyword values, in sorted emp_no order.
            String[] expectedFirstName = { "Alice", "Bob", "Carol" };
            String[] expectedHostIp = { "10.0.0.1", "10.0.0.2", "10.0.0.3" };

            for (int i = 0; i < rows.size(); i++) {
                Object source = rows.get(i).get(sourceIdx);
                assertThat("_source must be present for row " + i, source, notNullValue());
                // getValuesList already deserializes a SOURCE column into a Map (ResponseValueUtils).
                Map<?, ?> parsed = asMap(source);
                // The two keyword columns must round-trip through reader -> _source synthesis as their
                // exact string values. A format whose reader hands SynthesizeExternalSource a non-text
                // block for a keyword column (or mangles the bytes) shows up right here.
                assertThat("first_name in _source for row " + i, objToString(parsed.get("first_name")), equalTo(expectedFirstName[i]));
                assertThat("host_ip in _source for row " + i, objToString(parsed.get("host_ip")), equalTo(expectedHostIp[i]));
            }
        }
    }

    /**
     * Narrowed-KEEP path: when the user projects only {@code _source} (no data columns in KEEP),
     * the synthesizer must still render the full row. Indexed {@code _source} reads from the
     * stored doc and is independent of projection; external {@code _source} has no stored doc, so
     * the optimizer must pin the file-resident data columns when {@code _source} is in the output.
     * Regression guard for the data-column pruning bug.
     */
    public void testSourceSurvivesKeepNarrowingDataColumns() throws Exception {
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _source | SORT emp_no | KEEP _source | LIMIT 10"), TIMEOUT)) {
            List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat("only _source in projected columns", names, equalTo(List.of("_source")));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));

            String[] expectedFirstName = { "Alice", "Bob", "Carol" };
            String[] expectedHostIp = { "10.0.0.1", "10.0.0.2", "10.0.0.3" };
            Integer[] expectedEmpNo = { 1, 2, 3 };

            for (int i = 0; i < rows.size(); i++) {
                Map<?, ?> parsed = asMap(rows.get(i).get(0));
                assertThat(
                    "emp_no in _source even though KEEP narrowed it away (row " + i + ")",
                    ((Number) parsed.get("emp_no")).intValue(),
                    equalTo(expectedEmpNo[i])
                );
                assertThat(
                    "first_name in _source even though KEEP narrowed it away (row " + i + ")",
                    objToString(parsed.get("first_name")),
                    equalTo(expectedFirstName[i])
                );
                assertThat(
                    "host_ip in _source even though KEEP narrowed it away (row " + i + ")",
                    objToString(parsed.get("host_ip")),
                    equalTo(expectedHostIp[i])
                );
            }
        }
    }

    public void testVersionIsFileMtimeMillis() throws Exception {
        // _version is wired to the file's last-modified-time in milliseconds (see the
        // _file.modified plumbing through FileMetadataColumns + the StoragePath stat). Read the
        // mtime back from the fixture path and pin the response value, so a regression in the
        // mtime → _version threading is caught.
        long fixtureMtimeMillis = Files.getLastModifiedTime(PathUtils.get(URI.create(fixtureUri))).toMillis();

        // METADATA surfaces _version with no KEEP; it equals the fixture file's mtime in millis.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _version | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_version");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(
                    "_version must equal the fixture file's last-modified-time in millis",
                    ((Number) row.get(idx)).longValue(),
                    equalTo(fixtureMtimeMillis)
                );
            }
        }
    }

    public void testFileMetadataColumnsOnFromDataset() throws Exception {
        // _file.* virtual columns must surface on FROM <dataset> when requested via METADATA, with
        // the same shapes as the legacy EXTERNAL command auto-attaches them: per-file constants
        // populated from the file's StoragePath + stat. _file.record_ref is exercised via _id by
        // testIdRendersLocationAndRowPosition; this test pins the per-file constants.
        String query =
            "FROM employees METADATA _file.path, _file.name, _file.directory, _file.size, _file.modified | SORT emp_no | LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            // METADATA surfaces the _file.* family with no KEEP.
            int pathI = columnIndex(response.columns(), "_file.path");
            int nameI = columnIndex(response.columns(), "_file.name");
            int dirI = columnIndex(response.columns(), "_file.directory");
            int sizeI = columnIndex(response.columns(), "_file.size");
            int modI = columnIndex(response.columns(), "_file.modified");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            String firstPath = objToString(rows.get(0).get(pathI));
            String firstName = objToString(rows.get(0).get(nameI));
            String firstDirectory = objToString(rows.get(0).get(dirI));
            for (List<Object> row : rows) {
                assertThat("_file.path is non-null", row.get(pathI), notNullValue());
                assertThat("_file.name is non-null", row.get(nameI), notNullValue());
                assertThat("_file.directory is non-null", row.get(dirI), notNullValue());
                assertThat("_file.size is positive", ((Number) row.get(sizeI)).longValue(), greaterThan(0L));
                assertThat("_file.modified is non-null", row.get(modI), notNullValue());
                // All rows come from the same single-file fixture, so every per-file constant matches.
                assertThat("_file.path is per-file constant", objToString(row.get(pathI)), equalTo(firstPath));
                assertThat("_file.name is per-file constant", objToString(row.get(nameI)), equalTo(firstName));
                assertThat("_file.directory is per-file constant", objToString(row.get(dirI)), equalTo(firstDirectory));
            }
        }
    }

    public void testAllStandardMetadataColumnsPinned() throws Exception {
        // Standing contract: every non-snapshot standard metadata name is accepted in one query,
        // returns a value or SQL NULL (never an error), and the value/null disposition is pinned.
        // _id and _source are composed per-row; _index carries the dataset name; _version the file
        // mtime; the remaining five have no external semantic and come back as NULL columns.
        // _tier is snapshot-only — see testTierIsNullOnExternalRowsSnapshotOnly.
        String query = "FROM employees METADATA _id, _source, _index, _version, _ignored, _index_mode, _tsid, _size, _score "
            + "| SORT emp_no "
            + "| LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            // METADATA surfaces every named standard column with no KEEP.
            int idI = columnIndex(response.columns(), "_id");
            int sourceI = columnIndex(response.columns(), "_source");
            int indexI = columnIndex(response.columns(), "_index");
            int versionI = columnIndex(response.columns(), "_version");
            int ignoredI = columnIndex(response.columns(), "_ignored");
            int indexModeI = columnIndex(response.columns(), "_index_mode");
            int tsidI = columnIndex(response.columns(), "_tsid");
            int sizeI = columnIndex(response.columns(), "_size");
            int scoreI = columnIndex(response.columns(), "_score");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_id is composed per-row, non-null", row.get(idI), notNullValue());
                assertThat("_source deserializes to a Map", asMap(row.get(sourceI)), notNullValue());
                assertThat("_index is the dataset name", row.get(indexI).toString(), equalTo("employees"));
                assertThat("_version is non-null", row.get(versionI), notNullValue());
                assertThat("_ignored is null on external rows", row.get(ignoredI), nullValue());
                assertThat("_index_mode is null on external rows", row.get(indexModeI), nullValue());
                assertThat("_tsid is null on external rows", row.get(tsidI), nullValue());
                assertThat("_size is null on external rows", row.get(sizeI), nullValue());
                assertThat("_score is null on external rows", row.get(scoreI), nullValue());
            }
        }
    }

    /**
     * {@code _tier} only exists in snapshot builds (see {@code MetadataAttribute.ATTRIBUTES_MAP}).
     * When present in the metadata map it must bind for external datasets and surface as SQL NULL —
     * external rows have no tier — never an error. Gated on snapshot detection so non-snapshot CI
     * runs do not parse-fail on the unknown name.
     */
    public void testTierIsNullOnExternalRowsSnapshotOnly() throws Exception {
        assumeTrue(
            "_tier is registered only in snapshot builds",
            org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.dataType("_tier") != null
        );

        String query = "FROM employees METADATA _tier | SORT emp_no | LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_tier");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_tier is null on external rows", row.get(idx), nullValue());
            }
        }
    }

    /** The deserialized {@code _source} value is a {@link Map}; fail loudly if a format yields otherwise. */
    private static Map<?, ?> asMap(Object source) {
        assertThat("_source deserializes to a Map", source, org.hamcrest.Matchers.instanceOf(Map.class));
        return (Map<?, ?>) source;
    }

    /** Keyword values may surface as String or BytesRef depending on block plumbing; normalize to String. */
    private static String objToString(Object value) {
        return value == null ? null : value.toString();
    }
}
