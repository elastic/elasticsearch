/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Per-format matrix for the standard metadata columns surfaced on {@code FROM <external-dataset>}.
 *
 * <p>The wiring that surfaces {@code _index} (and the always-null set
 * {@code _score / _ignored / _index_mode / _tsid / _size}), plus the {@code _file.*} family, reaches
 * into the per-format <em>reader</em>, so a format-specific regression in any of those paths would
 * otherwise pass with only the CSV coverage in {@link FromDatasetIT}.
 *
 * <p>{@code _id}, {@code _version} and {@code _source} are not in the matrix: a file holds no
 * document identity, no document version and no stored source, so a dataset does not answer them.
 * {@link #testDocumentMetadataIsRejected} pins that.
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
     * keyword column, so a typed KEYWORD-family render is exercised.
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

    /**
     * A file holds no document identity, no document version and no stored source, so a dataset does not
     * answer {@code _id}, {@code _version} or {@code _source}: each is rejected the way an unknown
     * metadata name is, rather than answered with an invented value.
     */
    public void testDocumentMetadataIsRejected() {
        for (String name : List.of("_id", "_version", "_source")) {
            Exception e = expectThrows(
                Exception.class,
                () -> run(syncEsqlQueryRequest("FROM employees METADATA " + name + " | LIMIT 10"), TIMEOUT).close()
            );
            assertThat(e.getMessage(), containsString("Unresolved metadata pattern [" + name + "]"));
        }
    }

    public void testFileMetadataColumnsOnFromDataset() throws Exception {
        // _file.* virtual columns must surface on FROM <dataset> when requested via METADATA, with
        // the same shapes as the legacy EXTERNAL command auto-attaches them: per-file constants
        // populated from the file's StoragePath + stat.
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
        // Standing contract: every standard metadata name a dataset answers is accepted in one query,
        // returns a value or SQL NULL (never an error), and the value/null disposition is pinned.
        // _index carries the dataset name; the remaining five have no external semantic and come back
        // as NULL columns. _tier is snapshot-only — see testTierIsNullOnExternalRowsSnapshotOnly.
        String query = "FROM employees METADATA _index, _ignored, _index_mode, _tsid, _size, _score | SORT emp_no | LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            // METADATA surfaces every named standard column with no KEEP.
            int indexI = columnIndex(response.columns(), "_index");
            int ignoredI = columnIndex(response.columns(), "_ignored");
            int indexModeI = columnIndex(response.columns(), "_index_mode");
            int tsidI = columnIndex(response.columns(), "_tsid");
            int sizeI = columnIndex(response.columns(), "_size");
            int scoreI = columnIndex(response.columns(), "_score");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_index is the dataset name", row.get(indexI).toString(), equalTo("employees"));
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

    /** Keyword values may surface as String or BytesRef depending on block plumbing; normalize to String. */
    private static String objToString(Object value) {
        return value == null ? null : value.toString();
    }
}
