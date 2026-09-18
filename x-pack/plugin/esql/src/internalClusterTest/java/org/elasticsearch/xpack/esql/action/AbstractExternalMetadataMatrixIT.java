/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Per-format matrix for the standard metadata columns surfaced on {@code FROM <external-dataset>}.
 *
 * <p>The wiring that surfaces the always-null set {@code _index / _id / _version / _source / _score /
 * _ignored / _index_mode / _tsid / _size}, plus the {@code _file.*} family, reaches into the
 * per-format <em>reader</em>, so a format-specific
 * regression in any of those paths would otherwise pass with only the CSV coverage in
 * {@link FromDatasetIT}.
 *
 * <p>{@code _id}, {@code _version} and {@code _source} are in that set because a file holds no
 * document identity, no document version and no stored source; {@code _index} is in it because it
 * names an index and a dataset is not one; {@code _score} is in it because nothing ranks a dataset
 * row, so there is no relevance to report. The name that does answer for a dataset is {@code _name}.
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
     * {@code host_ip} (keyword) "10.0.0.1","10.0.0.2","10.0.0.3". No test here reads {@code host_ip};
     * it keeps the fixture wider than the projections under test, so a reader that mis-associates a
     * metadata column with a file column has a third column to get wrong.
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

    public void testIndexIsNullAndNameIsTheDatasetName() throws Exception {
        // METADATA surfaces both with no KEEP. _index names an index and a dataset is not one, so it
        // answers SQL NULL; _name is the column that answers which relation a row came from.
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index, _name | SORT emp_no | LIMIT 10"), TIMEOUT)) {
            int indexI = columnIndex(response.columns(), "_index");
            int nameI = columnIndex(response.columns(), "_name");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat("_index is null on a dataset", row.get(indexI), nullValue());
                assertThat("_name is the dataset name", row.get(nameI).toString(), equalTo("employees"));
            }
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
        // None of the nine has an external semantic, so every one comes back as a NULL column.
        // _tier is snapshot-only — see testTierIsNullOnExternalRowsSnapshotOnly.
        String query = "FROM employees METADATA _index, _id, _version, _source, _ignored, _index_mode, _tsid, _size, _score "
            + "| SORT emp_no | LIMIT 10";

        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            // METADATA surfaces every named standard column with no KEEP.
            int indexI = columnIndex(response.columns(), "_index");
            int idI = columnIndex(response.columns(), "_id");
            int versionI = columnIndex(response.columns(), "_version");
            int sourceI = columnIndex(response.columns(), "_source");
            int ignoredI = columnIndex(response.columns(), "_ignored");
            int indexModeI = columnIndex(response.columns(), "_index_mode");
            int tsidI = columnIndex(response.columns(), "_tsid");
            int sizeI = columnIndex(response.columns(), "_size");
            int scoreI = columnIndex(response.columns(), "_score");

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                // _index names an index and a dataset is not one; _name answers that question instead.
                assertThat("_index is null on external rows", row.get(indexI), nullValue());
                // A file holds no document identity, no document version and no stored source.
                assertThat("_id is null on external rows", row.get(idI), nullValue());
                assertThat("_version is null on external rows", row.get(versionI), nullValue());
                assertThat("_source is null on external rows", row.get(sourceI), nullValue());
                assertThat("_ignored is null on external rows", row.get(ignoredI), nullValue());
                assertThat("_index_mode is null on external rows", row.get(indexModeI), nullValue());
                assertThat("_tsid is null on external rows", row.get(tsidI), nullValue());
                assertThat("_size is null on external rows", row.get(sizeI), nullValue());
                assertThat("_score is null on external rows", row.get(scoreI), nullValue());
            }
        }
    }

    public void testMetadataColumnGroupsInStats() throws Exception {
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | STATS c = COUNT(*) BY _index"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_index");
            int countIdx = columnIndex(response.columns(), "c");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat("every row shares the one null _index group", rows.get(0).get(idx), nullValue());
            assertThat(((Number) rows.get(0).get(countIdx)).longValue(), equalTo(3L));
        }

        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _file.name | STATS c = COUNT(*) BY _file.name"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            int nameIdx = columnIndex(response.columns(), "_file.name");
            int countIdx = columnIndex(response.columns(), "c");
            assertThat(rows.get(0).get(nameIdx), notNullValue());
            assertThat(((Number) rows.get(0).get(countIdx)).longValue(), equalTo(3L));
        }
    }

    /**
     * Iterates the live metadata registries so a name added later is grouped without extending this
     * test. Snapshot-only and feature-flagged names ({@code _tier}, {@code _slice}) appear only when
     * the corresponding map entry is present.
     */
    public void testEveryMetadataColumnGroupsInStats() throws Exception {
        Set<String> names = new LinkedHashSet<>();
        names.addAll(MetadataAttribute.ATTRIBUTES_MAP.keySet());
        names.addAll(FileMetadataColumns.COLUMNS.keySet());
        List<String> failures = new ArrayList<>();
        for (String name : names) {
            String query = "FROM employees METADATA " + name + " | STATS c = COUNT(*) BY " + name;
            try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                List<String> columns = response.columns().stream().map(ColumnInfo::name).toList();
                if (rows.isEmpty()) {
                    failures.add(name + ": empty result");
                }
                if (columns.contains("c") == false) {
                    failures.add(name + ": missing c, columns=" + columns);
                }
                if (columns.contains(name) == false) {
                    failures.add(name + ": missing grouping column, columns=" + columns);
                }
            } catch (Exception e) {
                failures.add(name + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        if (failures.isEmpty() == false) {
            fail("grouping by " + names.size() + " metadata columns failed:\n" + String.join("\n", failures));
        }
    }

    public void testMetadataColumnGroupsInInlineStats() throws Exception {
        assumeTrue("INLINE STATS requires the capability to be enabled", INLINE_STATS.isEnabled());
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | INLINE STATS c = COUNT(*) BY _index"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "_index");
            int countIdx = columnIndex(response.columns(), "c");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(idx), nullValue());
                assertThat(((Number) row.get(countIdx)).longValue(), equalTo(3L));
            }
        }
    }

    public void testMetadataColumnGroupsInStatsWithAlias() throws Exception {
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | STATS c = COUNT(*) BY i = _index"), TIMEOUT)) {
            int idx = columnIndex(response.columns(), "i");
            int countIdx = columnIndex(response.columns(), "c");
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(idx), nullValue());
            assertThat(((Number) rows.get(0).get(countIdx)).longValue(), equalTo(3L));
        }
    }

    /**
     * {@code _tier} only exists in snapshot builds (see {@code MetadataAttribute.ATTRIBUTES_MAP}).
     * When present in the metadata map it must bind for external datasets and surface as SQL NULL —
     * external rows have no tier — never an error. Gated on snapshot detection so non-snapshot CI
     * runs do not parse-fail on the unknown name.
     */
    public void testTierIsNullOnExternalRowsSnapshotOnly() throws Exception {
        assumeTrue("_tier is registered only in snapshot builds", MetadataAttribute.dataType("_tier") != null);

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

    public void testMetadataFilterSelectsRowsAndCountsThem() throws Exception {
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | WHERE _index IS NULL | SORT emp_no"), TIMEOUT)) {
            assertThat(getValuesList(response), hasSize(3));
        }
        try (
            var response = run(
                // Equality against a NULL column is UNKNOWN, so it selects nothing even for the dataset's own name.
                syncEsqlQueryRequest("FROM employees METADATA _index | WHERE _index == \"employees\" | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (
            var response = run(syncEsqlQueryRequest("FROM employees METADATA _index | WHERE _index IS NULL | STATS c = COUNT(*)"), TIMEOUT)
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _file.name | WHERE _file.name IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _file.record_ref | WHERE _file.record_ref IS NOT NULL | SORT emp_no"),
                TIMEOUT
            )
        ) {
            assertThat(getValuesList(response), hasSize(3));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _file.record_ref | WHERE _file.record_ref IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _version | WHERE _version IS NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _version | WHERE _version IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (
            var response = run(syncEsqlQueryRequest("FROM employees METADATA _score | WHERE _score IS NULL | STATS c = COUNT(*)"), TIMEOUT)
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _score | WHERE _score IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _name | WHERE _name == \"nosuchdataset\" | SORT emp_no"),
                TIMEOUT
            )
        ) {
            assertThat(getValuesList(response), hasSize(0));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _index | WHERE NOT (_index IS NULL) | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _index | EVAL idx = _index | WHERE idx IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (var response = run(syncEsqlQueryRequest("FROM employees METADATA _id | WHERE _id IS NULL | STATS c = COUNT(*)"), TIMEOUT)) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(syncEsqlQueryRequest("FROM employees METADATA _id | WHERE _id IS NOT NULL | STATS c = COUNT(*)"), TIMEOUT)
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _source | WHERE _source IS NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
        try (
            var response = run(
                syncEsqlQueryRequest("FROM employees METADATA _source | WHERE _source IS NOT NULL | STATS c = COUNT(*)"),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
    }

    public void testComputedMetadataFilterCounts() throws Exception {
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM employees METADATA _index | EVAL idx = TO_LOWER(_index) | WHERE idx IS NOT NULL | STATS c = COUNT(*)"
                ),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(0L));
        }
    }

    public void testComputedMetadataIsNullFilterCounts() throws Exception {
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM employees METADATA _index | EVAL idx = TO_LOWER(_index) | WHERE idx IS NULL | STATS c = COUNT(*)"
                ),
                TIMEOUT
            )
        ) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    public void testComputedMetadataShadowingIndexFiltersRowsAndCounts() throws Exception {
        // _index is NULL on a dataset, so the computed value is built from _name — the column that does
        // carry the relation's name. What is pinned is the shadowing: a user column named _index wins
        // over the metadata one for every downstream reference.
        String source = "FROM employees METADATA _index, _name | EVAL _index = CONCAT(_name, \"mytext\")";
        for (var testCase : List.of(
            Map.entry("_index == \"employeesmytext\"", 3L),
            Map.entry("_index == \"employees\"", 0L),
            Map.entry("_index IS NULL", 0L),
            Map.entry("_index IS NOT NULL", 3L)
        )) {
            String filteredQuery = source + " | WHERE " + testCase.getKey();
            try (var response = run(syncEsqlQueryRequest(filteredQuery + " | KEEP _index"), TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(filteredQuery, rows, hasSize(testCase.getValue().intValue()));
                for (List<Object> row : rows) {
                    assertThat(row.get(0).toString(), equalTo("employeesmytext"));
                }
            }
            try (var response = run(syncEsqlQueryRequest(filteredQuery + " | STATS c = COUNT(*)"), TIMEOUT)) {
                assertThat(filteredQuery, ((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(testCase.getValue()));
            }
        }
    }

    /** Keyword values may surface as String or BytesRef depending on block plumbing; normalize to String. */
    private static String objToString(Object value) {
        return value == null ? null : value.toString();
    }
}
