/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Two Parquet files under {@code schema_resolution=union_by_name} store column {@code c} at different types: as a
 * string in one, as int64 in the other. Reconciliation widens {@code c} to keyword, stringifying the integer
 * file's values above the reader. The LIKE family ({@code LIKE}, {@code STARTS_WITH}, {@code ENDS_WITH},
 * {@code CONTAINS}, and {@code NOT} of these) is pushed to the Parquet reader as an exact filter, which on the
 * integer file sees long values it cannot match a string pattern against and lets every row through. The planner
 * must therefore keep such a conjunct in the engine as well, so every query here answers against the
 * stringified values, i.e. exactly what the {@code STATS ... WHERE} form, which never leaves the engine, answers.
 * <p>
 * Values are {@code 10, 20, 11} in both files, so each filter has a different answer on the two-file dataset
 * than on either file alone; a dropped re-check shows as a count of 5 (or 4 for the negation) instead of 4
 * (or 2).
 * <p>
 * Pinned to one data node: {@code file://} datasets are read from each data node's local filesystem.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalParquetUnionByNameKeywordLikeIT extends AbstractExternalDataSourceIT {

    private String dataset;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void writeFixtureAndRegister() throws Exception {
        Path dir = createTempDir();
        String[] strings = { "10", "20", "11" };
        long[] longs = { 10L, 20L, 11L };
        writeParquet(
            dir.resolve("s.parquet"),
            "message s { required binary c (UTF8); }",
            strings.length,
            100,
            (g, i) -> { g.add("c", strings[i]); }
        );
        writeParquet(dir.resolve("n.parquet"), "message n { required int64 c; }", longs.length, 100, (g, i) -> { g.add("c", longs[i]); });
        // The reader follows the .parquet extension; the resolution must be explicit, see AbstractExternalDataSourceIT.
        dataset = registerDataset("kw_like", StoragePath.fileUri(dir) + "/*.parquet", Map.of("schema_resolution", "union_by_name"));
    }

    /** The reconciled column reads back as keyword with the integer file's values stringified. */
    public void testWidenedColumnReadsAsKeyword() {
        try (var response = run("FROM " + dataset + " | SORT c ASC | KEEP c")) {
            assertThat(response.columns().size(), equalTo(1));
            assertThat(response.columns().get(0).outputType(), equalTo("keyword"));
            List<String> values = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                Object cell = row.get(0);
                values.add(cell instanceof BytesRef bytes ? bytes.utf8ToString() : String.valueOf(cell));
            }
            assertThat(values, equalTo(List.of("10", "10", "11", "11", "20", "20")));
        }
    }

    public void testLikeKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE c LIKE \"1*\" | STATS n = COUNT(*)", 4);
    }

    public void testLikeInfixKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE c LIKE \"*1*\" | STATS n = COUNT(*)", 4);
    }

    public void testStartsWithKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE STARTS_WITH(c, \"1\") | STATS n = COUNT(*)", 4);
    }

    public void testEndsWithKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE ENDS_WITH(c, \"0\") | STATS n = COUNT(*)", 4);
    }

    public void testContainsKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE CONTAINS(c, \"0\") | STATS n = COUNT(*)", 4);
    }

    public void testNotLikeKeepsOnlyNonMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE NOT (c LIKE \"1*\") | STATS n = COUNT(*)", 2);
    }

    /** The LIKE inside an AND is still the exact-pushdown shape; the other conjunct is re-checked anyway. */
    public void testLikeInConjunctionKeepsOnlyMatchingRowsOfWidenedFile() {
        assertCount("FROM " + dataset + " | WHERE c LIKE \"1*\" AND c != \"11\" | STATS n = COUNT(*)", 2);
    }

    /** Control: the filtered aggregate keeps the LIKE in the engine and sees every row of both files. */
    public void testFilteredAggregateControl() {
        try (var response = run("FROM " + dataset + " | STATS rows = COUNT(*), n = COUNT(*) WHERE c LIKE \"1*\"")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(6L));
            assertThat(((Number) rows.get(0).get(1)).longValue(), equalTo(4L));
        }
    }

    /** Control: a comparison is re-checked by the engine and was never affected. */
    public void testEqualityControl() {
        assertCount("FROM " + dataset + " | WHERE c == \"10\" | STATS n = COUNT(*)", 2);
    }

    private void assertCount(String query, long expected) {
        try (var response = run(query)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(query, rows.size(), equalTo(1));
            assertThat(query, ((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
        }
    }
}
