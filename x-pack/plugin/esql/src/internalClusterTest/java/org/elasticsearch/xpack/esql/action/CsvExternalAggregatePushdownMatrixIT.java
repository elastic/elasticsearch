/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/** CSV binding of {@link AbstractExternalAggregatePushdownMatrixIT}. */
public class CsvExternalAggregatePushdownMatrixIT extends AbstractExternalAggregatePushdownMatrixIT {

    @Override
    protected String format() {
        return "csv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir, int rows) throws Exception {
        StringBuilder sb = new StringBuilder("emp_no:long,label:keyword,val:double\n");
        for (int i = 0; i < rows; i++) {
            sb.append(i).append(',').append(label(i)).append(',').append(i + 0.5).append('\n');
        }
        Path file = dir.resolve("employees.csv");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }

    /**
     * MIN/MAX on a double column containing a NaN must equal a full scan: the runtime aggregator uses
     * {@code Math.min}/{@code Math.max}, which propagate NaN, so any NaN makes the result NaN. The warm path
     * must return the same NaN the cold scan does (it must not skip the NaN and serve a normal extreme).
     */
    public void testMinMaxNaNDoubleColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        StringBuilder sb = new StringBuilder("emp_no:long,d:double\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append(i).append(',').append(i == 7 ? "NaN" : Double.toString(i + 0.5)).append('\n');
        }
        Path file = dir.resolve("nan.csv");
        Files.writeString(file, sb.toString());
        registerDataset("nan_employees", StoragePath.fileUri(file));

        assertColdThenWarmShortCircuit("nan_employees", "STATS lo = MIN(d), hi = MAX(d)", ROWS, rows -> {
            assertThat("MIN(d) is NaN when any value is NaN", ((Number) rows.get(0).get(0)).doubleValue(), equalTo(Double.NaN));
            assertThat("MAX(d) is NaN when any value is NaN", ((Number) rows.get(0).get(1)).doubleValue(), equalTo(Double.NaN));
        });
    }

    /**
     * Multivalue via {@code multi_value_syntax: brackets} (the text-format path, distinct from NDJSON arrays):
     * a bracketed cell {@code [a,b]} is two values in one column, so {@code COUNT(tags)} counts VALUES
     * ({@code 2*ROWS}, not {@code ROWS}) and {@code MIN}/{@code MAX} fold across ALL values. Row {@code i}
     * carries {@code [label(2i),label(2i+1)]}, so the whole-file MIN is {@code label(0)} and MAX is
     * {@code label(2*ROWS-1)}. This exercises the CSV reader producing multivalued blocks, which the
     * stats harvest must value-count correctly.
     */
    public void testBracketMultivalueColumnColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        StringBuilder sb = new StringBuilder("id:long,tags:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append(i).append(",[").append(label(2 * i)).append(',').append(label(2 * i + 1)).append("]\n");
        }
        Path file = dir.resolve("mv.csv");
        Files.writeString(file, sb.toString());
        registerDataset("mv_brackets", StoragePath.fileUri(file), Map.of("multi_value_syntax", "brackets"));

        assertColdThenWarmShortCircuit("mv_brackets", "STATS c = COUNT(tags), lo = MIN(tags), hi = MAX(tags)", ROWS, rows -> {
            assertThat("COUNT(tags) counts bracket values, not rows", ((Number) rows.get(0).get(0)).longValue(), equalTo(2L * ROWS));
            assertThat("MIN(tags) folds across all bracket values", String.valueOf(rows.get(0).get(1)), equalTo(label(0)));
            assertThat("MAX(tags) folds across all bracket values", String.valueOf(rows.get(0).get(2)), equalTo(label(2 * ROWS - 1)));
        });
    }
}
