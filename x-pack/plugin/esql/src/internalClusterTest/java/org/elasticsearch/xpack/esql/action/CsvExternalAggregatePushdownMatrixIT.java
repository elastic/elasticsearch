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
}
