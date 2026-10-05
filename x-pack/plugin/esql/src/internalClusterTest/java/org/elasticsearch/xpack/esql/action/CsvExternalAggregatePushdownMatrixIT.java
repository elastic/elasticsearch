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
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

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
        registerFormatDataset("mv_brackets", StoragePath.fileUri(file), Map.of("multi_value_syntax", "brackets"));

        assertColdThenWarmShortCircuit("mv_brackets", "STATS c = COUNT(tags), lo = MIN(tags), hi = MAX(tags)", ROWS, rows -> {
            assertThat("COUNT(tags) counts bracket values, not rows", ((Number) rows.get(0).get(0)).longValue(), equalTo(2L * ROWS));
            assertThat("MIN(tags) folds across all bracket values", String.valueOf(rows.get(0).get(1)), equalTo(label(0)));
            assertThat("MAX(tags) folds across all bracket values", String.valueOf(rows.get(0).get(2)), equalTo(label(2 * ROWS - 1)));
        });
    }

    /**
     * #150920 regression (elastic/elasticsearch): {@code MIN/MAX(ip)} must serve the SAME value warm as a
     * cold scan. IP is harvested as its 16-byte InetAddressPoint encoding (byte-lex order == address order),
     * and the warm serve reconstructs an IP block from that encoding. Before {@code buildBlock} gained an IP
     * arm, the harvested {@code BytesRef} fell to the default and warm {@code MIN/MAX(ip)} answered NULL.
     * Addresses increase with the row, so MIN is row 0's address and MAX is row {@code ROWS-1}'s.
     */
    public void testMinMaxIpColdThenWarmShortCircuits() throws Exception {
        Path dir = createTempDir();
        StringBuilder sb = new StringBuilder("emp_no:long,addr:ip\n");
        for (int i = 0; i < ROWS; i++) {
            sb.append(i).append(",10.0.0.").append(i).append('\n');
        }
        Path file = dir.resolve("ip.csv");
        Files.writeString(file, sb.toString());
        registerDataset("ip_employees", StoragePath.fileUri(file));

        assertColdThenWarmShortCircuit("ip_employees", "STATS lo = MIN(addr), hi = MAX(addr)", ROWS, rows -> {
            assertThat("MIN(addr)", String.valueOf(rows.get(0).get(0)), equalTo("10.0.0.0"));
            assertThat("MAX(addr)", String.valueOf(rows.get(0).get(1)), equalTo("10.0.0." + (ROWS - 1)));
        });
    }

    /**
     * Systematic cold==warm coverage for EVERY warm-servable {@code MIN/MAX} type ({@code MIN_MAX_TYPES}:
     * BOOLEAN, INTEGER, LONG, DOUBLE, DATETIME, DATE_NANOS, KEYWORD/TEXT, IP). CSV declares TEXT as KEYWORD,
     * so they share one column; the warm-serve / {@code buildBlock} path under test is format-agnostic, so
     * CSV exercises it for all types. Each type gets its own fixture + dataset to isolate the warm-cache flip.
     * This is the anti-piecemeal guard: a supported type without warm coverage (like {@code MIN/MAX(ip)}
     * once was — it served NULL) shows up here rather than being found one type at a time.
     */
    public void testAllMinMaxTypesColdThenWarmShortCircuit() throws Exception {
        record TypeCase(String token, String col, IntFunction<String> value) {}
        List<TypeCase> cases = List.of(
            new TypeCase("boolean", "b_flag", i -> (i % 2 == 0) ? "true" : "false"),
            new TypeCase("integer", "i_n", i -> Integer.toString(i)),
            new TypeCase("long", "l_n", i -> Long.toString(i)),
            new TypeCase("double", "d_v", i -> Double.toString(i + 0.5)),
            new TypeCase("datetime", "dt_ts", i -> String.format(Locale.ROOT, "2020-01-01T00:00:%02d.000Z", i)),
            new TypeCase("date_nanos", "dn_ts", i -> String.format(Locale.ROOT, "2020-01-01T00:00:%02d.000000000Z", i)),
            new TypeCase("keyword", "k_lbl", i -> label(i)),
            new TypeCase("ip", "ip_addr", i -> "10.0.0." + i)
        );
        for (TypeCase c : cases) {
            Path dir = createTempDir();
            StringBuilder sb = new StringBuilder("id:long," + c.col() + ":" + c.token() + "\n");
            for (int i = 0; i < ROWS; i++) {
                sb.append(i).append(',').append(c.value().apply(i)).append('\n');
            }
            Path file = dir.resolve(c.col() + ".csv");
            Files.writeString(file, sb.toString());
            String ds = "mmtype_" + c.col();
            registerDataset(ds, StoragePath.fileUri(file));
            assertColdEqualsWarmMinMax(ds, c.col());
        }
    }

}
