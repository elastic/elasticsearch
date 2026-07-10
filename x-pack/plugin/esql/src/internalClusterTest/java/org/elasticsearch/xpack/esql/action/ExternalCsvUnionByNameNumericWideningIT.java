/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Multi-file CSV glob reads under {@code schema_resolution = union_by_name} where one file's shared column is inferred
 * from a numeric sampled prefix but carries a wider out-of-sample value further down, while another file infers the
 * wider numeric type. Reconciliation widens the column ({@code INTEGER} to {@code LONG} or {@code DOUBLE}), and because
 * a text reader parses each token at its pinned read type, the numeric-inferred file must be read at the reconciled
 * type so the out-of-sample value that overflows the narrower type still parses instead of null-filling.
 * <p>
 * {@code schema_sample_size} is small so the numeric-inferred file's sample sees only its leading narrow rows and never
 * the wider trailing row. {@code error_mode = null_field} keeps a lost value visible as a {@code null} cell rather than
 * a query failure.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvUnionByNameNumericWideningIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testIntInferredColumnKeepsOutOfSampleOverflowWhenWidenedToLong() throws Exception {
        Path dir = createTempDir().resolve("ubn_int_widened_long");
        Files.createDirectories(dir);
        // a.csv: sample (10, 20) infers INTEGER, but the trailing value overflows int and the sample never saw it.
        Files.writeString(dir.resolve("a.csv"), "id,col\n1,10\n2,20\n3,3000000000\n", StandardCharsets.UTF_8);
        // b.csv: values exceed int range -> inferred LONG, so union_by_name widens col to LONG.
        Files.writeString(dir.resolve("b.csv"), "id,col\n4,5000000000\n5,6000000000\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset(
            "ubn_int_widened_long",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );

        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains(10L, 20L, 3000000000L, 5000000000L, 6000000000L));
            assertThat("the out-of-sample overflow value must not be dropped", rows.get(2).get(0), equalTo(3000000000L));
        }
    }

    public void testIntInferredColumnKeepsOutOfSampleDecimalWhenWidenedToDouble() throws Exception {
        Path dir = createTempDir().resolve("ubn_int_widened_double");
        Files.createDirectories(dir);
        // a.csv: sample (10, 20) infers INTEGER, but the trailing value is a decimal the sample never saw.
        Files.writeString(dir.resolve("a.csv"), "id,col\n1,10\n2,20\n3,1.5\n", StandardCharsets.UTF_8);
        // b.csv: decimal values -> inferred DOUBLE, so union_by_name widens col to DOUBLE.
        Files.writeString(dir.resolve("b.csv"), "id,col\n4,1.1\n5,2.2\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset(
            "ubn_int_widened_double",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );

        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains(10.0, 20.0, 1.5, 1.1, 2.2));
            assertThat("the out-of-sample decimal value must not be dropped", rows.get(2).get(0), equalTo(1.5));
        }
    }

    public void testWidenedKeywordColumnStatsAggregateRawValuesAcrossRepeatedRuns() throws Exception {
        Path dir = createTempDir().resolve("ubn_keyword_stats");
        Files.createDirectories(dir);
        // a.csv: sample (100, 200) infers numeric, trailing text "oops" is out of sample.
        Files.writeString(dir.resolve("a.csv"), "id,col\n1,100\n2,200\n3,oops\n", StandardCharsets.UTF_8);
        // b.csv: text values -> inferred keyword, so union_by_name widens col to KEYWORD.
        Files.writeString(dir.resolve("b.csv"), "id,col\n4,abc\n5,def\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.csv";
        String dataset = registerDataset(
            "ubn_keyword_stats",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );

        // Run twice: the second run may serve warm per-column stats. The reconciled column is KEYWORD, read at KEYWORD,
        // so MIN/MAX must be the lexicographic keyword extrema of the raw tokens, not a stale numeric extremum. MAX is
        // the discriminating assertion: the keyword max "oops" differs from any numeric max (200), so a leaked numeric
        // extremum would fail here.
        String query = "FROM " + dataset + " | STATS mn = MIN(col), mx = MAX(col)";
        for (int run = 0; run < 2; run++) {
            try (var response = run(syncEsqlQueryRequest(query))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.get(0).get(0), equalTo("100"));
                assertThat(rows.get(0).get(1), equalTo("oops"));
            }
        }
    }
}
