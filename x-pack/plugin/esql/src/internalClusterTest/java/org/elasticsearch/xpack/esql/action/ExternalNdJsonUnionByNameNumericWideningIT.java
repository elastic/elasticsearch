/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The NDJSON counterpart of {@link ExternalCsvUnionByNameNumericWideningIT}: a multi-file NDJSON glob read under
 * {@code schema_resolution = union_by_name} where one file's shared column is inferred from a narrow sampled prefix
 * but carries a wider out-of-sample value further down, while another file infers the wider type. Reconciliation
 * widens the column ({@code INTEGER} to {@code LONG}, or a numeric/text mix to {@code KEYWORD}), and because the
 * narrow-inferred file is pinned to the reconciled read type its out-of-sample value still parses instead of
 * null-filling.
 * <p>
 * NDJSON is worth its own end-to-end coverage because it decodes through {@link org.elasticsearch.xpack.esql.datasource
 * .ndjson.NdJsonPageDecoder}, a separate path from the {@code CsvFormatReader} the CSV/TSV suites exercise. The
 * reconciliation-level pinning is asserted format-agnostically in {@code SchemaReconciliationTests}; this test proves
 * the decoder honours the pin on real data.
 * <p>
 * {@code schema_sample_size} is small so the narrow-inferred file's sample sees only its leading rows and never the
 * wider trailing row. Under {@code error_mode = null_field} a value that does not fit its read type surfaces as a
 * {@code null} cell; under {@code error_mode = fail_fast} it aborts the query. NDJSON decodes strict reads through its
 * own {@code decodePageFailFast} path, so both error modes are covered here rather than relying on the CSV suite.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalNdJsonUnionByNameNumericWideningIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    public void testNumericInferredColumnKeepsTextTailWhenWidenedToKeyword() throws Exception {
        Path dir = createTempDir().resolve("ubn_ndjson_widened_keyword");
        Files.createDirectories(dir);
        // a.ndjson: with schema_sample_size=2 the sampler sees only col=100 and col=200 -> inferred numeric,
        // but the third record carries a text value the sample never saw.
        Files.writeString(
            dir.resolve("a.ndjson"),
            "{\"id\":1,\"col\":100}\n{\"id\":2,\"col\":200}\n{\"id\":3,\"col\":\"oops\"}\n",
            StandardCharsets.UTF_8
        );
        // b.ndjson: col is text in every sampled record -> inferred keyword, so union_by_name widens col to keyword.
        Files.writeString(dir.resolve("b.ndjson"), "{\"id\":4,\"col\":\"abc\"}\n{\"id\":5,\"col\":\"def\"}\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset(
            "ubn_ndjson_widened_keyword",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "null_field")
        );

        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            // col is keyword in the unified schema, so numeric tokens read back as their raw string form. The
            // third record's text value "oops" is the one the sample never saw; it must survive rather than null-fill.
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains("100", "200", "oops", "abc", "def"));
            assertThat("the numeric-inferred file's text tail must not be dropped", rows.get(2).get(0), equalTo("oops"));
        }
    }

    public void testIntInferredColumnKeepsOutOfSampleOverflowWhenWidenedToLong() throws Exception {
        Path dir = createTempDir().resolve("ubn_ndjson_widened_long");
        Files.createDirectories(dir);
        // a.ndjson: sample (10, 20) infers INTEGER, but the trailing value overflows int and the sample never saw it.
        Files.writeString(
            dir.resolve("a.ndjson"),
            "{\"id\":1,\"col\":10}\n{\"id\":2,\"col\":20}\n{\"id\":3,\"col\":3000000000}\n",
            StandardCharsets.UTF_8
        );
        // b.ndjson: values exceed int range -> inferred LONG, so union_by_name widens col to LONG.
        Files.writeString(
            dir.resolve("b.ndjson"),
            "{\"id\":4,\"col\":5000000000}\n{\"id\":5,\"col\":6000000000}\n",
            StandardCharsets.UTF_8
        );

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset(
            "ubn_ndjson_widened_long",
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

    public void testFailFastNumericInferredColumnKeepsTextTailWhenWidenedToKeyword() throws Exception {
        Path dir = createTempDir().resolve("ubn_ndjson_failfast_keyword");
        Files.createDirectories(dir);
        // a.ndjson: sample (100, 200) infers numeric; the out-of-sample text "oops" would fail a numeric coercion.
        Files.writeString(
            dir.resolve("a.ndjson"),
            "{\"id\":1,\"col\":100}\n{\"id\":2,\"col\":200}\n{\"id\":3,\"col\":\"oops\"}\n",
            StandardCharsets.UTF_8
        );
        // b.ndjson: col is text -> inferred keyword, so union_by_name widens col to keyword.
        Files.writeString(dir.resolve("b.ndjson"), "{\"id\":4,\"col\":\"abc\"}\n{\"id\":5,\"col\":\"def\"}\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset(
            "ubn_ndjson_failfast_keyword",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "fail_fast")
        );

        // Under fail_fast, coercing "oops" to the sampled numeric type would abort the whole query through the NDJSON
        // decodePageFailFast path. Pinned to KEYWORD the decoder returns the raw token, so the query succeeds.
        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains("100", "200", "oops", "abc", "def"));
            assertThat("the numeric-inferred file's text tail must not abort the read", rows.get(2).get(0), equalTo("oops"));
        }
    }

    public void testFailFastIntInferredColumnKeepsOutOfSampleOverflowWhenWidenedToLong() throws Exception {
        Path dir = createTempDir().resolve("ubn_ndjson_failfast_long");
        Files.createDirectories(dir);
        // a.ndjson: sample (10, 20) infers INTEGER; the out-of-sample value overflows int and would abort a fail_fast read.
        Files.writeString(
            dir.resolve("a.ndjson"),
            "{\"id\":1,\"col\":10}\n{\"id\":2,\"col\":20}\n{\"id\":3,\"col\":3000000000}\n",
            StandardCharsets.UTF_8
        );
        // b.ndjson: values exceed int range -> inferred LONG, so union_by_name widens col to LONG.
        Files.writeString(
            dir.resolve("b.ndjson"),
            "{\"id\":4,\"col\":5000000000}\n{\"id\":5,\"col\":6000000000}\n",
            StandardCharsets.UTF_8
        );

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset(
            "ubn_ndjson_failfast_long",
            glob,
            Map.of("schema_resolution", "union_by_name", "schema_sample_size", 2, "error_mode", "fail_fast")
        );

        // Pinned to LONG the decoder parses 3000000000 directly, so the fail_fast query succeeds instead of aborting.
        String query = "FROM " + dataset + " | SORT id ASC | KEEP col";
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains(10L, 20L, 3000000000L, 5000000000L, 6000000000L));
            assertThat("the out-of-sample overflow value must not abort the read", rows.get(2).get(0), equalTo(3000000000L));
        }
    }

    /**
     * Two files, one integer-only and one fractional, under default {@code union_by_name}. The same
     * two values in one file infer {@code double}; split across files they must still be {@code double}
     * so {@code SUM(v)} analyzes. {@code 9007199254740993} is not an exact double (it reads as
     * {@code 9007199254740992.0}); adding {@code 1.5} lands on {@code 9007199254740994.0} because the
     * ulp at {@code 2^53} is 2.
     */
    public void testSplitLongAndDoubleFilesWidenToDoubleAndSumResolves() throws Exception {
        Path dir = createTempDir().resolve("ubn_ndjson_long_double");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a.ndjson"), "{\"v\":9007199254740993}\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("b.ndjson"), "{\"v\":1.5}\n", StandardCharsets.UTF_8);

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset("ubn_ndjson_long_double", glob, Map.of("schema_resolution", "union_by_name"));
        String query = "FROM " + dataset + " | STATS s = SUM(v)";

        // Plan-time SkipWarnings land on the coordinator request thread. Execute there and read
        // that node's Warning headers so the precision-loss phrases are asserted on the client
        // path, not only via a hand-bound test ThreadContext (same pattern as
        // ExternalCsvHivePartitionedIT.testHivePartitionShadowWarningReachesClient).
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<String> outputType = new AtomicReference<>();
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        AtomicReference<Integer> columnCount = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                columnCount.set(response.columns().size());
                outputType.set(response.columns().get(0).outputType());
                values.set(getValuesList(response));
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                threadContext.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(w -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(w, false)))
                    .forEach(warnings::add);
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("SUM(v) must analyze after LONG + DOUBLE widens to double", failure.get());
        }

        assertThat(columnCount.get(), equalTo(1));
        assertThat(outputType.get(), equalTo("double"));
        assertThat(values.get(), hasSize(1));
        assertThat(values.get().get(0).get(0), equalTo(9007199254740994.0));

        String pathA = StoragePath.ofLocalPath(dir.resolve("a.ndjson")).toString();
        String pathB = StoragePath.ofLocalPath(dir.resolve("b.ndjson")).toString();
        String summary = "Schema reconciliation widened long and double columns to double; integers above 2^53"
            + " are not exact. Hint: use schema_resolution = \"strict\" to fail instead.";
        String detailAb = "Column [v] widened to double: " + pathA + " (long), " + pathB + " (double); distinct types: [long, double]";
        String detailBa = "Column [v] widened to double: " + pathB + " (double), " + pathA + " (long); distinct types: [double, long]";
        List<String> reconciliation = warnings.stream()
            .filter(w -> w.startsWith("Schema reconciliation") || w.startsWith("Column ["))
            .toList();
        assertThat(reconciliation, containsInAnyOrder(equalTo(summary), either(equalTo(detailAb)).or(equalTo(detailBa))));
    }
}
