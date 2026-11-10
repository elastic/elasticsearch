/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * A CSV timestamp naming a day its month lacks (for example {@code 2024-02-30}) must not be read as a different,
 * real date. With no {@code datetime_format}, a declared {@code date} column treats it as an invalid cell routed
 * through {@code error_mode}, and an inferred column stays text. The controls pin that the real leap day, the ISO
 * end-of-day hour and the already-refused day 32 keep their behaviour, so refusing every date cannot pass as a fix.
 */
public class ExternalCsvImpossibleCalendarDateIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private Path file(String name, String body, String fileName) throws Exception {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Path f = dir.resolve(fileName);
        Files.writeString(f, body, StandardCharsets.UTF_8);
        return f;
    }

    private String inferred(String name, String body) throws Exception {
        return registerLocalFileDataset(name, StoragePath.fileUri(file(name, body, "d.csv")), Map.of());
    }

    private String inferredTsv(String name, String body) throws Exception {
        return registerLocalFileDataset(name, StoragePath.fileUri(file(name, body, "d.tsv")), Map.of());
    }

    private String declaredDatetime(String name, String body) throws Exception {
        return declaredDatetime(name, body, Map.of());
    }

    private String declaredDatetime(String name, String body, Map<String, Object> settings) throws Exception {
        return declaredDatetime(name, body, settings, "d.csv");
    }

    private String declaredDatetime(String name, String body, Map<String, Object> settings, String fileName) throws Exception {
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("t", new DatasetFieldMapping("date", null));
        props.put("qty", new DatasetFieldMapping("integer", null));
        return registerStrictDataset(name, StoragePath.fileUri(file(name, body, fileName)), props, settings);
    }

    private record Result(List<List<Object>> rows, List<String> warnings) {}

    private Result runWithWarnings(String query) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        AtomicReference<Result> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                result.set(new Result(getValuesList(response), threadContext.getResponseHeaders().getOrDefault("Warning", List.of())));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("query failed", failure.get());
        }
        return result.get();
    }

    /** Both reference engines type this column a string; it must not come back as February 29. */
    public void testInferredTimestampOnImpossibleDayStaysText() throws Exception {
        String ds = inferred("tform", "t,qty\n2024-02-30T10:00:00Z,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-02-30T10:00:00Z"))));
        }
    }

    public void testInferredSpaceTimestampOnImpossibleDayStaysText() throws Exception {
        String ds = inferred("sform", "t,qty\n2024-02-30 10:00:00,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-02-30 10:00:00"))));
        }
    }

    /**
     * A date-only value: DuckDB keeps it as text and ClickHouse rolls it forward to March 1, so the reference
     * engines disagree on what it becomes. They agree it is not February 29.
     */
    public void testInferredDateOnImpossibleDayIsNotMovedToMonthEnd() throws Exception {
        String ds = inferred("dform", "t,qty\n2024-02-30,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response).get(0).get(0), not(equalTo("2024-02-29T00:00:00.000Z")));
        }
    }

    public void testDeclaredDateRefusesImpossibleDay() throws Exception {
        List<String> days = List.of(
            "2024-02-30",
            "2023-02-29",
            "2024-04-31",
            "2024-02-30T10:00:00Z",
            "2024-02-30 10:00:00",
            "2024-02-30T24:00:00Z"
        );
        for (int i = 0; i < days.size(); i++) {
            String day = days.get(i);
            String ds = declaredDatetime("decl" + i, "t,qty\n" + day + ",1\n");
            Exception e = expectThrows(Exception.class, () -> {
                try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
                    fail("declared date read [" + day + "] as " + getValuesList(response));
                }
            });
            assertThat(e.getMessage(), containsString(day));
        }
    }

    public void testDeclaredDateSkipRowDropsImpossibleDayRow() throws Exception {
        String ds = declaredDatetime("skiprow", "t,qty\n2024-02-30T10:00:00Z,1\n", Map.of("error_mode", "skip_row"));
        Result projected = runWithWarnings("FROM " + ds + " | KEEP t");
        assertThat(projected.rows(), empty());
        assertThat(projected.warnings().toString(), projected.warnings().size(), greaterThanOrEqualTo(1));
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | STATS c = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1L))));
        }
    }

    public void testDeclaredDateNullFieldNullsImpossibleDay() throws Exception {
        String ds = declaredDatetime("nullfield", "t,qty\n2024-02-30T10:00:00Z,1\n", Map.of("error_mode", "null_field"));
        Result result = runWithWarnings("FROM " + ds + " | KEEP t, qty");
        assertThat(result.rows(), equalTo(List.of(java.util.Arrays.asList(null, 1))));
        assertThat(result.warnings().toString(), result.warnings().size(), greaterThanOrEqualTo(1));
    }

    /** One impossible day among real timestamps widens the whole column to text; no value is moved to another date. */
    public void testInferredColumnWithOneImpossibleDayWidensToText() throws Exception {
        String ds = inferred("mixed", "t,qty\n2024-02-28T10:00:00Z,1\n2024-02-30T10:00:00Z,2\n");
        Result result = runWithWarnings("FROM " + ds + " | SORT qty | KEEP t");
        assertThat(result.rows(), equalTo(List.of(List.of("2024-02-28T10:00:00Z"), List.of("2024-02-30T10:00:00Z"))));
    }

    public void testTsvInferredTimestampOnImpossibleDayStaysText() throws Exception {
        String ds = inferredTsv("tsvinf", "t\tqty\n2024-02-30T10:00:00Z\t1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-02-30T10:00:00Z"))));
        }
    }

    public void testTsvDeclaredDateRefusesImpossibleDay() throws Exception {
        String ds = declaredDatetime("tsvdecl", "t\tqty\n2024-02-30\t1\n", Map.of(), "d.tsv");
        Exception e = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
                fail("declared date read 2024-02-30 as " + getValuesList(response));
            }
        });
        assertThat(e.getMessage(), containsString("2024-02-30"));
    }

    /** Control: days 29 to 31 of months that have them still read, so strict parsing is not refusing every late day. */
    public void testEndOfMonthDaysStillRead() throws Exception {
        String ds = inferred("eom", "t,qty\n2024-01-31,1\n2024-12-31,2\n2024-04-30 10:00:00,3\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT qty | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(List.of("2024-01-31T00:00:00.000Z"), List.of("2024-12-31T00:00:00.000Z"), List.of("2024-04-30T10:00:00.000Z"))
                )
            );
        }
    }

    /** Control: the real leap day reads, inferred and declared. */
    public void testLeapDayStillReads() throws Exception {
        String ds = inferred("leap", "t,qty\n2024-02-29,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-02-29T00:00:00.000Z"))));
        }
        String decl = declaredDatetime("leapdecl", "t,qty\n2024-02-29 10:00:00,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + decl + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-02-29T10:00:00.000Z"))));
        }
    }

    /** Control: the ISO end-of-day hour still reads as the next day's midnight, as it does in both reference engines. */
    public void testEndOfDayHourStillReads() throws Exception {
        String ds = inferred("eod", "t,qty\n2024-06-01T24:00:00Z,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-06-02T00:00:00.000Z"))));
        }
        String decl = declaredDatetime("eoddecl", "t,qty\n2024-06-01 24:00:00,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + decl + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("2024-06-02T00:00:00.000Z"))));
        }
    }

    /** Control: a day no month has is already refused. */
    public void testDeclaredDateAlreadyRefusesDayThirtyTwo() throws Exception {
        String ds = declaredDatetime("d32", "t,qty\n2024-01-32,1\n");
        Exception e = expectThrows(Exception.class, () -> {
            try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
                fail("declared date read 2024-01-32 as " + getValuesList(response));
            }
        });
        assertThat(e.getMessage(), containsString("2024-01-32"));
    }
}
