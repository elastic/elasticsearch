/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * A CSV or TSV cell naming a day its month does not have ({@code 2024-02-30}, {@code 2023-02-29}, {@code 2024-04-31}).
 * ClickHouse and DuckDB both refuse such a value in a declared timestamp column, and both keep a timestamp-shaped one
 * as text under inference. Neither returns the last day of the month.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalCsvImpossibleCalendarDateIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private Path file(String name, String body) throws Exception {
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Path f = dir.resolve("d.csv");
        Files.writeString(f, body, StandardCharsets.UTF_8);
        return f;
    }

    private String inferred(String name, String body) throws Exception {
        return registerLocalFileDataset(name, StoragePath.fileUri(file(name, body)), Map.of());
    }

    private String declaredDatetime(String name, String body) throws Exception {
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("t", new DatasetFieldMapping("date", null));
        props.put("qty", new DatasetFieldMapping("integer", null));
        return registerStrictDataset(name, StoragePath.fileUri(file(name, body)), props, Map.of());
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
     * A date-only value: DuckDB keeps it as text and ClickHouse rolls it forward to March 1, so the reference engines
     * disagree on what it becomes. They agree it is not February 29.
     */
    public void testInferredDateOnImpossibleDayIsNotMovedToMonthEnd() throws Exception {
        String ds = inferred("dform", "t,qty\n2024-02-30,1\n");
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | EVAL s = TO_STRING(t) | KEEP s"))) {
            assertThat(getValuesList(response).get(0).get(0), not(equalTo("2024-02-29T00:00:00.000Z")));
        }
    }

    public void testDeclaredDateRefusesImpossibleDay() throws Exception {
        for (String day : List.of("2024-02-30", "2023-02-29", "2024-04-31", "2024-02-30T10:00:00Z")) {
            String ds = declaredDatetime("decl" + day.replaceAll("[^0-9]", ""), "t,qty\n" + day + ",1\n");
            Exception e = expectThrows(Exception.class, () -> {
                try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP t"))) {
                    fail("declared date read [" + day + "] as " + getValuesList(response));
                }
            });
            assertThat(e.getMessage(), containsString(day));
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
