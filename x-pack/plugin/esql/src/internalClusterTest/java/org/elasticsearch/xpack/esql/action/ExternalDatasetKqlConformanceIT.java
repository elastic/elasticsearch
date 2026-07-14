/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/**
 * {@code FROM <dataset> | WHERE KQL("...")} is proven to select the same rows as the equivalent native ES|QL
 * {@code WHERE} on the same dataset. On a dataset the {@code KQL()} function has no Lucene scan to push to — the analyzer
 * parses the KQL against the dataset schema and translates it to the same ES|QL predicates a hand-written {@code WHERE}
 * would produce. Each probe asserts the KQL and its native equivalent select the identical id set.
 */
public class ExternalDatasetKqlConformanceIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 40;
    private static final Instant BASE = Instant.parse("2020-01-01T12:34:56Z");

    private String dataset;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private static int status(int i) {
        return 200 + (i % 3) * 100; // 200, 300, 400
    }

    private static String tag(int i) {
        return "t" + (i % 4); // t0..t3
    }

    private static long bytes(int i) {
        return i * 1000L;
    }

    private static String ts(int i) {
        return DateTimeFormatter.ISO_INSTANT.format(BASE.plus(Duration.ofDays(i)));
    }

    @Before
    public void loadDataset() throws Exception {
        StringBuilder csv = new StringBuilder("id:integer,status:integer,tags:keyword,bytes:long,ts:date\n");
        for (int i = 0; i < ROWS; i++) {
            csv.append(i)
                .append(',')
                .append(status(i))
                .append(',')
                .append(tag(i))
                .append(',')
                .append(bytes(i))
                .append(',')
                .append(ts(i))
                .append('\n');
        }
        Path csvFile = createTempDir().resolve("kql_conformance.csv");
        Files.writeString(csvFile, csv.toString(), StandardCharsets.UTF_8);
        dataset = registerStrictDataset("kql_ds", StoragePath.fileUri(csvFile), declaredColumns(), Map.of("format", "csv"));
    }

    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumns() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("status", new DatasetFieldMapping("integer", null));
        properties.put("tags", new DatasetFieldMapping("keyword", null));
        properties.put("bytes", new DatasetFieldMapping("long", null));
        properties.put("ts", new DatasetFieldMapping("date", null));
        return properties;
    }

    /** The KQL and the equivalent native ES|QL WHERE must select the identical id set over the same dataset. */
    private void assertKqlMatchesNative(String kql, String nativeWhere) {
        // Escape any double-quotes in the KQL so an inner quoted value (a date) does not close the ES|QL string.
        List<Object> viaKql = selectedIds("WHERE KQL(\"" + kql.replace("\"", "\\\"") + "\")");
        List<Object> viaNative = selectedIds("WHERE " + nativeWhere);
        assertEquals("KQL [" + kql + "] must select the same rows as WHERE [" + nativeWhere + "]", viaNative, viaKql);
    }

    private List<Object> selectedIds(String whereClause) {
        String query = "FROM " + dataset + " | " + whereClause + " | KEEP id | SORT id ASC";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            return getValuesList(response).stream().map(row -> row.get(0)).toList();
        }
    }

    public void testKeywordEquality() {
        assertKqlMatchesNative("tags: t2", "tags == \"t2\"");
    }

    public void testKeywordDisjunction() {
        assertKqlMatchesNative("tags: (t1 or t2)", "tags == \"t1\" OR tags == \"t2\"");
    }

    public void testNumericEquality() {
        assertKqlMatchesNative("status: 300", "status == 300");
    }

    public void testRange() {
        assertKqlMatchesNative("bytes >= 5000 and bytes < 25000", "bytes >= 5000 AND bytes < 25000");
    }

    public void testConjunction() {
        assertKqlMatchesNative("status: 300 and tags: t1", "status == 300 AND tags == \"t1\"");
    }

    public void testNegation() {
        assertKqlMatchesNative("not tags: t0", "NOT tags == \"t0\"");
    }

    public void testExists() {
        assertKqlMatchesNative("tags: *", "tags IS NOT NULL");
    }

    public void testDateRange() {
        // A coarse KQL date bound rounds to the edge of its unit, exactly as the translator's date path does; the native
        // comparison against the day's start selects the same rows (the data sits at 12:34:56 each day).
        assertKqlMatchesNative("ts >= \"2020-01-10\"", "ts >= \"2020-01-10T00:00:00Z\"::datetime");
    }

    /** A KQL clause over a field the dataset does not have matches nothing — there is no native equivalent to compare. */
    public void testUnknownFieldMatchesNothing() {
        assertThat(selectedIds("WHERE KQL(\"nope: x\")"), empty());
    }

    /** Fail-closed: a KQL construct outside the translatable subset (a wildcard) errors on a dataset — never unfiltered. */
    public void testUntranslatableKqlErrors() {
        String query = "FROM " + dataset + " | WHERE KQL(\"tags: t*\") | KEEP id";
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query), TIMEOUT).close());
        assertThat(ex.getMessage() + ex.getCause(), org.hamcrest.Matchers.containsString("not supported on federated data sources"));
    }
}
