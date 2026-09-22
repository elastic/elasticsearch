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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
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
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;

/**
 * A column declared {@code integer}, {@code long}, {@code unsigned_long} or {@code double} over text — a CSV cell, a JSON
 * string, a Parquet string — must read a value only when the text is a number in the file's own terms. Both reference
 * engines (ClickHouse, DuckDB) refuse a Java type suffix ({@code 10D}), a hexadecimal float ({@code 0x1p3}) and digits
 * outside ASCII (Arabic-Indic, fullwidth, Devanagari) in a declared numeric column. The default error mode fails the query on such a value.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalDeclaredNumericSpellingIT extends AbstractExternalDataSourceIT {

    private static final String ARABIC_INDIC_123 = "١٢٣";
    private static final String FULLWIDTH_123 = "１２３";
    private static final String DEVANAGARI_123 = "१२३";

    private static final List<String> FORMATS = List.of("csv", "ndjson", "parquet");

    private int seq;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    /** Registers a strict dataset over one file holding a single row whose {@code v} is the given text. */
    private String dataset(String format, String type, String text) throws Exception {
        String name = "ds" + (seq++);
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Path file;
        switch (format) {
            case "csv" -> {
                file = dir.resolve("d.csv");
                Files.writeString(file, "n,v\n1," + text + "\n", StandardCharsets.UTF_8);
            }
            case "ndjson" -> {
                file = dir.resolve("d.ndjson");
                Files.writeString(file, "{\"n\":1,\"v\":\"" + text + "\"}\n", StandardCharsets.UTF_8);
            }
            case "parquet" -> {
                file = dir.resolve("d.parquet");
                writeParquet(file, "message test { required int32 n; required binary v (UTF8); }", 1, 1000, (g, i) -> {
                    g.add("n", 1);
                    g.add("v", text);
                });
            }
            default -> throw new AssertionError(format);
        }
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("n", new DatasetFieldMapping("integer", null));
        props.put("v", new DatasetFieldMapping(type, null));
        return registerStrictDataset(name, StoragePath.fileUri(file), props, Map.of());
    }

    private void assertRefused(String type, String text) throws Exception {
        for (String format : FORMATS) {
            String ds = dataset(format, type, text);
            Exception e = expectThrows(Exception.class, () -> {
                try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | KEEP v"))) {
                    fail(format + " read [" + text + "] into a declared " + type + " column as " + getValuesList(response));
                }
            });
            // Every reader names the declared type in its refusal; the Parquet message does not always echo the value.
            assertThat(format + " " + type + " [" + text + "]", e.getMessage(), containsStringIgnoringCase("[" + type + "]"));
        }
    }

    private void assertReads(String type, String text, Object expected) throws Exception {
        for (String format : FORMATS) {
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset(format, type, text) + " | KEEP v"))) {
                assertThat(format + " " + type + " [" + text + "]", getValuesList(response), equalTo(List.of(List.of(expected))));
            }
        }
    }

    public void testDoubleRefusesJavaTypeSuffix() throws Exception {
        assertRefused("double", "10D");
        assertRefused("double", "14f");
    }

    public void testDoubleRefusesHexadecimalFloat() throws Exception {
        assertRefused("double", "0x1p3");
    }

    public void testIntegerRefusesJavaTypeSuffix() throws Exception {
        assertRefused("integer", "10D");
    }

    public void testLongRefusesHexadecimalFloat() throws Exception {
        assertRefused("long", "0x1p3");
    }

    public void testIntegerRefusesNonAsciiDigits() throws Exception {
        assertRefused("integer", ARABIC_INDIC_123);
    }

    public void testLongRefusesNonAsciiDigits() throws Exception {
        assertRefused("long", FULLWIDTH_123);
    }

    public void testUnsignedLongRefusesNonAsciiDigits() throws Exception {
        assertRefused("unsigned_long", DEVANAGARI_123);
    }

    /** Control: the same spellings already fail an unsigned_long column, whose parse is not Java's float grammar. */
    public void testUnsignedLongAlreadyRefusesJavaSpellings() throws Exception {
        assertRefused("unsigned_long", "10D");
        assertRefused("unsigned_long", "0x1p3");
    }

    /** Control: a double column already refuses non-ASCII digits, which the Java float grammar rejects. */
    public void testDoubleAlreadyRefusesNonAsciiDigits() throws Exception {
        assertRefused("double", ARABIC_INDIC_123);
    }

    /** Control: spellings both reference engines read as numbers must keep reading. */
    public void testOrdinaryNumbersStillRead() throws Exception {
        assertReads("double", "10.5", 10.5);
        assertReads("double", "1e5", 100000.0);
        assertReads("double", "+5", 5.0);
        assertReads("double", "NaN", Double.NaN);
        assertReads("integer", "123", 123);
        assertReads("long", "-42", -42L);
    }
}
