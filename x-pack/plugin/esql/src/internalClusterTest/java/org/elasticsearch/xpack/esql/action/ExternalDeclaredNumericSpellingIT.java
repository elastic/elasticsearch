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
import java.util.ArrayList;
import java.util.Arrays;
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
 * string, a Parquet string — must refuse a Java type suffix ({@code 10D}), a hexadecimal float ({@code 0x1p3}) and digits
 * outside ASCII. The default error mode fails the query on such a value.
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
        return dataset(format, type, List.of(text), Map.of());
    }

    /** Registers a strict dataset over one file with a row per text: {@code n} is the 1-based row number, {@code v} the text. */
    private String dataset(String format, String type, List<String> texts, Map<String, Object> settings) throws Exception {
        String name = "ds" + (seq++);
        Path dir = createTempDir().resolve(name);
        Files.createDirectories(dir);
        Path file;
        switch (format) {
            case "csv" -> {
                file = dir.resolve("d.csv");
                StringBuilder body = new StringBuilder("n,v\n");
                for (int i = 0; i < texts.size(); i++) {
                    body.append(i + 1).append(',').append(texts.get(i)).append('\n');
                }
                Files.writeString(file, body.toString(), StandardCharsets.UTF_8);
            }
            case "ndjson" -> {
                file = dir.resolve("d.ndjson");
                StringBuilder body = new StringBuilder();
                for (int i = 0; i < texts.size(); i++) {
                    body.append("{\"n\":").append(i + 1).append(",\"v\":\"").append(texts.get(i)).append("\"}\n");
                }
                Files.writeString(file, body.toString(), StandardCharsets.UTF_8);
            }
            case "parquet" -> {
                file = dir.resolve("d.parquet");
                writeParquet(file, "message test { required int32 n; required binary v (UTF8); }", texts.size(), 1000, (g, i) -> {
                    g.add("n", i + 1);
                    g.add("v", texts.get(i));
                });
            }
            default -> throw new AssertionError(format);
        }
        LinkedHashMap<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("n", new DatasetFieldMapping("integer", null));
        props.put("v", new DatasetFieldMapping(type, null));
        return registerStrictDataset(name, StoragePath.fileUri(file), props, settings);
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

    /** Control: ordinary spellings keep reading. */
    public void testOrdinaryNumbersStillRead() throws Exception {
        assertReads("double", "10.5", 10.5);
        assertReads("double", "1e5", 100000.0);
        assertReads("double", "+5", 5.0);
        assertReads("double", "NaN", Double.NaN);
        assertReads("integer", "123", 123);
        assertReads("long", "-42", -42L);
    }

    public void testNullFieldNullsJavaOnlyDoubleSpellings() throws Exception {
        for (String format : FORMATS) {
            String ds = dataset(format, "double", List.of("10D", "1.5", "0x1p3", "٣"), Map.of("error_mode", "null_field"));
            try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT n | KEEP n, v"))) {
                assertThat(
                    format,
                    getValuesList(response),
                    equalTo(List.of(Arrays.asList(1, null), Arrays.asList(2, 1.5), Arrays.asList(3, null), Arrays.asList(4, null)))
                );
            }
        }
    }

    public void testSkipRowDropsRowsWithJavaOnlyDoubleSpellings() throws Exception {
        for (String format : FORMATS) {
            String ds = dataset(format, "double", List.of("10D", "1.5", "0x1p3", "2.5"), Map.of("error_mode", "skip_row"));
            try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT n | KEEP n, v"))) {
                assertThat(format, getValuesList(response), equalTo(List.of(List.of(2, 1.5), List.of(4, 2.5))));
            }
        }
    }

    /**
     * Random declared type, format and error mode over rows that alternate between a plain number and a random
     * Java-only spelling. The plain rows must always read; the others must be nulled or dropped, never read as a number.
     */
    public void testLenientModesNeverReadRandomJavaOnlySpellingsAsNumbers() throws Exception {
        String format = randomFrom(FORMATS);
        String type = randomFrom("integer", "long", "unsigned_long", "double");
        boolean nullField = randomBoolean();
        int rows = randomIntBetween(2, 40);
        List<String> texts = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            texts.add(i % 2 == 0 ? Integer.toString(randomIntBetween(0, 1_000_000)) : randomJavaOnlySpelling());
        }
        String ds = dataset(format, type, texts, Map.of("error_mode", nullField ? "null_field" : "skip_row"));

        List<List<Object>> expected = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            if (i % 2 == 0) {
                expected.add(Arrays.asList(i + 1, Double.valueOf(texts.get(i))));
            } else if (nullField) {
                expected.add(Arrays.asList(i + 1, null));
            }
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + ds + " | SORT n | KEEP n, v"))) {
            List<List<Object>> actual = new ArrayList<>();
            for (List<Object> row : getValuesList(response)) {
                actual.add(Arrays.asList(row.get(0), row.get(1) == null ? null : ((Number) row.get(1)).doubleValue()));
            }
            assertThat(format + " " + type + " " + (nullField ? "null_field" : "skip_row") + " " + texts, actual, equalTo(expected));
        }
    }

    private static String randomJavaOnlySpelling() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> randomIntBetween(0, 1000) + randomFrom("d", "D", "f", "F");
            case 1 -> {
                StringBuilder digits = new StringBuilder();
                for (int i = randomIntBetween(1, 5); i > 0; i--) {
                    digits.appendCodePoint(randomFrom(0x0661, 0x0662, 0x0663, 0x0967, 0x0968, 0xFF11, 0xFF12, 0xFF13));
                }
                yield digits.toString();
            }
            case 2 -> "0x" + Integer.toHexString(randomIntBetween(1, 255)) + "p" + randomIntBetween(0, 5);
            default -> throw new AssertionError();
        };
    }
}
