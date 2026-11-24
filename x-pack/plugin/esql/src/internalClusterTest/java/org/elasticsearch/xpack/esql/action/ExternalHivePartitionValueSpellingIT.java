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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Hive partition folders whose values Java's number parsers accept. DuckDB's {@code hive_partitioning} and ClickHouse's
 * {@code use_hive_partitioning} both type {@code code=10D} as a string and return the folder text.
 */
public class ExternalHivePartitionValueSpellingIT extends AbstractExternalDataSourceIT {

    private static final String ARABIC_INDIC_123 = "١٢٣";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    private String dataset(String name, String... codes) throws IOException {
        Path root = createTempDir().resolve(name);
        int id = 0;
        for (String code : codes) {
            Path dir = root.resolve("code=" + code);
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("f.csv"), "id\n" + (++id) + "\n", StandardCharsets.UTF_8);
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        return registerDataset(name, glob, Map.of("partition_detection", "hive"));
    }

    private List<List<Object>> codes(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT id | KEEP code"))) {
            return getValuesList(response);
        }
    }

    public void testTypeSuffixedFolderValuesStayText() throws IOException {
        assertThat(codes(dataset("hive_suffix", "10D", "12d")), equalTo(List.of(List.of("10D"), List.of("12d"))));
    }

    public void testHexadecimalFloatFolderValueStaysText() throws IOException {
        assertThat(codes(dataset("hive_hex", "0x1p3")), equalTo(List.of(List.of("0x1p3"))));
    }

    public void testNonAsciiDigitFolderValueStaysText() throws IOException {
        assertThat(codes(dataset("hive_arabic", ARABIC_INDIC_123)), equalTo(List.of(List.of(ARABIC_INDIC_123))));
    }

    /** Control: ASCII whole-number folders still type as integers, as DuckDB types them. */
    public void testAsciiWholeNumberFoldersStillNumeric() throws IOException {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset("hive_ascii", "10", "20") + " | STATS s = SUM(code)"))) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(30L));
        }
    }

    /** Template detection types its values through the same two methods, so it must answer the same. */
    public void testTypeSuffixedTemplateFolderValuesStayText() throws IOException {
        Path root = createTempDir().resolve("tmpl_suffix");
        int id = 0;
        for (String code : List.of("10D", "12d")) {
            Path dir = root.resolve(code);
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("f.csv"), "id\n" + (++id) + "\n", StandardCharsets.UTF_8);
        }
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("tmpl_suffix", glob, Map.of("partition_detection", "template", "partition_path", "{code}"));
        assertThat(codes(dataset), equalTo(List.of(List.of("10D"), List.of("12d"))));
    }
}
