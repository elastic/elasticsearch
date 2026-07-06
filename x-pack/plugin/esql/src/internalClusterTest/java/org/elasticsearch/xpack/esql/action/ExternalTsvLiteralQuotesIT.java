/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code FROM <dataset>} TSV with literal {@code "} bytes, exercising the stream-only (gzip), segmentable
 * uncompressed, and single-thread parsing paths. A mid-field {@code "} must be treated as a literal,
 * so {@code STATS COUNT(*)} returns the exact row count on every path.
 */
public class ExternalTsvLiteralQuotesIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    /**
     * Deterministic guard: row 0 has one mid-field {@code "}, the rest are quote-free, so a quote-toggle
     * scanner finds no boundary and the grow loop hits a low {@code max_record_size}. Fails without the
     * fix; with it the {@code "} is literal and the count is exact.
     */
    public void testGzipStreamOnlyLeadingQuoteIsLiteral() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 150_000;
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.SINGLE_LEADING);
        try {
            // parsing_parallelism > 1 forces the streaming-parallel segmentator path; max_record_size
            // caps the per-record grow so the pre-fix runaway is reached at a small, fast fixture.
            assertCount(file, pragmas(4, "1mb"), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Stream-only path with dense literal quotes throughout the data, default cap. */
    public void testGzipStreamOnlyDenseLiteralQuotes() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 150_000;
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.DENSE);
        try {
            assertCount(file, pragmas(4, null), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Segmentable uncompressed parallel path ({@code ParallelParsingCoordinator}) with dense quotes. */
    public void testUncompressedParallelDenseLiteralQuotes() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 250_000; // large enough (> 2 chunks) that the parallel splitter engages
        Path file = writeTsv(rows, Codec.NONE, QuoteShape.DENSE);
        try {
            assertCount(file, pragmas(4, null), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Single-thread fallback (direct {@code reader.read}) with dense quotes. */
    public void testSingleThreadFallbackDenseLiteralQuotes() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 100_000;
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.DENSE);
        try {
            assertCount(file, pragmas(1, null), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Field-leading quotes through the streaming (gzip) path. The low {@code max_record_size} cap
     * makes a regression to a quoting {@code .tsv} default fail fast (glued records trip the cap)
     * instead of scanning the full default window.
     */
    public void testGzipStreamOnlyFieldLeadingQuoteIsData() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 150_000;
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.FIELD_LEADING);
        try {
            assertCount(file, pragmas(4, "1mb"), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Field-leading quotes through the segmentable uncompressed parallel path. */
    public void testUncompressedParallelFieldLeadingQuoteIsData() throws Exception {
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 250_000; // large enough (> 2 chunks) that the parallel splitter engages
        Path file = writeTsv(rows, Codec.NONE, QuoteShape.FIELD_LEADING);
        try {
            assertCount(file, pragmas(4, "1mb"), rows);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private enum Codec {
        NONE(".tsv"),
        GZIP(".tsv.gz");

        final String ext;

        Codec(String ext) {
            this.ext = ext;
        }
    }

    private enum QuoteShape {
        /** Row 0 has one literal mid-field quote, the rest are quote-free (forces a no-boundary chunk). */
        SINGLE_LEADING,
        /** Every row has an even number of literal mid-field quotes. */
        DENSE,
        /**
         * Every 100th row's field STARTS with a literal {@code "} — the ClickBench TSV shape. A
         * quoting mode opens an unclosed quoted field here and glues records to the size cap;
         * the plain {@code .tsv} baseline reads the quote as data.
         */
        FIELD_LEADING
    }

    private QueryPragmas pragmas(int parsingParallelism, String maxRecordSize) {
        Settings.Builder b = Settings.builder().put(QueryPragmas.PARSING_PARALLELISM.getKey(), parsingParallelism);
        if (maxRecordSize != null) {
            b.put(QueryPragmas.MAX_RECORD_SIZE.getKey(), maxRecordSize);
        }
        return new QueryPragmas(b.build());
    }

    private void assertCount(Path file, QueryPragmas pragmas, int expectedRows) {
        String dataset = registerDataset("tsv_quotes", StoragePath.fileUri(file), Map.of("header_row", false));
        String query = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(query).pragmas(pragmas), TimeValue.timeValueMinutes(2))) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(1));
            assertThat(columns.get(0).name(), equalTo("c"));

            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            assertThat(((Number) values.get(0).get(0)).longValue(), equalTo((long) expectedRows));
        }
    }

    private Path writeTsv(int rows, Codec codec, QuoteShape shape) throws Exception {
        Path file = createTempDir().resolve("quotes" + codec.ext);
        OutputStream os = Files.newOutputStream(file);
        try (
            OutputStream sink = codec == Codec.GZIP ? new GZIPOutputStream(os) : os;
            Writer w = new OutputStreamWriter(sink, StandardCharsets.UTF_8)
        ) {
            for (int i = 0; i < rows; i++) {
                String mid = switch (shape) {
                    case SINGLE_LEADING -> i == 0 ? "a\"b" : "a" + i; // one literal quote, only on row 0
                    case DENSE -> "x\"y\"z"; // two literal quotes every row
                    case FIELD_LEADING -> i % 100 == 0 ? "\"starts" + i : "a" + i; // field-leading quote, never closed
                };
                w.write("f0_" + i + "\t" + mid + "\tf2_" + i + "\n");
            }
        }
        return file;
    }
}
