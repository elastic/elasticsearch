/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code EXTERNAL} TSV with literal {@code "} bytes, exercising the stream-only (gzip), segmentable
 * uncompressed, and single-thread parsing paths. A mid-field {@code "} must be treated as a literal,
 * so {@code STATS COUNT(*)} returns the exact row count on every path.
 */
public class ExternalTsvLiteralQuotesIT extends AbstractEsqlIntegTestCase {

    /**
     * Schema sample size used by the cap-tripping tests. Kept small so the unclosed quote can sit just
     * past the sample window: schema inference reads only the first {@code SCHEMA_SAMPLE} clean rows and
     * succeeds, and the malformed record is reached later, on the read/cap path — which is what we test.
     */
    private static final int SCHEMA_SAMPLE = 5_000;

    /** Row whose middle field opens an unclosed quote in {@link QuoteShape#UNCLOSED_LEADING}; past {@link #SCHEMA_SAMPLE}. */
    private static final int UNCLOSED_QUOTE_ROW = 10_000;

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(GzipDataSourcePlugin.class);
        return plugins;
    }

    /**
     * Deterministic guard: row 0 has one mid-field {@code "}, the rest are quote-free, so a quote-toggle
     * scanner finds no boundary and the grow loop hits a low {@code max_record_size}. Fails without the
     * fix; with it the {@code "} is literal and the count is exact.
     */
    public void testGzipStreamOnlyLeadingQuoteIsLiteral() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 100_000;
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.DENSE);
        try {
            assertCount(file, pragmas(1, null), rows);
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

    /**
     * A field past the schema-sample window opens a quote that never closes. Schema inference samples only
     * the earlier clean rows and succeeds, so the failure lands on the read/cap path: the unclosed quote
     * makes the record scanner grow until it trips {@code max_record_size}, raising an {@code IOException}.
     * That is undecodable input, so the query must fail HTTP 400 — not a 500 from the error class being lost
     * in a RuntimeException wrap on the parallel path.
     */
    public void testGzipStreamOnlyUnclosedLeadingQuoteTripsCapAs400() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 150_000; // a few MB uncompressed — comfortably past the 1mb cap once the quote opens
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.UNCLOSED_LEADING);
        try {
            assertCapTrippedAs400(file, pragmas(4, "1mb"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Twin of {@link #testGzipStreamOnlyUnclosedLeadingQuoteTripsCapAs400} on the segmentable uncompressed
     * parallel path ({@code ParallelParsingCoordinator}): the same field-leading unclosed quote trips the
     * cap and must surface HTTP 400. Guards the second coordinator's rewrap site, not just the streaming one.
     */
    public void testUncompressedParallelUnclosedLeadingQuoteTripsCapAs400() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        int rows = 250_000; // > 2 chunks so the parallel splitter engages, and > 1mb so the cap is reached
        Path file = writeTsv(rows, Codec.NONE, QuoteShape.UNCLOSED_LEADING);
        try {
            assertCapTrippedAs400(file, pragmas(4, "1mb"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Companion to the cap tests on the planning path: when the unclosed quote lands <em>inside</em> the
     * schema-sample window, schema inference itself fails to decode the row. That is still a data error, so
     * the query must fail 400 — not the 500 produced when the resolver buries the client-class failure in an
     * {@code ExecutionException} from the schema cache and relabels it a server fault.
     */
    public void testMalformedSchemaSampleFailsAs400() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        int rows = 200; // tiny: the quote sits at row 0, well inside the default sample window
        Path file = writeTsv(rows, Codec.GZIP, QuoteShape.UNCLOSED_AT_START);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(file) + "\" WITH {\"header_row\": false} | STATS c = COUNT(*)";
            Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query), TimeValue.timeValueMinutes(2)).close());
            assertThat("undecodable schema sample is a 400, not a 500", ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /** Runs {@code STATS COUNT(*)} expecting the read to trip the record cap, and asserts an actionable 400. */
    private void assertCapTrippedAs400(Path file, QueryPragmas pragmas) {
        String query = "EXTERNAL \""
            + StoragePath.fileUri(file)
            + "\" WITH {\"header_row\": false, \"schema_sample_size\": "
            + SCHEMA_SAMPLE
            + "} | STATS c = COUNT(*)";
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(pragmas), TimeValue.timeValueMinutes(2)).close()
        );
        assertThat("a record we cannot decode is a 400, not a 500", ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat("the failure must stay actionable through classification", e.getMessage(), containsString("max_record_size"));
    }

    private enum QuoteShape {
        /** Row 0 has one literal mid-field quote, the rest are quote-free (forces a no-boundary chunk). */
        SINGLE_LEADING,
        /** Every row has an even number of literal mid-field quotes. */
        DENSE,
        /** One row past the schema-sample window opens a quote that never closes — undecodable, trips the cap. */
        UNCLOSED_LEADING,
        /** Row 0 opens a quote that never closes — undecodable inside the schema sample, fails resolution. */
        UNCLOSED_AT_START
    }

    private QueryPragmas pragmas(int parsingParallelism, String maxRecordSize) {
        Settings.Builder b = Settings.builder().put(QueryPragmas.PARSING_PARALLELISM.getKey(), parsingParallelism);
        if (maxRecordSize != null) {
            b.put(QueryPragmas.MAX_RECORD_SIZE.getKey(), maxRecordSize);
        }
        return new QueryPragmas(b.build());
    }

    private void assertCount(Path file, QueryPragmas pragmas, int expectedRows) {
        String query = "EXTERNAL \"" + StoragePath.fileUri(file) + "\" WITH {\"header_row\": false} | STATS c = COUNT(*)";
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
                    // open a quote past the schema-sample window so it is reached on the read path, not sampling
                    case UNCLOSED_LEADING -> i == UNCLOSED_QUOTE_ROW ? "\"open" : "a" + i;
                    case UNCLOSED_AT_START -> i == 0 ? "\"open" : "a" + i; // quote inside the schema sample
                };
                w.write("f0_" + i + "\t" + mid + "\tf2_" + i + "\n");
            }
        }
        return file;
    }
}
