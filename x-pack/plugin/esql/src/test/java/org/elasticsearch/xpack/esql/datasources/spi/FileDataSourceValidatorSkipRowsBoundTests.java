/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.DecompressionCodecRegistry;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the registration-time {@code skip_rows} bound against the CSV reader's cap so the two cannot drift.
 * Parquet must keep rejecting the key; CSV/TSV admit {@code 0} through the cap inclusive.
 */
public class FileDataSourceValidatorSkipRowsBoundTests extends ESTestCase {

    public void testMaxMatchesReaderConstant() {
        assertEquals(CsvFormatReader.SKIP_ROWS_MAX, FileDataSourceValidator.SKIP_ROWS_MAX);
    }

    public void testZeroIsAcceptedForCsv() {
        Map<String, Object> accepted = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events.csv",
            Map.of("skip_rows", "0")
        );
        assertEquals(0, accepted.get("skip_rows"));
    }

    public void testCapIsAcceptedForCsv() {
        Map<String, Object> accepted = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events.csv",
            Map.of("skip_rows", String.valueOf(CsvFormatReader.SKIP_ROWS_MAX))
        );
        assertEquals(CsvFormatReader.SKIP_ROWS_MAX, accepted.get("skip_rows"));
    }

    public void testJustAboveCapIsRejected() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(
                Map.of(),
                "file:///data/events.csv",
                Map.of("skip_rows", String.valueOf(CsvFormatReader.SKIP_ROWS_MAX + 1))
            )
        );
        assertThat(e.getMessage(), containsString("skip_rows"));
        assertThat(e.getMessage(), containsString("1000"));
    }

    public void testNegativeIsRejected() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.csv", Map.of("skip_rows", "-1"))
        );
        assertThat(e.getMessage(), containsString("skip_rows"));
    }

    public void testNonIntegerIsRejected() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.csv", Map.of("skip_rows", "two"))
        );
        assertThat(e.getMessage(), containsString("skip_rows"));
        assertThat(e.getMessage(), containsString("must be a number"));
    }

    public void testRejectedForParquetWhenInferredFromExtension() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.parquet", Map.of("skip_rows", "2"))
        );
        assertEquals(List.of(FileDataSourceValidator.notSupportedByFormatError("skip_rows", "parquet")), e.validationErrors());
    }

    public void testAcceptedForTsvWhenInferredFromExtension() {
        Map<String, Object> accepted = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events.tsv",
            Map.of("skip_rows", "2")
        );
        assertEquals(2, accepted.get("skip_rows"));
    }

    private static FileDataSourceValidator.FormatConfigKeyResolver formatResolver() {
        return FileDataSourceValidator.FormatConfigKeyResolver.of(
            Map.of("parquet", Set.of(), "csv", Set.of("skip_rows", "header_row"), "tsv", Set.of("skip_rows", "header_row")),
            Map.of(".parquet", "parquet", ".csv", "csv", ".tsv", "tsv")
        );
    }

    private static FileDataSourceValidator validatorWithResolver() {
        return new FileDataSourceValidator("file", (raw, consumed) -> null, Set.of("file")).withFormatConfigKeyResolver(formatResolver())
            .withFormatReaderRegistry(formatRegistry());
    }

    private static FormatReaderRegistry formatRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        FormatReader tsv = mock(FormatReader.class);
        when(tsv.formatName()).thenReturn("tsv");
        when(tsv.fileExtensions()).thenReturn(List.of(".tsv"));
        when(tsv.supportsWholeFileCompression()).thenReturn(true);
        FormatReader parquet = mock(FormatReader.class);
        when(parquet.formatName()).thenReturn("parquet");
        when(parquet.fileExtensions()).thenReturn(List.of(".parquet"));
        when(parquet.supportsWholeFileCompression()).thenReturn(false);
        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new DecompressionCodec() {
            @Override
            public String name() {
                return "gzip";
            }

            @Override
            public List<String> extensions() {
                return List.of(".gz");
            }

            @Override
            public InputStream decompress(InputStream raw) {
                return raw;
            }
        });
        FormatReaderRegistry registry = new FormatReaderRegistry(codecs);
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");
        registry.registerLazy("tsv", (s, bf) -> tsv, Settings.EMPTY, null);
        registry.registerExtension(".tsv", "tsv");
        registry.registerLazy("parquet", (s, bf) -> parquet, Settings.EMPTY, null);
        registry.registerExtension(".parquet", "parquet");
        return registry;
    }
}
