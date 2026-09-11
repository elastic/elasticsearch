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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
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
 * The registration-time bound on {@code schema_sample_size} must admit the value the readers use by default.
 * It did not: the validator capped at 1000 while both text readers default to 20000, so a user could not set
 * any value from 1001 upwards — including, absurdly, the default itself. Asking explicitly for the behaviour
 * you already get by saying nothing was a validation failure.
 *
 * <p>Pinning the bound against the reader's own constant, rather than against a literal, is the point: a
 * hardcoded expectation here would drift the same way the original bound did the moment a reader default moves.
 */
public class FileDataSourceValidatorSampleSizeBoundTests extends ESTestCase {

    private static FileDataSourceValidator validator() {
        return new FileDataSourceValidator("file", (raw, consumed) -> null, Set.of("file"));
    }

    public void testEveryReaderDefaultIsAcceptedAtRegistration() {
        // Both readers, not just one: pinning a single default leaves the other free to move above the bound and
        // silently reopen the gap while this test stays green.
        for (int readerDefault : new int[] { NdJsonFormatReader.DEFAULT_SCHEMA_SAMPLE_SIZE, CsvFormatReader.DEFAULT_SCHEMA_SAMPLE_SIZE }) {
            Map<String, Object> accepted = validator().validateDataset(
                Map.of(),
                "file:///data/events.ndjson",
                Map.of("schema_sample_size", String.valueOf(readerDefault))
            );
            assertEquals(readerDefault, accepted.get("schema_sample_size"));
        }
    }

    public void testAValueJustAboveTheOldBoundIsAccepted() {
        Map<String, Object> accepted = validator().validateDataset(
            Map.of(),
            "file:///data/events.ndjson",
            Map.of("schema_sample_size", "1001")
        );

        assertEquals(1001, accepted.get("schema_sample_size"));
    }

    public void testNonPositiveIsStillRejected() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator().validateDataset(Map.of(), "file:///data/events.ndjson", Map.of("schema_sample_size", "0"))
        );
        assertThat(e.getMessage(), containsString("schema_sample_size"));
    }

    // ---- Format-scoped rejection / acceptance (resolver-aware tests) ----

    /**
     * Resolver: parquet has no schema_sample_size; csv and ndjson do. Parquet claims no dataset keys at
     * all, matching the real {@code FormatSpec} (its former tuning keys were removed).
     */
    private static FileDataSourceValidator.FormatConfigKeyResolver formatResolver() {
        return FileDataSourceValidator.FormatConfigKeyResolver.of(
            Map.of(
                "parquet",
                Set.of(),
                "csv",
                Set.of("schema_sample_size", "delimiter"),
                "ndjson",
                Set.of("schema_sample_size", "segment_size")
            ),
            Map.of(".parquet", "parquet", ".csv", "csv", ".ndjson", "ndjson")
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
        FormatReader ndjson = mock(FormatReader.class);
        when(ndjson.formatName()).thenReturn("ndjson");
        when(ndjson.fileExtensions()).thenReturn(List.of(".ndjson"));
        when(ndjson.supportsWholeFileCompression()).thenReturn(true);
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
        registry.registerLazy("ndjson", (s, bf) -> ndjson, Settings.EMPTY, null);
        registry.registerExtension(".ndjson", "ndjson");
        registry.registerLazy("parquet", (s, bf) -> parquet, Settings.EMPTY, null);
        registry.registerExtension(".parquet", "parquet");
        return registry;
    }

    public void testSchemaSampleSizeIsRejectedForParquetWhenInferredFromExtension() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.parquet", Map.of("schema_sample_size", "100"))
        );
        assertEquals(List.of(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "parquet")), e.validationErrors());
    }

    public void testSchemaSampleSizeIsRejectedForParquetWhenFormatExplicit() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(
                Map.of(),
                "file:///data/events",
                Map.of("format", "parquet", "schema_sample_size", "100")
            )
        );
        assertEquals(List.of(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "parquet")), e.validationErrors());
    }

    public void testSchemaSampleSizeIsAcceptedForCsvWhenInferredFromExtension() {
        Map<String, Object> result = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events.csv",
            Map.of("schema_sample_size", "100")
        );
        assertEquals(100, result.get("schema_sample_size"));
    }

    public void testSchemaSampleSizeIsAcceptedForNdjsonWhenInferredFromExtension() {
        Map<String, Object> result = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events.ndjson",
            Map.of("schema_sample_size", "100")
        );
        assertEquals(100, result.get("schema_sample_size"));
    }

    public void testSchemaSampleSizeIsRejectedForExtensionlessResource() {
        // Format unknown at PUT time: like any other format-specific key, the value is not stored
        // tentatively — the user is told to pin `format`. Pre-tightening datasets are tolerated at
        // query time instead (FileSourceFactory#LEGACY_VOCABULARY_KEYS).
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events", Map.of("schema_sample_size", "100"))
        );
        assertEquals(
            List.of(FileDataSourceValidator.cannotDetermineFormatError("file:///data/events", Set.of("schema_sample_size"))),
            e.validationErrors()
        );
    }

    public void testParquetRejectionErrorNamesTheSettingAndTheFormat() {
        // Documented vocabulary that does not apply to Parquet: the message must say exactly that,
        // not "unknown setting" or a pin-the-format hint.
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.parquet", Map.of("schema_sample_size", "50"))
        );
        assertEquals(List.of("[schema_sample_size] is not supported for format [parquet]"), e.validationErrors());
    }
}
