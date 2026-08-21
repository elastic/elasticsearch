/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

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

    /** Resolver: parquet has no schema_sample_size; csv and ndjson do. */
    private static FileDataSourceValidator.FormatConfigKeyResolver formatResolver() {
        return FileDataSourceValidator.FormatConfigKeyResolver.of(
            Map.of(
                "parquet",
                Set.of("optimized_reader", "late_materialization"),
                "csv",
                Set.of("schema_sample_size", "delimiter"),
                "ndjson",
                Set.of("schema_sample_size", "segment_size")
            ),
            Map.of(".parquet", "parquet", ".csv", "csv", ".ndjson", "ndjson")
        );
    }

    private static FileDataSourceValidator validatorWithResolver() {
        return new FileDataSourceValidator("file", (raw, consumed) -> null, Set.of("file")).withFormatConfigKeyResolver(
            formatResolver(),
            Set.of()
        );
    }

    public void testSchemaSampleSizeIsRejectedForParquetWhenInferredFromExtension() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.parquet", Map.of("schema_sample_size", "100"))
        );
        assertThat(e.getMessage(), containsString("schema_sample_size"));
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
        assertThat(e.getMessage(), containsString("schema_sample_size"));
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

    public void testSchemaSampleSizeIsAcceptedForExtensionlessResource() {
        // Format unknown at PUT time — stored tentatively so it can reach a text reader at query time.
        Map<String, Object> result = validatorWithResolver().validateDataset(
            Map.of(),
            "file:///data/events",
            Map.of("schema_sample_size", "100")
        );
        assertEquals(100, result.get("schema_sample_size"));
    }

    public void testParquetRejectionErrorNamesTheSettingNotTheFormat() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validatorWithResolver().validateDataset(Map.of(), "file:///data/events.parquet", Map.of("schema_sample_size", "50"))
        );
        assertThat(e.getMessage(), containsString("schema_sample_size"));
        // Not an unknown-format error (those carry a "set \"format\"" hint).
        assertThat(e.getMessage(), not(containsString("set \"format\"")));
    }
}
