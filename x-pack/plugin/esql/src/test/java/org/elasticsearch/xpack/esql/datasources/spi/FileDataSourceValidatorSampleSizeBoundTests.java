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
}
