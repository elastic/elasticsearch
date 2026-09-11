/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Direct coverage for the two behaviours this parser gained: nanosecond-resolution {@code date_nanos}
 * parsing, and header type canonicalisation.
 *
 * <p>Both were previously exercised only indirectly, through five integration suites, which is a poor
 * place to learn that a conversion is off by a factor of a million. Worse for {@code canonicalType}: no
 * fixture CSV in the corpus writes {@code datetime}, {@code dt} or {@code bool} in a header today, so the
 * fold is a no-op on real data and nothing anywhere proved it works. These tests are that proof.
 */
public class CsvFixtureParserTests extends ESTestCase {

    private Path csv(String contents) throws IOException {
        Path file = createTempDir().resolve("fixture.csv");
        Files.writeString(file, contents, StandardCharsets.UTF_8);
        return file;
    }

    private static Object cell(CsvFixtureParser.CsvFixtureResult result, int row, int column) {
        return result.rows().get(row)[column];
    }

    /**
     * The 10^6 bug: an ISO {@code date_nanos} cell used to be routed through the datetime path, which
     * returns epoch MILLIS, so every sub-millisecond fixture value was silently scaled by a million.
     * Two nanoseconds must stay two nanoseconds.
     */
    public void testIsoDateNanosKeepsNanosecondResolution() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date_nanos
            1970-01-01T00:00:00.000000002Z
            1970-01-01T00:00:00.002Z
            """));

        assertThat(cell(result, 0, 0), equalTo(2L));
        assertThat("a millisecond is a million nanoseconds, not a thousand", cell(result, 1, 0), equalTo(2_000_000L));
    }

    /** A whole-second ISO value, to pin the seconds-to-nanos multiplication itself. */
    public void testIsoDateNanosConvertsWholeSeconds() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date_nanos
            1970-01-01T00:00:03Z
            """));

        assertThat(cell(result, 0, 0), equalTo(3_000_000_000L));
    }

    /** A numeric {@code date_nanos} cell is already epoch nanos and must be taken verbatim. */
    public void testNumericDateNanosIsTakenVerbatim() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date_nanos
            1234567890123456789
            """));

        assertThat(cell(result, 0, 0), equalTo(1234567890123456789L));
    }

    /** An unparseable cell yields null rather than a wrong number. */
    public void testUnparseableDateNanosIsNull() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date_nanos
            not-a-timestamp
            """));

        assertThat(cell(result, 0, 0), nullValue());
    }

    /** {@code date} stays millis-resolution -- the fix must not have moved the ordinary datetime path. */
    public void testDateStillParsesAsEpochMillis() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date
            1970-01-01T00:00:00.002Z
            """));

        assertThat(cell(result, 0, 0), equalTo(2L));
    }

    /**
     * Outside the date_nanos window a nanosecond count does not fit a long. The multiplication this
     * method used to do wrapped silently and produced a plausible wrong instant -- the same shape as the
     * bug the method exists to fix. Unrepresentable must read as absent.
     */
    public void testDateNanosOutsideTheRepresentableWindowIsNullNotWrapped() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            ts:date_nanos
            2263-01-01T00:00:00.123456789Z
            1600-01-01T00:00:00.000000001Z
            """));

        assertThat("beyond 2262 has no nanosecond representation", cell(result, 0, 0), nullValue());
        assertThat("before 1678 has no nanosecond representation", cell(result, 1, 0), nullValue());
    }

    /**
     * The aliases canonicalType folds. No fixture in the corpus uses them, so without this test the fold
     * is asserted by nothing at all.
     */
    public void testHeaderTypeAliasesAreCanonicalised() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            a:datetime,b:dt,c:bool,d:keyword
            1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.002Z,true,x
            """));

        List<CsvFixtureParser.ColumnSpec> schema = result.schema();
        assertThat(schema.get(0).type(), equalTo("date"));
        assertThat(schema.get(1).type(), equalTo("date"));
        assertThat(schema.get(2).type(), equalTo("boolean"));
        assertThat("a type with no alias is left alone", schema.get(3).type(), equalTo("keyword"));
    }

    /**
     * Canonicalisation must reach the VALUE path too, not just the declared schema: a column headed
     * {@code datetime} has to parse as a date, otherwise the fold would rename the type while leaving the
     * cell unconverted.
     */
    public void testCanonicalisedAliasAlsoDrivesValueParsing() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            a:datetime,b:bool
            1970-01-01T00:00:00.002Z,true
            """));

        assertThat(cell(result, 0, 0), equalTo(2L));
        assertThat(cell(result, 0, 1), equalTo(Boolean.TRUE));
    }

    /** Header types are case-insensitive before canonicalisation. */
    public void testHeaderTypeIsLowercasedBeforeCanonicalisation() throws IOException {
        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv("""
            a:DateTime,b:BOOL
            1970-01-01T00:00:00.002Z,true
            """));

        assertThat(result.schema().get(0).type(), equalTo("date"));
        assertThat(result.schema().get(1).type(), equalTo("boolean"));
    }
}
