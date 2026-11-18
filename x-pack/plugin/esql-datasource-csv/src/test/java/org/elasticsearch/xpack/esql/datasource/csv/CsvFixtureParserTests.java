/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class CsvFixtureParserTests extends ESTestCase {

    /**
     * A {@code date_nanos} or {@code date_micros} header names the unit its cells count in, and the fixture
     * generators write whatever this parser returns verbatim as that unit. Both cell forms therefore have to land
     * on the same number: a plain count, and an ISO instant scaled to the header's unit rather than truncated to
     * milliseconds.
     *
     * <p>The oracle is {@code data/date_nanos.csv}, which carries one instant twice — as
     * {@code 2023-10-23T13:55:01.543123456Z} under a {@code date_nanos} header and as
     * {@code 1698069301543123456} under a {@code long} one. Parsing the ISO form through
     * {@code tryParseDatetime} answers 1698069301543, losing six digits; rejecting it outright nulls a cell that
     * file writes legitimately, which is what a plain {@code tryParseLong} did.
     */
    public void testSubMillisecondHeadersAgreeOnPlainCountsAndIsoInstants() throws Exception {
        Path csv = createTempFile("date_units", ".csv");
        Files.writeString(csv, """
            id:integer,nanos:date_nanos,micros:date_micros
            1,2023-10-23T13:55:01.543123456Z,2023-10-23T13:55:01.543123Z
            2,1698069301543123456,1698069301543123
            3,not-a-date,not-a-date
            """, StandardCharsets.UTF_8);

        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv);
        assertEquals(3, result.rows().size());

        Object[] iso = result.rows().get(0);
        assertEquals("ISO nanos scale to the header's unit, not to millis", 1698069301543123456L, iso[1]);
        assertEquals("ISO micros scale to the header's unit, not to millis", 1698069301543123L, iso[2]);

        Object[] plain = result.rows().get(1);
        assertEquals("a plain count passes through unchanged", 1698069301543123456L, plain[1]);
        assertEquals("a plain count passes through unchanged", 1698069301543123L, plain[2]);

        Object[] junk = result.rows().get(2);
        assertNull("an unparseable cell is null, never a wrong instant", junk[1]);
        assertNull("an unparseable cell is null, never a wrong instant", junk[2]);
    }

    /**
     * The negative edge, where the seconds term alone underflows a long although the instant itself does not.
     * {@code Instant.getNano()} is never negative, so it adds back: 1677-09-21T00:12:43.5Z is
     * -9223372036500000000 nanoseconds, which fits, while its seconds term times 1e9 is -9223372037000000000, which
     * does not. Computing the product in a long first throws and the cell silently nulls.
     */
    public void testSubMillisecondHeaderKeepsAnInstantWhoseSecondsTermAloneUnderflows() throws Exception {
        Path csv = createTempFile("date_edge", ".csv");
        Files.writeString(csv, """
            id:integer,nanos:date_nanos
            1,1677-09-21T00:12:43.500000000Z
            """, StandardCharsets.UTF_8);

        CsvFixtureParser.CsvFixtureResult result = CsvFixtureParser.parseCsvFile(csv);
        assertEquals(-9223372036500000000L, result.rows().get(0)[1]);
    }
}
