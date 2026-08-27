/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.util.DateUtils;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * The discriminator is fed by two different parsers in production &mdash; the NDJSON rail's
 * {@code strict_date_optional_time} {@link DateFormatter} and the CSV rail's
 * {@link DateUtils#asDateTime} &mdash; which hand back two different {@link TemporalAccessor}
 * implementations with different notions of which fields are supported. Every case here is asserted
 * through both, because a rule that holds on one accessor and not the other is a rail-dependent
 * answer to a question that is supposed to be decided once.
 */
public class TemporalInferenceTests extends ESTestCase {

    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME = DateFormatter.forPattern("strict_date_optional_time");

    public void testSubMillisecondComponentForces() {
        assertForcesOnBothRails("2023-10-23T12:15:03.360103847Z");
        assertForcesOnBothRails("2023-10-23T12:15:03.3601Z");
        assertForcesOnBothRails("2023-10-23T12:15:03.000000001Z");
    }

    public void testMillisecondExactFractionDoesNotForce() {
        assertDoesNotForceOnBothRails("2023-10-23T12:15:03.360Z");
        assertDoesNotForceOnBothRails("2023-10-23T12:15:03Z");
        // The trailing-zero rendering: nine digits of text, but millisecond-exact as a value. This is
        // the pin for choosing a value-based discriminator over a text-based one — a text rule would
        // retype this whole population for no gain.
        assertDoesNotForceOnBothRails("2023-10-23T12:15:03.360000000Z");
        assertDoesNotForceOnBothRails("2023-10-23T12:15:03.000000000Z");
    }

    public void testDateOnlyDoesNotForce() {
        // No time-of-day at all: NANO_OF_SECOND is unsupported, and there is no fraction to lose.
        assertDoesNotForceOnBothRails("2023-10-23");
    }

    public void testPreEpochDoesNotForce() {
        // date_nanos has no pre-1970 representation whatsoever, so datetime — which keeps the value
        // to millisecond precision — is the better reading even though precision is lost.
        assertDoesNotForceOnBothRails("1969-12-31T23:59:59.999999999Z");
        assertDoesNotForceOnBothRails("1900-01-01T00:00:00.123456789Z");
    }

    public void testBeyondCeilingDoesNotForce() {
        assertDoesNotForceOnBothRails("2262-04-11T23:47:16.854775808Z");
        assertDoesNotForceOnBothRails("2263-01-01T00:00:00.123456789Z");
        assertDoesNotForceOnBothRails("9999-01-01T00:00:00.123456789Z");
    }

    public void testWindowBoundariesForce() {
        // The first and last representable date_nanos values that carry sub-millisecond digits.
        assertForcesOnBothRails("1970-01-01T00:00:00.000000001Z");
        assertForcesOnBothRails("2262-04-11T23:47:16.854775807Z");
    }

    public void testOffsetCrossingEpochBoundary() {
        // Local year 1969 but the instant is post-epoch: the year pre-filter cannot decide these, so
        // they exercise the exact-check arm in both directions.
        assertForcesOnBothRails("1969-12-31T23:00:00.000000001-05:00");
        // Local year 1970 but the instant is pre-epoch.
        assertDoesNotForceOnBothRails("1970-01-01T00:30:00.000000001+01:00");
    }

    public void testYearPreFilterAgreesWithExactCheckAcrossBoundaryYears() {
        // Every value the year pre-filter answers outright must get the same answer from the exact
        // check, or the fast path is lying. Sweep the boundary years and their neighbours.
        for (String date : new String[] {
            "1968-06-01T12:00:00",
            "1969-06-01T12:00:00",
            "1970-06-01T12:00:00",
            "1971-06-01T12:00:00",
            "2261-06-01T12:00:00",
            "2262-01-01T12:00:00",
            "2263-06-01T12:00:00" }) {
            String value = date + ".123456789Z";
            boolean expected = date.compareTo("1970-01-01") >= 0 && date.compareTo("2262-04-11") <= 0;
            assertEquals(value + " (ndjson rail)", expected, TemporalInference.forcesDateNanos(ndjsonParse(value)));
            assertEquals(value + " (csv rail)", expected, TemporalInference.forcesDateNanos(csvParse(value)));
        }
    }

    /**
     * The NDJSON fast parser leaves the offset unresolved for a named zone, and reading such an
     * accessor's {@code INSTANT_SECONDS} silently assumes UTC — which near the epoch floor can place
     * the instant on the wrong side of the window. The discriminator must decline rather than guess:
     * this value is really 1969-12-31T23:00:00.000000001Z, which date_nanos cannot hold.
     */
    public void testNamedZoneAtWindowBoundaryDoesNotForce() {
        assertFalse(TemporalInference.forcesDateNanos(ndjsonParse("1970-01-01T00:00:00.000000001Europe/Paris")));
    }

    public void testNamedZoneWellInsideWindowStillForces() {
        // Away from the boundary years the zone cannot move the instant out of the window, so the
        // conservative decline above costs nothing here.
        assertTrue(TemporalInference.forcesDateNanos(ndjsonParse("2023-10-23T12:15:03.360103847Europe/Paris")));
    }

    /**
     * A boundary-year timestamp with no zone and no offset at all. The exact-window arm must read it
     * as UTC — which is what the decoders will do with it too — rather than declining the way it
     * declines a named zone it cannot resolve.
     */
    public void testZonelessBoundaryYearForces() {
        assertForcesOnBothRails("1970-01-01T00:00:00.000000001");
    }

    /**
     * Inside the ceiling year but past the ceiling instant: the year pre-filter cannot decide it and
     * the exact check has to reject on the seconds comparison, not on the nanosecond tie-break.
     */
    public void testWithinCeilingYearButPastCeilingInstantDoesNotForce() {
        assertDoesNotForceOnBothRails("2262-06-01T00:00:00.123456789Z");
    }

    /**
     * An accessor carrying a year and a time but no month or day cannot be placed on the timeline, so
     * the window is undecidable and the value stays datetime. Unreachable through either production
     * parser — both require a full date — but {@link TemporalInference#forcesDateNanos} is public and
     * takes any accessor, so the branch is real and pinned here rather than left to trust.
     */
    public void testAccessorWithoutFullDateDoesNotForce() {
        DateTimeFormatter yearAndTime = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4)
            .appendLiteral('T')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter(Locale.ROOT);
        TemporalAccessor parsed = yearAndTime.parse("1970T00:00:00.000000001");
        assertTrue("precondition: the fraction is visible", parsed.isSupported(ChronoField.NANO_OF_SECOND));
        assertFalse("precondition: it cannot be placed on the timeline", parsed.isSupported(ChronoField.INSTANT_SECONDS));
        assertFalse(TemporalInference.forcesDateNanos(parsed));
    }

    private static TemporalAccessor ndjsonParse(String value) {
        TemporalAccessor parsed = STRICT_DATE_OPTIONAL_TIME.tryParse(value);
        assertNotNull("the ndjson rail must accept [" + value + "] for this case to mean anything", parsed);
        return parsed;
    }

    private static TemporalAccessor csvParse(String value) {
        return DateUtils.asDateTime(value);
    }

    private static void assertForcesOnBothRails(String value) {
        assertTrue(value + " (ndjson rail)", TemporalInference.forcesDateNanos(ndjsonParse(value)));
        assertTrue(value + " (csv rail)", TemporalInference.forcesDateNanos(csvParse(value)));
    }

    private static void assertDoesNotForceOnBothRails(String value) {
        assertFalse(value + " (ndjson rail)", TemporalInference.forcesDateNanos(ndjsonParse(value)));
        assertFalse(value + " (csv rail)", TemporalInference.forcesDateNanos(csvParse(value)));
    }
}
