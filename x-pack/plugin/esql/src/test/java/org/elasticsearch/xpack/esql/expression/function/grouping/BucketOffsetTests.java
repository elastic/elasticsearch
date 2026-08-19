/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

public class BucketOffsetTests extends ESTestCase {

    public void testDurationSpanUsesOffset() {
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.timeDuration(Source.EMPTY, Duration.ofHours(1)),
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER,
            Duration.ofMinutes(30).toMillis(),
            Rounding.RoundingConvention.UP
        );

        long value = Instant.parse("2024-01-01T01:20:00Z").toEpochMilli();
        long rounded = bucket.getDateRounding(FoldContext.small(), null, null).round(value);
        assertEquals(Instant.parse("2024-01-01T01:30:00Z").toEpochMilli(), rounded);
    }

    public void testAutoSpanIgnoresOffset() {
        // The auto-span (numeric bucket count) path does not support offset yet.
        // TSTEP only uses the duration span path where offset is applied.
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.integer(Source.EMPTY, 4),
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T00:00:00Z")),
            Literal.dateTime(Source.EMPTY, Instant.parse("2024-01-01T04:00:00Z")),
            ConfigurationAware.CONFIGURATION_MARKER,
            Duration.ofMinutes(30).toMillis(),
            Rounding.RoundingConvention.UP
        );

        long value = Instant.parse("2024-01-01T01:20:00Z").toEpochMilli();
        long rounded = bucket.getDateRounding(FoldContext.small(), null, null).round(value);
        assertEquals(Instant.parse("2024-01-01T02:00:00Z").toEpochMilli(), rounded);
    }

    /**
     * The DST-aware fast counting in {@link Bucket.DateRoundingPicker#roundingIsOkFixedWidthUnit} must agree with the
     * naive, one-bucket-at-a-time {@link Bucket.DateRoundingPicker#roundingIsOkCalendarBasedUnit} for every fixed-width unit.
     * This exercises it across fixed-offset zones, whole-hour DST zones, a 30-minute DST zone, single- and multi-transition
     * ranges, and a range with no transition.
     */
    public void testFixedWidthCountMatchesNaiveAcrossTransitions() {
        List<ZoneId> zones = List.of(
            ZoneOffset.UTC,
            ZoneId.of("Asia/Kolkata"),         // fixed +05:30
            ZoneId.of("America/Phoenix"),      // no DST
            ZoneId.of("America/New_York"),     // whole-hour DST
            ZoneId.of("Europe/London"),        // whole-hour DST, transitions at different instants
            ZoneId.of("Australia/Lord_Howe")   // 30-minute DST
        );

        // An inclusive-from, exclusive-to date range parsed from ISO-8601 instant strings.
        record DateRange(Instant from, Instant to) {
            DateRange(String from, String to) {
                this(Instant.parse(from), Instant.parse(to));
            }
        }
        List<DateRange> ranges = List.of(
            new DateRange("2024-06-01T00:00:00Z", "2024-06-03T00:00:00Z"), // no transition
            new DateRange("2024-03-10T00:00:00Z", "2024-03-11T00:00:00Z"), // US spring-forward day
            new DateRange("2024-11-03T00:00:00Z", "2024-11-04T00:00:00Z"), // US fall-back day
            new DateRange("2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z")  // full year, multiple transitions
        );
        List<Long> targets = List.of(1L, 5L, 24L, 25L, 48L, 300L, 730L, 2000L);
        List<Bucket.DateRoundingPicker.Unit> units = List.of(
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueHours(12)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueHours(1)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueMinutes(30)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueMinutes(5)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueMinutes(1)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueSeconds(1)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueMillis(100)),
            Bucket.DateRoundingPicker.Unit.of(TimeValue.timeValueMillis(1))
        );

        for (ZoneId zone : zones) {
            for (DateRange range : ranges) {
                long from = range.from().toEpochMilli();
                long to = range.to().toEpochMilli();
                for (long target : targets) {
                    Bucket.DateRoundingPicker picker = new Bucket.DateRoundingPicker(target, from, to, zone);
                    for (Bucket.DateRoundingPicker.Unit unit : units) {
                        assertEquals(
                            "zone=" + zone + " range=" + range + " target=" + target + " width=" + unit.fixedWidthMillis(),
                            picker.roundingIsOkCalendarBasedUnit(unit),
                            picker.roundingIsOkFixedWidthUnit(unit)
                        );
                    }
                }
            }
        }
    }
}
