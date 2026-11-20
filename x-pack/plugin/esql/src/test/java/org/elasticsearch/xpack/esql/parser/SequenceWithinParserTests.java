/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Fixed windows lower to maxspan without introducing a new keyword outside the sequence declaration. */
public class SequenceWithinParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    /** Compact and long fixed units share exact nanosecond conversion, including fixed 24-hour days. */
    public void testFixedUnitsAndCompactSpelling() {
        for (var entry : Map.ofEntries(
            Map.entry("1 ns", 1L),
            Map.entry("1 nanos", 1L),
            Map.entry("1 us", 1_000L),
            Map.entry("1 microsecond", 1_000L),
            Map.entry("1 ms", 1_000_000L),
            Map.entry("+1 millisecond", 1_000_000L),
            Map.entry("10s", 10_000_000_000L),
            Map.entry("10seconds", 10_000_000_000L),
            Map.entry("10 seconds", 10_000_000_000L),
            Map.entry("2 minutes", 120_000_000_000L),
            Map.entry("3 HOURS", 10_800_000_000_000L),
            Map.entry("2 days", 172_800_000_000_000L),
            Map.entry("9223372036854775807 nanos", Long.MAX_VALUE)
        ).entrySet()) {
            assertEquals(entry.getKey(), entry.getValue().longValue(), parse("WITHIN " + entry.getKey()).maxSpanNanos());
        }
        assertEquals(30_000_000_000L, parse("wItHiN 30 seconds").maxSpanNanos());
    }

    /** Decimals, calendars and overflow are errors rather than rounding, saturation or calendar arithmetic. */
    public void testNonpositiveCalendarDecimalAndOverflowAreRejected() {
        for (String duration : List.of("0s", "-1s", "1.5s")) {
            rejected("WITHIN " + duration, "positive integer fixed duration");
        }
        for (String duration : List.of("1 month", "1 year", "1 week", "1 M")) {
            expectThrows(ParsingException.class, () -> parse("WITHIN " + duration));
        }
        for (String duration : List.of("106752 days", "9223372036854775808 ns")) {
            rejected("WITHIN " + duration, "too large for a signed nanosecond span");
        }
        rejected("NEAR 10s", "Expected WITHIN");
        expectThrows(ParsingException.class, () -> parse("WITH {\"maxspan\":\"30s\"} WITHIN 30s"));
        expectThrows(ParsingException.class, () -> parse("WITHIN 30s WITH {\"maxspan\":\"30s\"}"));
    }

    /** Contextual WITHIN does not reserve existing field names, aliases or PRIMARY selectors. */
    public void testWithinRemainsAnIdentifierInExistingForms() {
        for (String window : List.of("WITHIN 1m", "WITHIN 1 minute")) {
            var statement = parser.createStatement("""
                SEQUENCE BY within %s PRIMARY within
                (FROM a) AS other, (FROM b) AS within
                | EVAL selected=MEMBER(other,within) | KEEP within,selected | LIMIT 10
                """.formatted(window));
            assertEquals(1, statement.plan().collect(SequenceMatchOutput.Scope.class).getFirst().primary());
            assertEquals(60_000_000_000L, statement.plan().collect(Sequence.class).getFirst().maxSpanNanos());
            assertNotNull(parser.createStatement("""
                SEQUENCE %s PRIMARY within
                (FROM a) AS other BY within, (FROM b) AS within BY within
                | EVAL selected=MEMBER(other,within) | KEEP within,selected | LIMIT 10
                """.formatted(window)));
        }
        EsqlParser released = new EsqlParser(new EsqlConfig(false, TEST_FUNCTION_REGISTRY));
        assertNotNull(released.createStatement("ROW within=1 | KEEP within"));
        expectThrows(ParsingException.class, () -> released.createStatement("SEQUENCE WITHIN 1s PRIMARY 1 (FROM a), (FROM b)"));
    }

    /** The single window spelling retains the explicit PRIMARY requirement. */
    public void testWindowStillRequiresPrimary() {
        assertThat(
            expectThrows(ParsingException.class, () -> parser.createStatement("SEQUENCE WITHIN 1s (FROM a), (FROM b)")).getMessage(),
            containsString("SEQUENCE requires an explicit PRIMARY")
        );
    }

    /** Recognizing a retired map produces one useful diagnostic without evaluating any option. */
    public void testWithMapsAreRefusedAndPointToWithin() {
        for (String options : List.of(
            "{}",
            "{\"maxspan\":\"30s\"}",
            "{\"maxspan\":\"30s\",\"pairing\":\"today\"}",
            "{\"maxspan\":\"30s\",\"pairing\":\"latest_valid\"}",
            "{\"maxspan\":\"30s\",\"max_candidates\":8}",
            "{\"maxspan\":\"30s\",\"other\":true}"
        )) {
            rejected("WITH " + options, "SEQUENCE does not accept WITH options; use WITHIN <duration>");
            rejected("WITHIN 30s WITH " + options, "SEQUENCE does not accept WITH options; use WITHIN <duration>");
        }
    }

    private Sequence parse(String window) {
        return parser.createStatement("SEQUENCE BY key " + window + " PRIMARY 1 (FROM a), (FROM b)")
            .plan()
            .collect(Sequence.class)
            .getFirst();
    }

    private void rejected(String window, String message) {
        assertThat(expectThrows(ParsingException.class, () -> parse(window)).getMessage(), containsString(message));
    }
}
