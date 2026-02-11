/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.util.List;
import java.util.Map;

public class RecordingMeterRegistryTests extends ESTestCase {
    public void testMeasuresLongMatcher() {
        final var expectedValue = randomLong();
        final var matcher = RecordingMeterRegistry.measures(expectedValue);

        assertTrue(matcher.matches(List.of(new Measurement(expectedValue, Map.of(), false))));

        final var matcherDescription = new StringDescription();
        matcher.describeTo(matcherDescription);
        assertEquals("metric which measures <" + expectedValue + "L>", matcherDescription.toString());

        final var otherValue = randomValueOtherThan(expectedValue, ESTestCase::randomLong);
        assertMismatch(matcher, List.of(new Measurement(otherValue, Map.of(), false)), "was <" + otherValue + "L>");

        final var doubleValue = randomDouble();
        assertMismatch(
            matcher,
            List.of(new Measurement(doubleValue, Map.of(), true)),
            "Measurement.isLong() expected but saw <" + doubleValue + ">"
        );
        assertMismatch(matcher, List.of(new Object()), "not a List<Measurement>, first item was a \"java.lang.Object\"");
        assertMismatch(
            matcher,
            List.of(new Measurement(expectedValue, Map.of(), randomBoolean()), new Measurement(expectedValue, Map.of(), randomBoolean())),
            "List<Measurement> size expected to be 1 but was <2>"
        );
        assertMismatch(matcher, new Object(), "not a List<>");
        assertMismatch(matcher, null, "not a List<>");

    }

    public void testMeasuresDoubleMatcher() {
        final var expectedValue = randomDouble();
        final var matcher = RecordingMeterRegistry.measures(expectedValue);

        assertTrue(matcher.matches(List.of(new Measurement(expectedValue, Map.of(), true))));

        final var matcherDescription = new StringDescription();
        matcher.describeTo(matcherDescription);
        assertEquals("metric which measures <" + expectedValue + ">", matcherDescription.toString());

        final var otherValue = randomValueOtherThan(expectedValue, ESTestCase::randomDouble);
        assertMismatch(matcher, List.of(new Measurement(otherValue, Map.of(), true)), "was <" + otherValue + ">");

        final var longValue = randomLong();
        assertMismatch(
            matcher,
            List.of(new Measurement(longValue, Map.of(), false)),
            "Measurement.isDouble() expected but saw <" + longValue + "L>"
        );
        assertMismatch(matcher, List.of(new Object()), "not a List<Measurement>, first item was a \"java.lang.Object\"");
        assertMismatch(
            matcher,
            List.of(new Measurement(expectedValue, Map.of(), randomBoolean()), new Measurement(expectedValue, Map.of(), randomBoolean())),
            "List<Measurement> size expected to be 1 but was <2>"
        );
        assertMismatch(matcher, new Object(), "not a List<>");
        assertMismatch(matcher, null, "not a List<>");

    }

    private static void assertMismatch(Matcher<List<Measurement>> matcher, Object mismatchedValue, String expectedDescription) {
        assertFalse(matcher.matches(mismatchedValue));
        final var mismatchDescription = new StringDescription();
        matcher.describeMismatch(mismatchedValue, mismatchDescription);
        assertEquals(expectedDescription, mismatchDescription.toString());
    }
}
