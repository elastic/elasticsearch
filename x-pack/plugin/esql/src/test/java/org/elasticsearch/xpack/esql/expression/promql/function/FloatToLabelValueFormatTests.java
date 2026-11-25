/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Checks {@link FloatToLabelValue#format} against Go's {@code strconv.FormatFloat(f, 'f', -1, 64)}, which Prometheus
 * uses to render the {@code count_values} label. The expected strings were produced by running that Go function on
 * the same inputs; they must not be edited by hand.
 */
public class FloatToLabelValueFormatTests extends ESTestCase {

    private static final Map<Double, String> GO_FORMAT_FLOAT = Map.ofEntries(
        Map.entry(0.0, "0"),
        Map.entry(-0.0, "-0"),
        Map.entry(1.0, "1"),
        Map.entry(0.1, "0.1"),
        Map.entry(100.0, "100"),
        Map.entry(1e21, "1" + "0".repeat(21)),
        Map.entry(123456789012345678.0, "123456789012345680"),
        Map.entry(1e-7, "0.0000001"),
        Map.entry(-1.5, "-1.5"),
        Map.entry(-1234.5678, "-1234.5678"),
        Map.entry(0.000123, "0.000123"),
        Map.entry(12345.678e10, "123456780000000"),
        Map.entry(9007199254740992.0, "9007199254740992"),
        Map.entry(Math.nextUp(9007199254740992.0), "9007199254740994"),
        Map.entry(Math.nextDown(9007199254740992.0), "9007199254740991"),
        Map.entry(Double.MAX_VALUE, "17976931348623157" + "0".repeat(292)),
        Map.entry(Double.MIN_NORMAL, "0." + "0".repeat(307) + "22250738585072014"),
        Map.entry(-Double.MIN_VALUE, "-0." + "0".repeat(323) + "5"),
        Map.entry(Double.longBitsToDouble(1L), "0." + "0".repeat(323) + "5"),
        Map.entry(Double.longBitsToDouble(2L), "0." + "0".repeat(322) + "1"),
        Map.entry(Double.longBitsToDouble(3L), "0." + "0".repeat(322) + "15"),
        Map.entry(Double.longBitsToDouble(20L), "0." + "0".repeat(321) + "1"),
        Map.entry(Double.longBitsToDouble(21L), "0." + "0".repeat(321) + "104"),
        Map.entry(Double.longBitsToDouble(20000L), "0." + "0".repeat(319) + "98813")
    );

    public void testMatchesGoFormatFloat() {
        for (Map.Entry<Double, String> e : GO_FORMAT_FLOAT.entrySet()) {
            assertEquals("format(" + e.getKey() + ")", e.getValue(), FloatToLabelValue.format(e.getKey()));
        }
    }

    public void testNonFinite() {
        assertEquals("NaN", FloatToLabelValue.format(Double.NaN));
        assertEquals("NaN", FloatToLabelValue.format(Double.longBitsToDouble(0x7ff8000000000123L)));
        assertEquals("+Inf", FloatToLabelValue.format(Double.POSITIVE_INFINITY));
        assertEquals("-Inf", FloatToLabelValue.format(Double.NEGATIVE_INFINITY));
    }

    /**
     * Any finite value renders in plain notation and parses back to the same bits, the round-trip guarantee of the
     * shortest-digits format.
     */
    public void testRandomFiniteRoundTrips() {
        for (int i = 0; i < 10_000; i++) {
            double d = randomBoolean() ? Double.longBitsToDouble(randomLong()) : randomDouble() * randomIntBetween(-1_000_000, 1_000_000);
            if (Double.isFinite(d) == false) {
                continue;
            }
            String formatted = FloatToLabelValue.format(d);
            assertThat(formatted, not(containsString("E")));
            assertEquals(formatted, Double.doubleToRawLongBits(d), Double.doubleToRawLongBits(Double.parseDouble(formatted)));
            if (d != 0 && Math.abs(d) >= Double.MIN_NORMAL) {
                // Double.toString picks the shortest digits for every normal double, as Go does
                assertEquals(new BigDecimal(Double.toString(d)).stripTrailingZeros().toPlainString(), formatted);
            }
        }
    }
}
