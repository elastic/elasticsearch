/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link RatioValue} class
 */
public class RatioValueTests extends ESTestCase {
    public void testParsing() {
        assertThat(RatioValue.parseRatioValue("100%").toString(), is("100.0%"));
        assertThat(RatioValue.parseRatioValue("0%").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0%").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("15.1%").toString(), is("15.1%"));
        assertThat(RatioValue.parseRatioValue("0.1%").toString(), is("0.1%"));
        assertThat(RatioValue.parseRatioValue("1.0").toString(), is("100.0%"));
        assertThat(RatioValue.parseRatioValue("0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("0.0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0.0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("0.151").toString(), is("15.1%"));
        assertThat(RatioValue.parseRatioValue("0.001").toString(), is("0.1%"));
    }

    public void testNegativeCase() {
        testInvalidRatio("100.0001%");
        testInvalidRatio("-0.1%");
        testInvalidRatio("1a0%");
        testInvalidRatio("2");
        testInvalidRatio("-0.01");
        testInvalidRatio("0.1.0");
        testInvalidRatio("five");
        testInvalidRatio("1/2");
    }

    public void testToStringNoTrailingZeros() {
        assertThat((new RatioValue(100.0)).formatNoTrailingZerosPercent(), is("100%"));
        assertThat((new RatioValue(.000000)).formatNoTrailingZerosPercent(), is("0%"));
        assertThat((new RatioValue(0.000000)).formatNoTrailingZerosPercent(), is("0%"));
        assertThat((new RatioValue(-0)).formatNoTrailingZerosPercent(), is("0%"));
        assertThat((new RatioValue(0)).formatNoTrailingZerosPercent(), is("0%"));
        assertThat((new RatioValue(15.1)).formatNoTrailingZerosPercent(), is("15.1%"));
        assertThat((new RatioValue(0.1000000)).formatNoTrailingZerosPercent(), is("0.1%"));
        assertThat((new RatioValue(1.1234567890)).formatNoTrailingZerosPercent(), is("1.123456789%"));
    }

    public void testParseRatioValueWithBoundsPercentFormatValidValues() {
        RatioValue lower = new RatioValue(20);
        RatioValue upper = new RatioValue(80);
        assertThat(RatioValue.parseRatioValue("20%", lower, upper).getAsPercent(), is(20.0));
        assertThat(RatioValue.parseRatioValue("50%", lower, upper).getAsPercent(), is(50.0));
        assertThat(RatioValue.parseRatioValue("80%", lower, upper).getAsPercent(), is(80.0));
    }

    public void testParseRatioValueWithBoundsPercentFormatOutsideBounds() {
        RatioValue lower = new RatioValue(20);
        RatioValue upper = new RatioValue(80);
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("19%", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("81%", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("0%", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("100%", lower, upper));
    }

    public void testParseRatioValueWithBoundsRatioFormatValidValues() {
        RatioValue lower = new RatioValue(20);
        RatioValue upper = new RatioValue(80);
        assertThat(RatioValue.parseRatioValue("0.2", lower, upper).getAsRatio(), is(0.2));
        assertThat(RatioValue.parseRatioValue("0.5", lower, upper).getAsRatio(), is(0.5));
        assertThat(RatioValue.parseRatioValue("0.8", lower, upper).getAsRatio(), is(0.8));
    }

    public void testParseRatioValueWithBoundsRatioFormatOutsideBounds() {
        RatioValue lower = new RatioValue(20);
        RatioValue upper = new RatioValue(80);
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("0.19", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("0.81", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("0.0", lower, upper));
        expectThrows(ElasticsearchParseException.class, () -> RatioValue.parseRatioValue("1.0", lower, upper));
    }

    public void testInvalidRatio(String r) {
        try {
            RatioValue.parseRatioValue(r);
            fail("Value: [" + r + "] should be an invalid ratio");
        } catch (ElasticsearchParseException e) {
            // success
        }
    }
}
