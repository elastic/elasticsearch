/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public void testInvalidRatio(String r) {
        try {
            RatioValue.parseRatioValue(r);
            fail("Value: [" + r + "] should be an invalid ratio");
        } catch (ElasticsearchParseException e) {
            // success
        }
    }
}
