/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SizeValueTests extends ESTestCase {
    public void testThatConversionWorks() {
        SizeValue sizeValue = new SizeValue(1000);
        assertThat(sizeValue.kilo(), is(1L));
        assertThat(sizeValue.toString(), is("1k"));

        sizeValue = new SizeValue(1000, SizeUnit.KILO);
        assertThat(sizeValue.singles(), is(1000000L));
        assertThat(sizeValue.toString(), is("1m"));

        sizeValue = new SizeValue(1000, SizeUnit.MEGA);
        assertThat(sizeValue.singles(), is(1000000000L));
        assertThat(sizeValue.toString(), is("1g"));

        sizeValue = new SizeValue(1000, SizeUnit.GIGA);
        assertThat(sizeValue.singles(), is(1000000000000L));
        assertThat(sizeValue.toString(), is("1t"));

        sizeValue = new SizeValue(1000, SizeUnit.TERA);
        assertThat(sizeValue.singles(), is(1000000000000000L));
        assertThat(sizeValue.toString(), is("1p"));

        sizeValue = new SizeValue(1000, SizeUnit.PETA);
        assertThat(sizeValue.singles(), is(1000000000000000000L));
        assertThat(sizeValue.toString(), is("1000p"));
    }

    public void testThatParsingWorks() {
        assertThat(SizeValue.parseSizeValue("1k").toString(), is(new SizeValue(1000).toString()));
        assertThat(SizeValue.parseSizeValue("1p").toString(), is(new SizeValue(1, SizeUnit.PETA).toString()));
        assertThat(SizeValue.parseSizeValue("1G").toString(), is(new SizeValue(1, SizeUnit.GIGA).toString()));
    }

    public void testThatNegativeValuesThrowException() {
        try {
            new SizeValue(-1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("may not be negative"));
        }
    }

    public void testCompareEquality() {
        long randomValue = randomNonNegativeLong();
        SizeUnit randomUnit = randomFrom(SizeUnit.values());
        SizeValue firstValue = new SizeValue(randomValue, randomUnit);
        SizeValue secondValue = new SizeValue(randomValue, randomUnit);
        assertEquals(0, firstValue.compareTo(secondValue));
    }

    public void testCompareValue() {
        long firstRandom = randomNonNegativeLong();
        long secondRandom = randomValueOtherThan(firstRandom, ESTestCase::randomNonNegativeLong);
        SizeUnit unit = randomFrom(SizeUnit.values());
        SizeValue firstSizeValue = new SizeValue(firstRandom, unit);
        SizeValue secondSizeValue = new SizeValue(secondRandom, unit);
        assertEquals(firstRandom > secondRandom, firstSizeValue.compareTo(secondSizeValue) > 0);
        assertEquals(secondRandom > firstRandom, secondSizeValue.compareTo(firstSizeValue) > 0);
    }

    public void testCompareUnits() {
        long number = randomNonNegativeLong();
        SizeUnit randomUnit = randomValueOtherThan(SizeUnit.PETA, () -> randomFrom(SizeUnit.values()));
        SizeValue firstValue = new SizeValue(number, randomUnit);
        SizeValue secondValue = new SizeValue(number, SizeUnit.PETA);
        assertTrue(firstValue.compareTo(secondValue) < 0);
        assertTrue(secondValue.compareTo(firstValue) > 0);
    }

    public void testConversionHashCode() {
        SizeValue firstValue = new SizeValue(randomIntBetween(0, Integer.MAX_VALUE), SizeUnit.GIGA);
        SizeValue secondValue = new SizeValue(firstValue.singles(), SizeUnit.SINGLE);
        assertEquals(firstValue.hashCode(), secondValue.hashCode());
    }
}
