/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        SizeUnit randomUnit = randomValueOtherThan(SizeUnit.PETA, ()->randomFrom(SizeUnit.values()));
        SizeValue firstValue = new SizeValue(number, randomUnit);
        SizeValue secondValue = new SizeValue(number, SizeUnit.PETA);
        assertTrue(firstValue.compareTo(secondValue) < 0);
        assertTrue(secondValue.compareTo(firstValue) > 0);
    }

    public void testConversionHashCode() {
        SizeValue firstValue = new SizeValue(randomIntBetween(0, Integer.MAX_VALUE), SizeUnit.GIGA);
        SizeValue secondValue = new SizeValue(firstValue.getSingles(), SizeUnit.SINGLE);
        assertEquals(firstValue.hashCode(), secondValue.hashCode());
    }
}
