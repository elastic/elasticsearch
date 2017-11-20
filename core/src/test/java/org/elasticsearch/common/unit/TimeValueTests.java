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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.PeriodType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.object.HasToString.hasToString;

public class TimeValueTests extends ESTestCase {

    public void testSimple() {
        assertThat(TimeUnit.MILLISECONDS.toMillis(10), equalTo(new TimeValue(10, TimeUnit.MILLISECONDS).millis()));
        assertThat(TimeUnit.MICROSECONDS.toMicros(10), equalTo(new TimeValue(10, TimeUnit.MICROSECONDS).micros()));
        assertThat(TimeUnit.SECONDS.toSeconds(10), equalTo(new TimeValue(10, TimeUnit.SECONDS).seconds()));
        assertThat(TimeUnit.MINUTES.toMinutes(10), equalTo(new TimeValue(10, TimeUnit.MINUTES).minutes()));
        assertThat(TimeUnit.HOURS.toHours(10), equalTo(new TimeValue(10, TimeUnit.HOURS).hours()));
        assertThat(TimeUnit.DAYS.toDays(10), equalTo(new TimeValue(10, TimeUnit.DAYS).days()));
    }

    public void testToString() {
        assertThat("10ms", equalTo(new TimeValue(10, TimeUnit.MILLISECONDS).toString()));
        assertThat("1.5s", equalTo(new TimeValue(1533, TimeUnit.MILLISECONDS).toString()));
        assertThat("1.5m", equalTo(new TimeValue(90, TimeUnit.SECONDS).toString()));
        assertThat("1.5h", equalTo(new TimeValue(90, TimeUnit.MINUTES).toString()));
        assertThat("1.5d", equalTo(new TimeValue(36, TimeUnit.HOURS).toString()));
        assertThat("1000d", equalTo(new TimeValue(1000, TimeUnit.DAYS).toString()));
    }

    public void testFormat() {
        assertThat(new TimeValue(1025, TimeUnit.MILLISECONDS).format(PeriodType.dayTime()), equalTo("1 second and 25 milliseconds"));
        assertThat(new TimeValue(1, TimeUnit.MINUTES).format(PeriodType.dayTime()), equalTo("1 minute"));
        assertThat(new TimeValue(65, TimeUnit.MINUTES).format(PeriodType.dayTime()), equalTo("1 hour and 5 minutes"));
        assertThat(new TimeValue(24 * 600 + 85, TimeUnit.MINUTES).format(PeriodType.dayTime()), equalTo("241 hours and 25 minutes"));
    }

    public void testMinusOne() {
        assertThat(new TimeValue(-1).nanos(), lessThan(0L));
    }

    public void testParseTimeValue() {
        // Space is allowed before unit:
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10 ms", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10ms", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10 MS", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MILLISECONDS),
                     TimeValue.parseTimeValue("10MS", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10 s", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10s", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10 S", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.SECONDS),
                     TimeValue.parseTimeValue("10S", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10 m", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10m", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10 h", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10h", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10 H", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.HOURS),
                     TimeValue.parseTimeValue("10H", null, "test"));

        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 d", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10d", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 D", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10D", null, "test"));

        // Time values of months should throw an exception as months are not
        // supported. Note that this is the only unit that is not case sensitive
        // as `m` is the only character that is overloaded in terms of which
        // time unit is expected between the upper and lower case versions
        expectThrows(ElasticsearchParseException.class, () -> {
            TimeValue.parseTimeValue("10 M", null, "test");
        });
        expectThrows(ElasticsearchParseException.class, () -> {
            TimeValue.parseTimeValue("10M", null, "test");
        });

        final int length = randomIntBetween(0, 8);
        final String zeros = new String(new char[length]).replace('\0', '0');
        assertTrue(TimeValue.parseTimeValue("-" + zeros + "1", null, "test") == TimeValue.MINUS_ONE);
        assertTrue(TimeValue.parseTimeValue(zeros + "0", null, "test") == TimeValue.ZERO);
    }

    public void testRoundTrip() {
        final String s = randomTimeValue();
        assertThat(TimeValue.parseTimeValue(s, null, "test").getStringRep(), equalTo(s));
        final TimeValue t = new TimeValue(randomIntBetween(1, 128), randomFrom(TimeUnit.values()));
        assertThat(TimeValue.parseTimeValue(t.getStringRep(), null, "test"), equalTo(t));
    }

    private static final String FRACTIONAL_TIME_VALUES_ARE_NOT_SUPPORTED = "fractional time values are not supported";

    public void testNonFractionalTimeValues() {
        final String s = randomAlphaOfLength(10) + randomTimeUnit();
        final ElasticsearchParseException e =
            expectThrows(ElasticsearchParseException.class, () -> TimeValue.parseTimeValue(s, null, "test"));
        assertThat(e, hasToString(containsString("failed to parse [" + s + "]")));
        assertThat(e, not(hasToString(containsString(FRACTIONAL_TIME_VALUES_ARE_NOT_SUPPORTED))));
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
    }

    public void testFractionalTimeValues() {
        double value;
        do {
            value = randomDouble();
        } while (value == 0);
        final String s = Double.toString(randomIntBetween(0, 128) + value) + randomTimeUnit();
        final ElasticsearchParseException e =
            expectThrows(ElasticsearchParseException.class, () -> TimeValue.parseTimeValue(s, null, "test"));
        assertThat(e, hasToString(containsString("failed to parse [" + s + "]")));
        assertThat(e, hasToString(containsString(FRACTIONAL_TIME_VALUES_ARE_NOT_SUPPORTED)));
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
    }

    private String randomTimeUnit() {
        return randomFrom("nanos", "micros", "ms", "s", "m", "h", "d");
    }

    private void assertEqualityAfterSerialize(TimeValue value, int expectedSize) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        value.writeTo(out);
        assertEquals(expectedSize, out.size());

        StreamInput in = out.bytes().streamInput();
        TimeValue inValue = new TimeValue(in);

        assertThat(inValue, equalTo(value));
        assertThat(inValue.duration(), equalTo(value.duration()));
        assertThat(inValue.timeUnit(), equalTo(value.timeUnit()));
    }

    public void testSerialize() throws Exception {
        assertEqualityAfterSerialize(new TimeValue(100, TimeUnit.DAYS), 3);
        assertEqualityAfterSerialize(timeValueNanos(-1), 2);
        assertEqualityAfterSerialize(timeValueNanos(1), 2);
        assertEqualityAfterSerialize(timeValueSeconds(30), 2);

        final TimeValue timeValue = new TimeValue(randomIntBetween(0, 1024), randomFrom(TimeUnit.values()));
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeZLong(timeValue.duration());
        assertEqualityAfterSerialize(timeValue, 1 + out.bytes().length());
    }

    public void testFailOnUnknownUnits() {
        try {
            TimeValue.parseTimeValue("23tw", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testFailOnMissingUnits() {
        try {
            TimeValue.parseTimeValue("42", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testNoDotsAllowed() {
        try {
            TimeValue.parseTimeValue("42ms.", null, "test");
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse"));
        }
    }

    public void testToStringRep() {
        assertEquals("-1", new TimeValue(-1).getStringRep());
        assertEquals("10ms", new TimeValue(10, TimeUnit.MILLISECONDS).getStringRep());
        assertEquals("1533ms", new TimeValue(1533, TimeUnit.MILLISECONDS).getStringRep());
        assertEquals("90s", new TimeValue(90, TimeUnit.SECONDS).getStringRep());
        assertEquals("90m", new TimeValue(90, TimeUnit.MINUTES).getStringRep());
        assertEquals("36h", new TimeValue(36, TimeUnit.HOURS).getStringRep());
        assertEquals("1000d", new TimeValue(1000, TimeUnit.DAYS).getStringRep());
    }

    public void testCompareEquality() {
        long randomLong = randomNonNegativeLong();
        TimeUnit randomUnit = randomFrom(TimeUnit.values());
        TimeValue firstValue = new TimeValue(randomLong, randomUnit);
        TimeValue secondValue = new TimeValue(randomLong, randomUnit);
        assertEquals(0, firstValue.compareTo(secondValue));
    }

    public void testCompareValue() {
        long firstRandom = randomNonNegativeLong();
        long secondRandom = randomValueOtherThan(firstRandom, ESTestCase::randomNonNegativeLong);
        TimeUnit unit = randomFrom(TimeUnit.values());
        TimeValue firstValue = new TimeValue(firstRandom, unit);
        TimeValue secondValue = new TimeValue(secondRandom, unit);
        assertEquals(firstRandom > secondRandom, firstValue.compareTo(secondValue) > 0);
        assertEquals(secondRandom > firstRandom, secondValue.compareTo(firstValue) > 0);
    }

    public void testCompareUnits() {
        long number = randomNonNegativeLong();
        TimeUnit randomUnit = randomValueOtherThan(TimeUnit.DAYS, ()->randomFrom(TimeUnit.values()));
        TimeValue firstValue = new TimeValue(number, randomUnit);
        TimeValue secondValue = new TimeValue(number, TimeUnit.DAYS);
        assertTrue(firstValue.compareTo(secondValue) < 0);
        assertTrue(secondValue.compareTo(firstValue) > 0);
    }

    public void testConversionHashCode() {
        TimeValue firstValue = new TimeValue(randomIntBetween(0, Integer.MAX_VALUE), TimeUnit.MINUTES);
        TimeValue secondValue = new TimeValue(firstValue.getSeconds(), TimeUnit.SECONDS);
        assertEquals(firstValue.hashCode(), secondValue.hashCode());
    }
}
