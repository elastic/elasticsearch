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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

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
        assertThat(new TimeValue(-1).nanos(), lessThan(0l));
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
        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10 M", null, "test"));
        assertEquals(new TimeValue(10, TimeUnit.MINUTES),
                     TimeValue.parseTimeValue("10M", null, "test"));

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

        assertEquals(new TimeValue(70, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 w", null, "test"));
        assertEquals(new TimeValue(70, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10w", null, "test"));
        assertEquals(new TimeValue(70, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10 W", null, "test"));
        assertEquals(new TimeValue(70, TimeUnit.DAYS),
                     TimeValue.parseTimeValue("10W", null, "test"));
    }

    private void assertEqualityAfterSerialize(TimeValue value) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        value.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        TimeValue inValue = TimeValue.readTimeValue(in);

        assertThat(inValue, equalTo(value));
    }

    public void testSerialize() throws Exception {
        assertEqualityAfterSerialize(new TimeValue(100, TimeUnit.DAYS));
        assertEqualityAfterSerialize(new TimeValue(-1));
        assertEqualityAfterSerialize(new TimeValue(1, TimeUnit.NANOSECONDS));
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testFailOnUnknownUnits() {
        TimeValue.parseTimeValue("23tw", null, "test");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testFailOnMissingUnits() {
        TimeValue.parseTimeValue("42", null, "test");
    }

    @Test(expected = ElasticsearchParseException.class)
    public void testNoDotsAllowed() {
        TimeValue.parseTimeValue("42ms.", null, "test");
    }
}
