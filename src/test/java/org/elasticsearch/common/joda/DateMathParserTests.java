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

package org.elasticsearch.common.joda;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DateMathParserTests extends ElasticsearchTestCase {

    FormatDateTimeFormatter formatter = Joda.forPattern("dateOptionalTime");
    DateMathParser parser = new DateMathParser(formatter, TimeUnit.MILLISECONDS);

    private static Callable<Long> callable(final long value) {
        return new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return value;
            }
        };
    }

    void assertDateMathEquals(String toTest, String expected) {
        assertDateMathEquals(toTest, expected, 0, false, null);
    }

    void assertDateMathEquals(String toTest, String expected, final long now, boolean roundUp, DateTimeZone timeZone) {
        long gotMillis = parser.parse(toTest, callable(now), roundUp, timeZone);
        assertDateEquals(gotMillis, toTest, expected);
    }

    void assertDateEquals(long gotMillis, String original, String expected) {
        long expectedMillis = parser.parse(expected, callable(0));
        if (gotMillis != expectedMillis) {
            fail("Date math not equal\n" +
                "Original              : " + original + "\n" +
                "Parsed                : " + formatter.printer().print(gotMillis) + "\n" +
                "Expected              : " + expected + "\n" +
                "Expected milliseconds : " + expectedMillis + "\n" +
                "Actual milliseconds   : " + gotMillis + "\n");
        }
    }

    @Test
    public void dataMathTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("now", callable(0)), equalTo(0l));
        assertThat(parser.parse("now+m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+1m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+11m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(11)));

        assertThat(parser.parse("now+1d", callable(0)), equalTo(TimeUnit.DAYS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        assertThat(parser.parse("now+1m-1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) - TimeUnit.SECONDS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s/m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parseRoundCeil("now+1m+1s/m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(2)));

        assertThat(parser.parse("now+4y", callable(0)), equalTo(TimeUnit.DAYS.toMillis(4*365 + 1)));
    }

    public void testRounding() {
        assertDateMathEquals("2014-11-18||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/y", "2015-01-01T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014-01-01T00:00:00.001||/y", "2015-01-01T00:00:00.000", 0, true, null);
        // rounding should also take into account time zone
        assertDateMathEquals("2014-11-18||/y", "2013-12-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/y", "2014-12-31T23:00:00.000", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/M", "2014-12-01T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11||/M", "2014-12-01T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18||/M", "2014-10-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/M", "2014-11-30T23:00:00.000Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/w", "2014-11-24T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-24T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("+01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-17T01:00:00.000Z", 0, false, DateTimeZone.forID("-01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-23T23:00:00.000Z", 0, true, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-07-22||/w", "2014-07-20T22:00:00.000Z", 0, false, DateTimeZone.forID("CET")); // with DST

        assertDateMathEquals("2014-11-18T14||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/d", "2014-11-19T00:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-19T00:00:00.000", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T15:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T15:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T15:00:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T15:00:00.000", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:28:00.000", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:28:00.000", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:33.000", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:33.000", 0, true, null);
    }

    @Test
    public void actualDateTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("1970-01-01", callable(0)), equalTo(0l));
        assertThat(parser.parse("1970-01-01||+1m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("1970-01-01||+1m+1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));

        assertThat(parser.parse("2013-01-01||+1y", callable(0)), equalTo(parser.parse("2013-01-01", callable(0)) + TimeUnit.DAYS.toMillis(365)));
        assertThat(parser.parse("2013-03-03||/y", callable(0)), equalTo(parser.parse("2013-01-01", callable(0))));
        assertThat(parser.parseRoundCeil("2013-03-03||/y", callable(0)), equalTo(parser.parse("2014-01-01", callable(0))));
    }

    public void testOnlyCallsNowIfNecessary() {
        final AtomicBoolean called = new AtomicBoolean();
        final Callable<Long> now = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                called.set(true);
                return 42L;
            }
        };
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        parser.parse("2014-11-18T14:27:32", now);
        assertFalse(called.get());
        parser.parse("now/d", now);
        assertTrue(called.get());
    }
}
