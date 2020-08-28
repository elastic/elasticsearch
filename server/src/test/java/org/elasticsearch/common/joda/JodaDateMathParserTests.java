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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JodaDateMathParserTests extends ESTestCase {

    DateFormatter formatter = Joda.forPattern("date_optional_time||epoch_millis");
    DateMathParser parser = formatter.toDateMathParser();

    void assertDateMathEquals(String toTest, String expected) {
        assertDateMathEquals(toTest, expected, 0, false, null);
    }

    void assertDateMathEquals(String toTest, String expected, final long now, boolean roundUp, DateTimeZone timeZone) {
        long gotMillis = parser.parse(toTest, () -> now, roundUp, timeZone).toEpochMilli();
        assertDateEquals(gotMillis, toTest, expected);
    }

    void assertDateEquals(long gotMillis, String original, String expected) {
        long expectedMillis = parser.parse(expected, () -> 0).toEpochMilli();
        if (gotMillis != expectedMillis) {
            fail("Date math not equal\n" +
                "Original              : " + original + "\n" +
                "Parsed                : " + formatter.formatMillis(gotMillis) + "\n" +
                "Expected              : " + expected + "\n" +
                "Expected milliseconds : " + expectedMillis + "\n" +
                "Actual milliseconds   : " + gotMillis + "\n");
        }
    }

    public void testOverridingLocaleOrZoneAndCompositeRoundUpParser() {
        //the pattern has to be composite and the match should not be on the first one
        DateFormatter formatter = Joda.forPattern("date||epoch_millis").withLocale(randomLocale(random()));
        DateMathParser parser = formatter.toDateMathParser();
        long gotMillis = parser.parse("297276785531", () -> 0, true, (ZoneId) null).toEpochMilli();
        assertDateEquals(gotMillis, "297276785531", "297276785531");

        formatter = Joda.forPattern("date||epoch_millis").withZone(ZoneOffset.UTC);
        parser = formatter.toDateMathParser();
        gotMillis = parser.parse("297276785531", () -> 0, true, (ZoneId) null).toEpochMilli();
        assertDateEquals(gotMillis, "297276785531", "297276785531");
    }

    public void testBasicDates() {
        assertDateMathEquals("2014", "2014-01-01T00:00:00.000");
        assertDateMathEquals("2014-05", "2014-05-01T00:00:00.000");
        assertDateMathEquals("2014-05-30", "2014-05-30T00:00:00.000");
        assertDateMathEquals("2014-05-30T20", "2014-05-30T20:00:00.000");
        assertDateMathEquals("2014-05-30T20:21", "2014-05-30T20:21:00.000");
        assertDateMathEquals("2014-05-30T20:21:35", "2014-05-30T20:21:35.000");
        assertDateMathEquals("2014-05-30T20:21:35.123", "2014-05-30T20:21:35.123");
    }

    public void testRoundingDoesNotAffectExactDate() {
        assertDateMathEquals("2014-11-12T22:55:00.000Z", "2014-11-12T22:55:00.000Z", 0, true, null);
        assertDateMathEquals("2014-11-12T22:55:00.000Z", "2014-11-12T22:55:00.000Z", 0, false, null);

        assertDateMathEquals("2014-11-12T22:55:00.000", "2014-11-12T21:55:00.000Z", 0, true, DateTimeZone.forID("+01:00"));
        assertDateMathEquals("2014-11-12T22:55:00.000", "2014-11-12T21:55:00.000Z", 0, false, DateTimeZone.forID("+01:00"));

        assertDateMathEquals("2014-11-12T22:55:00.000+01:00", "2014-11-12T21:55:00.000Z", 0, true, null);
        assertDateMathEquals("2014-11-12T22:55:00.000+01:00", "2014-11-12T21:55:00.000Z", 0, false, null);
    }

    public void testTimezone() {
        // timezone works within date format
        assertDateMathEquals("2014-05-30T20:21+02:00", "2014-05-30T18:21:00.000");

        // test alternative ways of writing zero offsets, according to ISO 8601 +00:00, +00, +0000 should work.
        // joda also seems to allow for -00:00, -00, -0000
        assertDateMathEquals("2014-05-30T18:21+00:00", "2014-05-30T18:21:00.000");
        assertDateMathEquals("2014-05-30T18:21+00", "2014-05-30T18:21:00.000");
        assertDateMathEquals("2014-05-30T18:21+0000", "2014-05-30T18:21:00.000");
        assertDateMathEquals("2014-05-30T18:21-00:00", "2014-05-30T18:21:00.000");
        assertDateMathEquals("2014-05-30T18:21-00", "2014-05-30T18:21:00.000");
        assertDateMathEquals("2014-05-30T18:21-0000", "2014-05-30T18:21:00.000");

        // but also externally
        assertDateMathEquals("2014-05-30T20:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("+02:00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("+00:00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("+00:00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("+00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("+0000"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("-00:00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("-00"));
        assertDateMathEquals("2014-05-30T18:21", "2014-05-30T18:21:00.000", 0, false, DateTimeZone.forID("-0000"));

        // and timezone in the date has priority
        assertDateMathEquals("2014-05-30T20:21+03:00", "2014-05-30T17:21:00.000", 0, false, DateTimeZone.forID("-08:00"));
        assertDateMathEquals("2014-05-30T20:21Z", "2014-05-30T20:21:00.000", 0, false, DateTimeZone.forID("-08:00"));
    }

    public void testBasicMath() {
        assertDateMathEquals("2014-11-18||+y", "2015-11-18");
        assertDateMathEquals("2014-11-18||-2y", "2012-11-18");

        assertDateMathEquals("2014-11-18||+3M", "2015-02-18");
        assertDateMathEquals("2014-11-18||-M", "2014-10-18");

        assertDateMathEquals("2014-11-18||+1w", "2014-11-25");
        assertDateMathEquals("2014-11-18||-3w", "2014-10-28");

        assertDateMathEquals("2014-11-18||+22d", "2014-12-10");
        assertDateMathEquals("2014-11-18||-423d", "2013-09-21");

        assertDateMathEquals("2014-11-18T14||+13h", "2014-11-19T03");
        assertDateMathEquals("2014-11-18T14||-1h", "2014-11-18T13");
        assertDateMathEquals("2014-11-18T14||+13H", "2014-11-19T03");
        assertDateMathEquals("2014-11-18T14||-1H", "2014-11-18T13");

        assertDateMathEquals("2014-11-18T14:27||+10240m", "2014-11-25T17:07");
        assertDateMathEquals("2014-11-18T14:27||-10m", "2014-11-18T14:17");

        assertDateMathEquals("2014-11-18T14:27:32||+60s", "2014-11-18T14:28:32");
        assertDateMathEquals("2014-11-18T14:27:32||-3600s", "2014-11-18T13:27:32");
    }

    public void testLenientEmptyMath() {
        assertDateMathEquals("2014-05-30T20:21||", "2014-05-30T20:21:00.000");
    }

    public void testMultipleAdjustments() {
        assertDateMathEquals("2014-11-18||+1M-1M", "2014-11-18");
        assertDateMathEquals("2014-11-18||+1M-1m", "2014-12-17T23:59");
        assertDateMathEquals("2014-11-18||-1m+1M", "2014-12-17T23:59");
        assertDateMathEquals("2014-11-18||+1M/M", "2014-12-01");
        assertDateMathEquals("2014-11-18||+1M/M+1h", "2014-12-01T01");
    }


    public void testNow() {
        final long now = parser.parse("2014-11-18T14:27:32", () -> 0, false, (ZoneId) null).toEpochMilli();

        assertDateMathEquals("now", "2014-11-18T14:27:32", now, false, null);
        assertDateMathEquals("now+M", "2014-12-18T14:27:32", now, false, null);
        assertDateMathEquals("now+M", "2014-12-18T14:27:32", now, true, null);
        assertDateMathEquals("now-2d", "2014-11-16T14:27:32", now, false, null);
        assertDateMathEquals("now-2d", "2014-11-16T14:27:32", now, true, null);
        assertDateMathEquals("now/m", "2014-11-18T14:27", now, false, null);
        assertDateMathEquals("now/m", "2014-11-18T14:27:59.999Z", now, true, null);
        assertDateMathEquals("now/M", "2014-11-01T00:00:00", now, false, null);
        assertDateMathEquals("now/M", "2014-11-30T23:59:59.999Z", now, true, null);

        // timezone does not affect now
        assertDateMathEquals("now/m", "2014-11-18T14:27", now, false, DateTimeZone.forID("+02:00"));
    }

    public void testRoundingPreservesEpochAsBaseDate() {
        // If a user only specifies times, then the date needs to always be 1970-01-01 regardless of rounding
        DateFormatter formatter = DateFormatter.forPattern("HH:mm:ss");
        DateMathParser parser = formatter.toDateMathParser();
        assertEquals(
                this.formatter.parseMillis("1970-01-01T04:52:20.000Z"),
                parser.parse("04:52:20", () -> 0, false, (ZoneId) null).toEpochMilli());
        assertEquals(
                this.formatter.parseMillis("1970-01-01T04:52:20.999Z"),
                parser.parse("04:52:20", () -> 0, true, (ZoneId) null).toEpochMilli());
    }

    // Implicit rounding happening when parts of the date are not specified
    public void testImplicitRounding() {
        assertDateMathEquals("2014-11-18", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18", "2014-11-18T23:59:59.999Z", 0, true, null);

        assertDateMathEquals("2014-11-18T09:20", "2014-11-18T09:20", 0, false, null);
        assertDateMathEquals("2014-11-18T09:20", "2014-11-18T09:20:59.999Z", 0, true, null);

        assertDateMathEquals("2014-11-18", "2014-11-17T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18", "2014-11-18T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18T09:20", "2014-11-18T08:20:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18T09:20", "2014-11-18T08:20:59.999Z", 0, true, DateTimeZone.forID("CET"));

        // implicit rounding with explicit timezone in the date format
        DateFormatter formatter = Joda.forPattern("yyyy-MM-ddZ");
        DateMathParser parser = formatter.toDateMathParser();
        Instant time = parser.parse("2011-10-09+01:00", () -> 0, false, (ZoneId) null);
        assertEquals(this.parser.parse("2011-10-09T00:00:00.000+01:00", () -> 0), time);
        time = parser.parse("2011-10-09+01:00", () -> 0, true, (ZoneId) null);
        assertEquals(this.parser.parse("2011-10-09T23:59:59.999+01:00", () -> 0), time);
    }

    // Explicit rounding using the || separator
    public void testExplicitRounding() {
        assertDateMathEquals("2014-11-18||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/y", "2014-12-31T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014||/y", "2014-01-01", 0, false, null);
        assertDateMathEquals("2014-01-01T00:00:00.001||/y", "2014-12-31T23:59:59.999", 0, true, null);
        // rounding should also take into account time zone
        assertDateMathEquals("2014-11-18||/y", "2013-12-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/y", "2014-12-31T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-18||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11-18||/M", "2014-11-30T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11||/M", "2014-11-01", 0, false, null);
        assertDateMathEquals("2014-11||/M", "2014-11-30T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/M", "2014-10-31T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/M", "2014-11-30T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));

        assertDateMathEquals("2014-11-17T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-19T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-20T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-21T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-22T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-23T14||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/w", "2014-11-23T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-17", 0, false, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-23T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("+01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-17T01:00:00.000Z", 0, false, DateTimeZone.forID("-01:00"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-16T23:00:00.000Z", 0, false, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-11-18||/w", "2014-11-23T22:59:59.999Z", 0, true, DateTimeZone.forID("CET"));
        assertDateMathEquals("2014-07-22||/w", "2014-07-20T22:00:00.000Z", 0, false, DateTimeZone.forID("CET")); // with DST

        assertDateMathEquals("2014-11-18T14||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/d", "2014-11-18T23:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-18", 0, false, null);
        assertDateMathEquals("2014-11-18||/d", "2014-11-18T23:59:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/h", "2014-11-18T14:59:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14", 0, false, null);
        assertDateMathEquals("2014-11-18T14||/H", "2014-11-18T14:59:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/m", "2014-11-18T14:27:59.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:27", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27||/m", "2014-11-18T14:27:59.999", 0, true, null);

        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32.123||/s", "2014-11-18T14:27:32.999", 0, true, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:32", 0, false, null);
        assertDateMathEquals("2014-11-18T14:27:32||/s", "2014-11-18T14:27:32.999", 0, true, null);
    }

    public void testTimestamps() {
        assertDateMathEquals("1418248078000", "2014-12-10T21:47:58.000");
        assertDateMathEquals("32484216259000", "2999-05-20T17:24:19.000");
        assertDateMathEquals("253382837059000", "9999-05-20T17:24:19.000");

        // datemath still works on timestamps
        assertDateMathEquals("1418248078000||/m", "2014-12-10T21:47:00.000");

        // also check other time units
        JodaDateMathParser parser = new JodaDateMathParser(Joda.forPattern("epoch_second"));
        long datetime = parser.parse("1418248078", () -> 0).toEpochMilli();
        assertDateEquals(datetime, "1418248078", "2014-12-10T21:47:58.000");

        // a timestamp before 10000 is a year
        assertDateMathEquals("9999", "9999-01-01T00:00:00.000");
        // 10000 is also a year, breaking bwc, used to be a timestamp
        assertDateMathEquals("10000", "10000-01-01T00:00:00.000");
        // but 10000 with T is still a date format
        assertDateMathEquals("10000T", "10000-01-01T00:00:00.000");
    }

    void assertParseException(String msg, String date, String exc) {
        try {
            parser.parse(date, () -> 0);
            fail("Date: " + date + "\n" + msg);
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage().contains(exc), equalTo(true));
        }
    }

    public void testIllegalMathFormat() {
        assertParseException("Expected date math unsupported operator exception", "2014-11-18||*5", "operator not supported");
        assertParseException("Expected date math incompatible rounding exception", "2014-11-18||/2m", "rounding");
        assertParseException("Expected date math illegal unit type exception", "2014-11-18||+2a", "unit [a] not supported");
        assertParseException("Expected date math truncation exception", "2014-11-18||+12", "truncated");
        assertParseException("Expected date math truncation exception", "2014-11-18||-", "truncated");
    }

    public void testIllegalDateFormat() {
        assertParseException("Expected bad timestamp exception", Long.toString(Long.MAX_VALUE) + "0", "failed to parse date field");
        assertParseException("Expected bad date format exception", "123bogus", "with format");
    }

    public void testOnlyCallsNowIfNecessary() {
        final AtomicBoolean called = new AtomicBoolean();
        final LongSupplier now = () -> {
            called.set(true);
            return 42L;
        };
        parser.parse("2014-11-18T14:27:32", now, false, (ZoneId) null);
        assertFalse(called.get());
        parser.parse("now/d", now, false, (ZoneId) null);
        assertTrue(called.get());
    }

    public void testThatUnixTimestampMayNotHaveTimeZone() {
        JodaDateMathParser parser = new JodaDateMathParser(Joda.forPattern("epoch_millis"));
        try {
            parser.parse("1234567890123", () -> 42, false, ZoneId.of("CET"));
            fail("Expected ElasticsearchParseException");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("failed to parse date field"));
            assertThat(e.getMessage(), containsString("with format [epoch_millis]"));
        }
    }
}
