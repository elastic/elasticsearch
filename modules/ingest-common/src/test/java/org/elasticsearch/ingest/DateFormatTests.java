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

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.Function;

import static org.hamcrest.core.IsEqual.equalTo;

public class DateFormatTests extends ESTestCase {

    public void testParseJoda() {
        Function<String, DateTime> jodaFunction = DateFormat.Joda.getFunction("MMM dd HH:mm:ss Z",
                DateTimeZone.forOffsetHours(-8), Locale.ENGLISH);
        assertThat(Instant.ofEpochMilli(jodaFunction.apply("Nov 24 01:29:01 -0800").getMillis())
                        .atZone(ZoneId.of("GMT-8"))
                        .format(DateTimeFormatter.ofPattern("MM dd HH:mm:ss", Locale.ENGLISH)),
                equalTo("11 24 01:29:01"));
    }

    public void testParseUnixMs() {
        assertThat(DateFormat.UnixMs.getFunction(null, DateTimeZone.UTC, null).apply("1000500").getMillis(), equalTo(1000500L));
    }

    public void testParseUnix() {
        assertThat(DateFormat.Unix.getFunction(null, DateTimeZone.UTC, null).apply("1000.5").getMillis(), equalTo(1000500L));
    }

    public void testParseISO8601() {
        assertThat(DateFormat.Iso8601.getFunction(null, DateTimeZone.UTC, null).apply("2001-01-01T00:00:00-0800").getMillis(),
                equalTo(978336000000L));
    }

    public void testParseISO8601Failure() {
        Function<String, DateTime> function = DateFormat.Iso8601.getFunction(null, DateTimeZone.UTC, null);
        try {
            function.apply("2001-01-0:00-0800");
            fail("parse should have failed");
        } catch(IllegalArgumentException e) {
            //all good
        }
    }

    public void testTAI64NParse() {
        String input = "4000000050d506482dbdf024";
        String expected = "2012-12-22T03:00:46.767+02:00";
        assertThat(DateFormat.Tai64n.getFunction(null, DateTimeZone.forOffsetHours(2), null)
                .apply((randomBoolean() ? "@" : "") + input).toString(), equalTo(expected));
    }

    public void testFromString() {
        assertThat(DateFormat.fromString("UNIX_MS"), equalTo(DateFormat.UnixMs));
        assertThat(DateFormat.fromString("unix_ms"), equalTo(DateFormat.Joda));
        assertThat(DateFormat.fromString("UNIX"), equalTo(DateFormat.Unix));
        assertThat(DateFormat.fromString("unix"), equalTo(DateFormat.Joda));
        assertThat(DateFormat.fromString("ISO8601"), equalTo(DateFormat.Iso8601));
        assertThat(DateFormat.fromString("iso8601"), equalTo(DateFormat.Joda));
        assertThat(DateFormat.fromString("TAI64N"), equalTo(DateFormat.Tai64n));
        assertThat(DateFormat.fromString("tai64n"), equalTo(DateFormat.Joda));
        assertThat(DateFormat.fromString("prefix-" + randomAsciiOfLengthBetween(1, 10)), equalTo(DateFormat.Joda));
    }
}
