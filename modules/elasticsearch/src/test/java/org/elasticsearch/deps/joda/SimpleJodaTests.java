/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.deps.joda;

import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.util.Date;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleJodaTests {

    @Test public void testIsoDateFormatDateTimeNoMillisUTC() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00Z");

        assertThat(millis, equalTo(0l));
    }

    @Test public void testIsoDateFormatDateOptionalTimeUTC() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00Z");
        assertThat(millis, equalTo(0l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.001Z");
        assertThat(millis, equalTo(1l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.1Z");
        assertThat(millis, equalTo(100l));
        millis = formatter.parseMillis("1970-01-01T00:00:00.1");
        assertThat(millis, equalTo(100l));
        millis = formatter.parseMillis("1970-01-01T00:00:00");
        assertThat(millis, equalTo(0l));
        millis = formatter.parseMillis("1970-01-01");
        assertThat(millis, equalTo(0l));

        millis = formatter.parseMillis("1970");
        assertThat(millis, equalTo(0l));

        try {
            formatter.parseMillis("1970 kuku");
            assert false : "formatting should fail";
        } catch (IllegalArgumentException e) {
            // all is well
        }
    }

    @Test public void testIsoVsCustom() {
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        long millis = formatter.parseMillis("1970-01-01T00:00:00");
        assertThat(millis, equalTo(0l));

        formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").withZone(DateTimeZone.UTC);
        millis = formatter.parseMillis("1970/01/01 00:00:00");
        assertThat(millis, equalTo(0l));

        FormatDateTimeFormatter formatter2 = Joda.forPattern("yyyy/MM/dd HH:mm:ss");
        millis = formatter2.parser().parseMillis("1970/01/01 00:00:00");
        assertThat(millis, equalTo(0l));
    }

    @Test public void testWriteAndParse() {
        DateTimeFormatter dateTimeWriter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
        DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        Date date = new Date();
        assertThat(formatter.parseMillis(dateTimeWriter.print(date.getTime())), equalTo(date.getTime()));
    }

    @Test public void testSlashInFormat() {
        FormatDateTimeFormatter formatter = Joda.forPattern("MM/yyyy");
        formatter.parser().parseMillis("01/2001");

        formatter = Joda.forPattern("yyyy/MM/dd HH:mm:ss");
        long millis = formatter.parser().parseMillis("1970/01/01 00:00:00");
        formatter.printer().print(millis);

        try {
            millis = formatter.parser().parseMillis("1970/01/01");
            assert false;
        } catch (IllegalArgumentException e) {
            // it really can't parse this one
        }
    }

    @Test public void testMultipleFormats() {
        FormatDateTimeFormatter formatter = Joda.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd");
        long millis = formatter.parser().parseMillis("1970/01/01 00:00:00");
        millis = formatter.parser().parseMillis("1970/01/01");
//        System.out.println("" + millis);

        System.out.println(formatter.printer().print(millis));
    }
}
