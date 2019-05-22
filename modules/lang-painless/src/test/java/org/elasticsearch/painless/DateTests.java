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

package org.elasticsearch.painless;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateTests extends ScriptTestCase {

    public void testLongToZonedDateTime() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC), exec(
                "long milliSinceEpoch = 434931330000L;" +
                "Instant instant = Instant.ofEpochMilli(milliSinceEpoch);" +
                "return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);"
        ));
    }

    public void testStringToZonedDateTime() {
        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC), exec(
                "String datetime = '1983-10-13T22:15:30Z';" +
                "return ZonedDateTime.parse(datetime);"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC), exec(
                "String datetime = 'Thu, 13 Oct 1983 22:15:30 GMT';" +
                "return ZonedDateTime.parse(datetime, DateTimeFormatter.RFC_1123_DATE_TIME);"
        ));

        assertEquals(ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC), exec(
                "String datetime = 'custom y 1983 m 10 d 13 22:15:30 Z';" +
                "DateTimeFormatter dtf = DateTimeFormatter.ofPattern(" +
                        "\"'custom' 'y' yyyy 'm' MM 'd' dd HH:mm:ss VV\");" +
                "return ZonedDateTime.parse(datetime, dtf);"
        ));
    }

    public void testZonedDatetimeToLong() {
        assertEquals(434931330000L, exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC);" +
                "return zdt.toInstant().toEpochMilli();"
        ));

        assertEquals("1983-10-13T22:15:30Z", exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC);" +
                 "return zdt.format(DateTimeFormatter.ISO_INSTANT);"
        ));

        assertEquals("date: 1983/10/13 time: 22:15:30", exec(
                "ZonedDateTime zdt = ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneOffset.UTC);" +
                "DateTimeFormatter dtf = DateTimeFormatter.ofPattern(" +
                    "\"'date:' yyyy/MM/dd 'time:' HH:mm:ss\");" +
                "return zdt.format(dtf);"
        ));
    }
}
