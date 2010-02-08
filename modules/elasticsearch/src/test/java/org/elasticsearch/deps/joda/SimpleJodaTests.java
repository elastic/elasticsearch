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

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

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
    }
}
