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

package org.elasticsearch.ingest.processor.date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ISO8601DateParser implements DateParser {

    private final DateTimeFormatter formatter;

    public ISO8601DateParser(DateTimeZone timezone) {
        formatter = ISODateTimeFormat.dateTimeParser().withZone(timezone);
    }

    public ISO8601DateParser() {
        formatter = ISODateTimeFormat.dateTimeParser().withOffsetParsed();
    }

    @Override
    public long parseMillis(String date) {
        return formatter.parseMillis(date);
    }

    @Override
    public DateTime parseDateTime(String date) {
        return formatter.parseDateTime(date);
    }
}
