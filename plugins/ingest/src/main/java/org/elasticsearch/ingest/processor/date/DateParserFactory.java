/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.ingest.processor.date;

import org.joda.time.DateTimeZone;

import java.util.Locale;

public class DateParserFactory {

    public static final String ISO8601 = "ISO8601";
    public static final String UNIX = "UNIX";
    public static final String UNIX_MS = "UNIX_MS";
    public static final String TAI64N = "TAI64N";

    public static DateParser createDateParser(String format, DateTimeZone timezone, Locale locale) {
        switch(format) {
            case ISO8601:
                // TODO(talevy): fallback solution for almost ISO8601
                return new ISO8601DateParser(timezone);
            case UNIX:
                return new UnixDateParser(timezone);
            case UNIX_MS:
                return new UnixMsDateParser(timezone);
            case TAI64N:
                return new TAI64NDateParser(timezone);
            default:
                return new JodaPatternDateParser(format, timezone, locale);
        }
    }
}
