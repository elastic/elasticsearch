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
package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * wrapper class around java.time.DateTimeFormatter that supports multiple formats for easier parsing
 */
public class DateFormatter {

    private DateTimeFormatter printer;
    private DateTimeFormatter[] parsers;

    public DateFormatter(DateTimeFormatter printer, DateTimeFormatter ... parsers) {
        this.printer = printer;
        this.parsers = parsers;
    }

    public TemporalAccessor parse(String input) {
        if (parsers.length > 0) {
            for (int i = 0; i < parsers.length; i++) {
                try {
                    return parsers[i].parse(input);
                } catch (DateTimeParseException e) {}
            }
        }

        return printer.parse(input);
    }

    public DateFormatter withZone(ZoneId zoneId) {
        printer = printer.withZone(zoneId);
        for (int i = 0; i < parsers.length; i++) {
            parsers[i] = parsers[i].withZone(zoneId);
        }

        return this;
    }

    public String format(TemporalAccessor accessor) {
        return printer.format(accessor);
    }

    // for internal use only
    private DateTimeFormatter[] all() {
        return Stream.concat(Arrays.stream(new DateTimeFormatter[] { printer }), Arrays.stream(parsers)).toArray(DateTimeFormatter[]::new);
    }

    public static DateFormatter merge(DateFormatter ... dateFormatters) {
        DateTimeFormatter[] parsers = Arrays.stream(dateFormatters).map(DateFormatter::all)
            .flatMap(Arrays::stream).toArray(DateTimeFormatter[]::new);
        return new DateFormatter(dateFormatters[0].printer, parsers);
    }
}
