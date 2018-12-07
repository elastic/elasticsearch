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

import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.time.ZoneId;
import java.util.Locale;

/**
 * A simple wrapper around {@link DateTimeFormatter} that retains the
 * format that was used to create it.
 */
public class FormatDateTimeFormatter {

    private final String pattern;

    private final DateTimeFormatter parser;

    private final DateTimeFormatter printer;

    public FormatDateTimeFormatter(String pattern, DateTimeFormatter parser, DateTimeFormatter printer) {
        this.pattern = pattern;
        this.printer = printer.withDefaultYear(1970);
        this.parser = parser.withDefaultYear(1970);
    }

    public String pattern() {
        return pattern;
    }

    public long parseMillis(String input) {
        return parser.parseMillis(input);
    }

    public DateTime parseJoda(String input) {
        return parser.parseDateTime(input);
    }

    public DateTimeFormatter parser() {
        return parser;
    }

    public DateTimeFormatter printer() {
        return this.printer;
    }

    public String formatJoda(DateTime dateTime) {
        return printer.print(dateTime);
    }

    public String formatMillis(long millis) {
        return printer.print(millis);
    }

    public FormatDateTimeFormatter withZone(ZoneId zoneId) {
        DateTimeFormatter parser = this.parser.withZone(DateUtils.zoneIdToDateTimeZone(zoneId));
        DateTimeFormatter printer = this.printer.withZone(DateUtils.zoneIdToDateTimeZone(zoneId));
        return new FormatDateTimeFormatter(pattern, parser, printer);
    }

    public FormatDateTimeFormatter withLocale(Locale locale) {
        DateTimeFormatter parser = this.parser.withLocale(locale);
        DateTimeFormatter printer = this.printer.withLocale(locale);
        return new FormatDateTimeFormatter(this.pattern, parser, printer);
    }

    public Locale locale() {
        return parser.getLocale();
    }

    public ZoneId zoneId() {
        return DateUtils.dateTimeZoneToZoneId(parser.getZone());
    }

    public DateMathParser toDateMathParser() {
        return new JodaDateMathParser(this);
    }
}
