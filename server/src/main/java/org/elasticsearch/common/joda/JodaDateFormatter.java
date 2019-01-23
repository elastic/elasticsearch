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

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

public class JodaDateFormatter implements DateFormatter {

    final String pattern;
    final DateTimeFormatter parser;
    final DateTimeFormatter printer;

    JodaDateFormatter(String pattern, DateTimeFormatter parser, DateTimeFormatter printer) {
        this.pattern = pattern;
        this.printer = printer;
        this.parser = parser;
    }

    @Override
    public TemporalAccessor parse(String input) {
        final DateTime dt = parser.parseDateTime(input);
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(dt.getMillis()), DateUtils.dateTimeZoneToZoneId(dt.getZone()));
    }

    public long parseMillis(String input) {
        return parser.parseMillis(input);
    }

    public DateTime parseJoda(String input) {
        return parser.parseDateTime(input);
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        DateTimeZone timeZone = DateUtils.zoneIdToDateTimeZone(zoneId);
        if (parser.getZone().equals(timeZone)) {
            return this;
        }
        DateTimeFormatter parser = this.parser.withZone(timeZone);
        DateTimeFormatter printer = this.printer.withZone(timeZone);
        return new JodaDateFormatter(pattern, parser, printer);
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        if (parser.getLocale().equals(locale)) {
            return this;
        }
        DateTimeFormatter parser = this.parser.withLocale(locale);
        DateTimeFormatter printer = this.printer.withLocale(locale);
        return new JodaDateFormatter(pattern, parser, printer);
    }

    @Override
    public String format(TemporalAccessor accessor) {
        DateTimeZone timeZone = DateUtils.zoneIdToDateTimeZone(ZoneId.from(accessor));
        DateTime dateTime = new DateTime(Instant.from(accessor).toEpochMilli(), timeZone);
        return printer.print(dateTime);
    }

    public String formatJoda(DateTime dateTime) {
        return printer.print(dateTime);
    }

    public String formatMillis(long millis) {
        return printer.print(millis);
    }

    public JodaDateFormatter withYear(int year) {
        if (parser.getDefaultYear() == year) {
            return this;
        }
        return new JodaDateFormatter(pattern, parser.withDefaultYear(year), printer.withDefaultYear(year));
    }

    @Override
    public String pattern() {
        return pattern;
    }

    @Override
    public Locale locale() {
        return printer.getLocale();
    }

    @Override
    public ZoneId zone() {
        return DateUtils.dateTimeZoneToZoneId(printer.getZone());
    }

    @Override
    public DateMathParser toDateMathParser() {
        return new JodaDateMathParser(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale(), zone(), pattern());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        JodaDateFormatter other = (JodaDateFormatter) obj;

        return Objects.equals(pattern(), other.pattern()) &&
            Objects.equals(locale(), other.locale()) &&
            Objects.equals(zone(), other.zone());
    }
}
