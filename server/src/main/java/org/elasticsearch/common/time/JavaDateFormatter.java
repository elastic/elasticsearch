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

import org.elasticsearch.ElasticsearchParseException;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

class JavaDateFormatter implements DateFormatter {

    // base fields which should be used for default parsing, when we round up for date math
    private static final Map<TemporalField, Long> ROUND_UP_BASE_FIELDS = new HashMap<>(6);
    {
        ROUND_UP_BASE_FIELDS.put(ChronoField.MONTH_OF_YEAR, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.DAY_OF_MONTH, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.HOUR_OF_DAY, 23L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MINUTE_OF_HOUR, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.SECOND_OF_MINUTE, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MILLI_OF_SECOND, 999L);
    }

    private final String format;
    private final DateTimeFormatter printer;
    private final DateTimeFormatter[] parsers;

    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        if (printer == null) {
            throw new IllegalArgumentException("printer may not be null");
        }
        long distinctZones = Arrays.stream(parsers).map(DateTimeFormatter::getZone).distinct().count();
        if (distinctZones > 1) {
            throw new IllegalArgumentException("formatters must have the same time zone");
        }
        long distinctLocales = Arrays.stream(parsers).map(DateTimeFormatter::getLocale).distinct().count();
        if (distinctLocales > 1) {
            throw new IllegalArgumentException("formatters must have the same locale");
        }
        this.printer = printer;
        this.format = format;
        if (parsers.length == 0) {
            this.parsers = new DateTimeFormatter[]{printer};
        } else {
            this.parsers = parsers;
        }
    }

    @Override
    public TemporalAccessor parse(final String input) {
        ElasticsearchParseException failure = null;
        for (int i = 0; i < parsers.length; i++) {
            try {
                return parsers[i].parse(input);
            } catch (DateTimeParseException e) {
                String msg = createExceptionMessage(input, e);
                if (failure == null) {
                    failure = new ElasticsearchParseException(msg);
                }
                failure.addSuppressed(new ElasticsearchParseException(msg, e));
            }
        }

        // ensure that all parsers exceptions are returned instead of only the last one
        throw failure;
    }

    private String createExceptionMessage(final String input ,final DateTimeParseException e) {
        String msg = "could not parse input [" + input + "] with date formatter [" + format + "]";
        if (locale().equals(Locale.ROOT) == false) {
            msg += " and locale [" + locale() + "]";
        }
        if (e.getErrorIndex() > 0) {
            msg += "at position [" + e.getErrorIndex() + "]";
        }
        msg += ": " + e.getMessage();

        return msg;
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(parsers[0].getZone())) {
            return this;
        }

        final DateTimeFormatter[] parsersWithZone = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            parsersWithZone[i] = parsers[i].withZone(zoneId);
        }

        return new JavaDateFormatter(format, printer.withZone(zoneId), parsersWithZone);
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcurt to not create new objects unnecessarily
        if (locale.equals(parsers[0].getLocale())) {
            return this;
        }

        final DateTimeFormatter[] parsersWithZone = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            parsersWithZone[i] = parsers[i].withLocale(locale);
        }

        return new JavaDateFormatter(format, printer.withLocale(locale), parsersWithZone);
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return printer.format(accessor);
    }

    @Override
    public String pattern() {
        return format;
    }

    @Override
    public Locale locale() {
        return this.printer.getLocale();
    }

    @Override
    public ZoneId zone() {
        return this.printer.getZone();
    }

    @Override
    public DateMathParser toDateMathParser() {
        DateFormatter roundUpFormatter = this.parseDefaulting(ROUND_UP_BASE_FIELDS).withLocale(locale());
        ZoneId zone = zone();
        if (zone != null) {
            roundUpFormatter.withZone(zone);
        }
        return new JavaDateMathParser(this, roundUpFormatter);
    }

    JavaDateFormatter parseDefaulting(Map<TemporalField, Long> fields) {
        final DateTimeFormatterBuilder parseDefaultingBuilder = new DateTimeFormatterBuilder().append(printer);
        fields.forEach(parseDefaultingBuilder::parseDefaulting);
        if (parsers.length == 1 && parsers[0].equals(printer)) {
            return new JavaDateFormatter(format, parseDefaultingBuilder.toFormatter(Locale.ROOT));
        } else {
            final DateTimeFormatter[] parsersWithDefaulting = new DateTimeFormatter[parsers.length];
            for (int i = 0; i < parsers.length; i++) {
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(parsers[i]);
                fields.forEach(builder::parseDefaulting);
                parsersWithDefaulting[i] = builder.toFormatter(Locale.ROOT);
            }
            return new JavaDateFormatter(format, parseDefaultingBuilder.toFormatter(Locale.ROOT), parsersWithDefaulting);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale(), printer.getZone(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        JavaDateFormatter other = (JavaDateFormatter) obj;

        return Objects.equals(format, other.format) &&
               Objects.equals(locale(), other.locale()) &&
               Objects.equals(this.printer.getZone(), other.printer.getZone());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "format[%s] locale[%s]", format, locale());
    }
}
