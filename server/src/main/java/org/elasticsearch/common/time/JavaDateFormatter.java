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

import org.elasticsearch.common.Strings;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

class JavaDateFormatter implements DateFormatter {

    // base fields which should be used for default parsing, when we round up for date math
    private static final Map<TemporalField, Long> ROUND_UP_BASE_FIELDS = new HashMap<>(6);
    {
        ROUND_UP_BASE_FIELDS.put(ChronoField.MONTH_OF_YEAR, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.DAY_OF_MONTH, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.HOUR_OF_DAY, 23L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MINUTE_OF_HOUR, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.SECOND_OF_MINUTE, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.NANO_OF_SECOND, 999_999_999L);
    }

    private final String format;
    private final DateTimeFormatter printer;
    private final DateTimeFormatter parser;
    private final DateTimeFormatter roundupParser;

    private JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter roundupParser, DateTimeFormatter parser) {
        this.format = "8" + format;
        this.printer = printer;
        this.roundupParser = roundupParser;
        this.parser = parser;
    }

    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        this(format, printer, builder -> ROUND_UP_BASE_FIELDS.forEach(builder::parseDefaulting), parsers);
    }

    JavaDateFormatter(String format, DateTimeFormatter printer, Consumer<DateTimeFormatterBuilder> roundupParserConsumer,
                              DateTimeFormatter... parsers) {
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
        if (parsers.length == 0) {
            this.parser = printer;
        } else if (parsers.length == 1) {
            this.parser = parsers[0];
        } else {
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
            for (DateTimeFormatter parser : parsers) {
                builder.appendOptional(parser);
            }
            this.parser = builder.toFormatter(Locale.ROOT);
        }
        this.format = "8" + format;
        this.printer = printer;

        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        if (format.contains("||") == false) {
            builder.append(this.parser);
        }
        roundupParserConsumer.accept(builder);
        DateTimeFormatter roundupFormatter = builder.toFormatter(parser.getLocale());
        if (printer.getZone() != null) {
            roundupFormatter = roundupFormatter.withZone(printer.getZone());
        }
        this.roundupParser = roundupFormatter;
    }

    DateTimeFormatter getRoundupParser() {
        return roundupParser;
    }

    DateTimeFormatter getParser() {
        return parser;
    }

    DateTimeFormatter getPrinter() {
        return printer;
    }

    @Override
    public TemporalAccessor parse(String input) {
        if (Strings.isNullOrEmpty(input)) {
            throw new IllegalArgumentException("cannot parse empty date");
        }
        return parser.parse(input);
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(parser.getZone())) {
            return this;
        }

        return new JavaDateFormatter(format, printer.withZone(zoneId), roundupParser.withZone(zoneId), parser.withZone(zoneId));
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcurt to not create new objects unnecessarily
        if (locale.equals(parser.getLocale())) {
            return this;
        }

        return new JavaDateFormatter(format, printer.withLocale(locale), roundupParser.withLocale(locale), parser.withLocale(locale));
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return printer.format(DateFormatters.from(accessor));
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
        return new JavaDateMathParser(format, parser, roundupParser);
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
