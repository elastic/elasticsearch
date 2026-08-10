/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

class FastDateTimeParser implements DateTimeParser {

    private final FastDateParser parser;
    private final ZoneId timezone;
    // the locale doesn't actually matter, as we're parsing in a standardized format,
    // and we already account for . or , in decimals
    private final Locale locale;

    FastDateTimeParser(
        Set<ChronoField> mandatoryFields,
        boolean optionalTime,
        ChronoField maxAllowedField,
        DecimalSeparator decimalSeparator,
        TimezonePresence timezonePresence
    ) {
        // standard ISO-8601 uses 'T' to separate the date and time components
        this(mandatoryFields, optionalTime, maxAllowedField, decimalSeparator, timezonePresence, 'T');
    }

    /**
     * As {@link #FastDateTimeParser(Set, boolean, ChronoField, DecimalSeparator, TimezonePresence)}, but with a configurable
     * character separating the date and time components. This allows parsing formats such as {@code yyyy-MM-dd HH:mm:ss},
     * which use a space instead of the standard ISO-8601 {@code 'T'}.
     */
    FastDateTimeParser(
        Set<ChronoField> mandatoryFields,
        boolean optionalTime,
        ChronoField maxAllowedField,
        DecimalSeparator decimalSeparator,
        TimezonePresence timezonePresence,
        char dateTimeSeparator
    ) {
        parser = new FastDateParser(
            mandatoryFields,
            optionalTime,
            maxAllowedField,
            decimalSeparator,
            timezonePresence,
            Map.of(),
            dateTimeSeparator
        );
        timezone = null;
        locale = null;
    }

    private FastDateTimeParser(FastDateParser parser, ZoneId timezone, Locale locale) {
        this.parser = parser;
        this.timezone = timezone;
        this.locale = locale;
    }

    @Override
    public ZoneId getZone() {
        return timezone;
    }

    @Override
    public Locale getLocale() {
        return locale;
    }

    @Override
    public DateTimeParser withZone(ZoneId zone) {
        return new FastDateTimeParser(parser, zone, locale);
    }

    @Override
    public DateTimeParser withLocale(Locale locale) {
        return new FastDateTimeParser(parser, timezone, locale);
    }

    FastDateTimeParser withDefaults(Map<ChronoField, Integer> defaults) {
        return new FastDateTimeParser(
            new FastDateParser(
                parser.mandatoryFields(),
                parser.optionalTime(),
                parser.maxAllowedField(),
                parser.decimalSeparator(),
                parser.timezonePresence(),
                defaults,
                parser.dateTimeSeparator()
            ),
            timezone,
            locale
        );
    }

    @Override
    public TemporalAccessor parse(CharSequence str) {
        var result = parser.tryParse(str, timezone);
        var temporal = result.result();
        if (temporal == null) {
            throw new DateTimeParseException("Could not fully parse datetime", str, result.errorIndex());
        }
        return temporal;
    }

    @Override
    public ParseResult tryParse(CharSequence str) {
        return parser.tryParse(str, timezone);
    }
}
