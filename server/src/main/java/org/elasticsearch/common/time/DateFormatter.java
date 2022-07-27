/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public interface DateFormatter {

    /**
     * Try to parse input to a java time TemporalAccessor
     * @param input                   An arbitrary string resembling the string representation of a date or time
     * @throws DateTimeParseException If parsing fails, this exception will be thrown.
     *                                Note that it can contained suppressed exceptions when several formatters failed parse this value
     * @return                        The java time object containing the parsed input
     */
    TemporalAccessor parse(String input);

    /**
     * Parse the given input into millis-since-epoch.
     */
    default long parseMillis(String input) {
        return DateFormatters.from(parse(input)).toInstant().toEpochMilli();
    }

    /**
     * Create a copy of this formatter that is configured to parse dates in the specified time zone
     *
     * @param zoneId The time zone to act on
     * @return       A copy of the date formatter this has been called on
     */
    DateFormatter withZone(ZoneId zoneId);

    /**
     * Create a copy of this formatter that is configured to parse dates in the specified locale
     *
     * @param locale The local to use for the new formatter
     * @return       A copy of the date formatter this has been called on
     */
    DateFormatter withLocale(Locale locale);

    /**
     * Print the supplied java time accessor in a string based representation according to this formatter
     *
     * @param accessor The temporal accessor used to format
     * @return         The string result for the formatting
     */
    String format(TemporalAccessor accessor);

    /**
     * Return the given millis-since-epoch formatted with this format.
     */
    default String formatMillis(long millis) {
        ZoneId zone = zone() != null ? zone() : ZoneOffset.UTC;
        return format(Instant.ofEpochMilli(millis).atZone(zone));
    }

    /**
     * A name based format for this formatter. Can be one of the registered formatters like <code>epoch_millis</code> or
     * a configured format like <code>HH:mm:ss</code>
     *
     * @return The name of this formatter
     */
    String pattern();

    /**
     * Returns the configured locale of the date formatter
     *
     * @return The locale of this formatter
     */
    Locale locale();

    /**
     * Returns the configured time zone of the date formatter
     *
     * @return The time zone of this formatter
     */
    ZoneId zone();

    /**
     * Create a DateMathParser from the existing formatter
     *
     * @return The DateMathParser object
     */
    DateMathParser toDateMathParser();

    static DateFormatter forPattern(String input) {
        if (Strings.hasLength(input) == false) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        // support the 6.x BWC compatible way of parsing java 8 dates
        if (input.startsWith("8")) {
            input = input.substring(1);
        }

        List<DateFormatter> formatters = new ArrayList<>();
        for (String pattern : Strings.delimitedListToStringArray(input, "||")) {
            if (Strings.hasLength(pattern) == false) {
                throw new IllegalArgumentException("Cannot have empty element in multi date format pattern: " + input);
            }
            formatters.add(DateFormatters.forPattern(pattern));
        }

        if (formatters.size() == 1) {
            return formatters.get(0);
        }

        return JavaDateFormatter.combined(input, formatters);
    }
}
