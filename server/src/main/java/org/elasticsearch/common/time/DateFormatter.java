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
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

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
    Locale getLocale();

    /**
     * Returns the configured time zone of the date formatter
     *
     * @return The time zone of this formatter
     */
    ZoneId getZone();

    /**
     * Return a {@link DateMathParser} built from this formatter.
     */
    DateMathParser toDateMathParser();

    /**
     * Merge several date formatters into a single one. Useful if you need to have several formatters with
     * different formats act as one, for example when you specify a
     * format like <code>date_hour||epoch_millis</code>
     *
     * @param formatters The list of date formatters to be merged together
     * @return           The new date formtter containing the specified date formatters
     */
    static DateFormatter merge(DateFormatter... formatters) {
        return new MergedDateFormatter(formatters);
    }

    class MergedDateFormatter implements DateFormatter {

        private final String format;
        private final DateFormatter[] formatters;
        private final DateMathParser[] dateMathParsers;

        MergedDateFormatter(DateFormatter... formatters) {
            this.formatters = formatters;
            this.format = Arrays.stream(formatters).map(DateFormatter::pattern).collect(Collectors.joining("||"));
            this.dateMathParsers = Arrays.stream(formatters).map(DateFormatter::toDateMathParser).toArray(DateMathParser[]::new);
        }

        @Override
        public TemporalAccessor parse(String input) {
            DateTimeParseException failure = null;
            for (DateFormatter formatter : formatters) {
                try {
                    return formatter.parse(input);
                } catch (DateTimeParseException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            throw failure;
        }

        @Override
        public DateFormatter withZone(ZoneId zoneId) {
            return new MergedDateFormatter(Arrays.stream(formatters).map(f -> f.withZone(zoneId)).toArray(DateFormatter[]::new));
        }

        @Override
        public DateFormatter withLocale(Locale locale) {
            return new MergedDateFormatter(Arrays.stream(formatters).map(f -> f.withLocale(locale)).toArray(DateFormatter[]::new));
        }

        @Override
        public String format(TemporalAccessor accessor) {
            return formatters[0].format(accessor);
        }

        @Override
        public String pattern() {
            return format;
        }

        @Override
        public Locale getLocale() {
            return formatters[0].getLocale();
        }

        @Override
        public ZoneId getZone() {
            return formatters[0].getZone();
        }

        @Override
        public DateMathParser toDateMathParser() {
            return (text, now, roundUp, tz) -> {
                ElasticsearchParseException failure = null;
                for (DateMathParser parser : dateMathParsers) {
                    try {
                        return parser.parse(text, now, roundUp, tz);
                    } catch (ElasticsearchParseException e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                    }
                }
                throw failure;
            };
        }
    }
}
