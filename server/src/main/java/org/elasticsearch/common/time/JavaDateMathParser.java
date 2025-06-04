/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;
import org.elasticsearch.exception.ElasticsearchParseException;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalQueries;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * A parser for date/time formatted text with optional date math.
 *
 * The format of the datetime is configurable, and unix timestamps can also be used. Datemath
 * is appended to a datetime with the following syntax:
 * <code>||[+-/](\d+)?[yMwdhHms]</code>.
 */
public class JavaDateMathParser implements DateMathParser {

    private final String format;
    private final Function<String, TemporalAccessor> parser;
    private final Function<String, TemporalAccessor> roundupParser;

    JavaDateMathParser(String format, Function<String, TemporalAccessor> parser, Function<String, TemporalAccessor> roundupParser) {
        this.format = format;
        this.parser = Objects.requireNonNull(parser);
        this.roundupParser = roundupParser;
    }

    @Override
    public Instant parse(String text, LongSupplier now, boolean roundUpProperty, ZoneId timeZone) {
        Instant time;
        String mathString;
        if (text.startsWith("now")) {
            try {
                // TODO only millisecond granularity here!
                time = Instant.ofEpochMilli(now.getAsLong());
            } catch (Exception e) {
                throw new ElasticsearchParseException("could not read the current timestamp", e);
            }
            mathString = text.substring("now".length());
        } else {
            int index = text.indexOf("||");
            if (index == -1) {
                return parseDateTime(text, timeZone, roundUpProperty);
            }
            time = parseDateTime(text.substring(0, index), timeZone, false);
            mathString = text.substring(index + 2);
        }

        return parseMath(mathString, time, roundUpProperty, timeZone);
    }

    private static Instant parseMath(final String mathString, final Instant time, final boolean roundUpProperty, ZoneId timeZone)
        throws ElasticsearchParseException {
        if (timeZone == null) {
            timeZone = ZoneOffset.UTC;
        }
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(time, timeZone);
        for (int i = 0; i < mathString.length();) {
            char c = mathString.charAt(i++);
            final boolean round;
            final int sign;
            if (c == '/') {
                round = true;
                sign = 1;
            } else {
                round = false;
                if (c == '+') {
                    sign = 1;
                } else if (c == '-') {
                    sign = -1;
                } else {
                    throw new ElasticsearchParseException("operator not supported for date math [{}]", mathString);
                }
            }

            if (i >= mathString.length()) {
                throw new ElasticsearchParseException("truncated date math [{}]", mathString);
            }

            final int num;
            if (Character.isDigit(mathString.charAt(i)) == false) {
                num = 1;
            } else {
                int numFrom = i;
                while (i < mathString.length() && Character.isDigit(mathString.charAt(i))) {
                    i++;
                }
                if (i >= mathString.length()) {
                    throw new ElasticsearchParseException("truncated date math [{}]", mathString);
                }
                num = Integer.parseInt(mathString.substring(numFrom, i));
            }
            if (round) {
                if (num != 1) {
                    throw new ElasticsearchParseException("rounding `/` can only be used on single unit types [{}]", mathString);
                }
            }
            char unit = mathString.charAt(i++);
            switch (unit) {
                case 'y':
                    if (round) {
                        dateTime = dateTime.withDayOfYear(1).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusYears(1);
                        }
                    } else {
                        dateTime = dateTime.plusYears(sign * num);
                    }
                    break;
                case 'M':
                    if (round) {
                        dateTime = dateTime.withDayOfMonth(1).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusMonths(1);
                        }
                    } else {
                        dateTime = dateTime.plusMonths(sign * num);
                    }
                    break;
                case 'w':
                    if (round) {
                        dateTime = dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusWeeks(1);
                        }
                    } else {
                        dateTime = dateTime.plusWeeks(sign * num);
                    }
                    break;
                case 'd':
                    if (round) {
                        dateTime = dateTime.with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusDays(1);
                        }
                    } else {
                        dateTime = dateTime.plusDays(sign * num);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        dateTime = dateTime.withMinute(0).withSecond(0).withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusHours(1);
                        }
                    } else {
                        dateTime = dateTime.plusHours(sign * num);
                    }
                    break;
                case 'm':
                    if (round) {
                        dateTime = dateTime.withSecond(0).withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusMinutes(1);
                        }
                    } else {
                        dateTime = dateTime.plusMinutes(sign * num);
                    }
                    break;
                case 's':
                    if (round) {
                        dateTime = dateTime.withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusSeconds(1);
                        }
                    } else {
                        dateTime = dateTime.plusSeconds(sign * num);
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("unit [{}] not supported for date math [{}]", unit, mathString);
            }
            if (round && roundUpProperty) {
                // subtract 1 millisecond to get the largest inclusive value
                dateTime = dateTime.minus(1, ChronoField.MILLI_OF_SECOND.getBaseUnit());
            }
        }
        return dateTime.toInstant();
    }

    private Instant parseDateTime(String value, ZoneId timeZone, boolean roundUpIfNoTime) {
        if (Strings.isNullOrEmpty(value)) {
            throw new ElasticsearchParseException("cannot parse empty datetime");
        }

        Function<String, TemporalAccessor> formatter = roundUpIfNoTime ? roundupParser : this.parser;
        try {
            if (timeZone == null) {
                return DateFormatters.from(formatter.apply(value)).toInstant();
            } else {
                TemporalAccessor accessor = formatter.apply(value);
                // Use the offset if provided, otherwise fall back to the zone, or null.
                ZoneOffset offset = TemporalQueries.offset().queryFrom(accessor);
                ZoneId zoneId = offset == null ? TemporalQueries.zoneId().queryFrom(accessor) : ZoneId.ofOffset("", offset);
                if (zoneId != null) {
                    timeZone = zoneId;
                }

                return DateFormatters.from(accessor).withZoneSameLocal(timeZone).toInstant();
            }
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new ElasticsearchParseException(
                "failed to parse date field [{}] with format [{}]: [{}]",
                e,
                value,
                format,
                e.getMessage()
            );
        }
    }
}
