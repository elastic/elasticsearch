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
import org.elasticsearch.common.Strings;

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
import java.util.function.LongSupplier;

/**
 * A parser for date/time formatted text with optional date math.
 *
 * The format of the datetime is configurable, and unix timestamps can also be used. Datemath
 * is appended to a datetime with the following syntax:
 * <code>||[+-/](\d+)?[yMwdhHms]</code>.
 */
public class JavaDateMathParser implements DateMathParser {

    private final JavaDateFormatter formatter;
    private final String format;
    private final JavaDateFormatter roundupParser;

    JavaDateMathParser(String format, JavaDateFormatter formatter, JavaDateFormatter roundupParser) {
        this.format = format;
        this.roundupParser = roundupParser;
        Objects.requireNonNull(formatter);
        this.formatter = formatter;
    }

    @Override
    public long parse(String text, LongSupplier now, boolean roundUp, ZoneId timeZone) {
        long time;
        String mathString;
        if (text.startsWith("now")) {
            try {
                time = now.getAsLong();
            } catch (Exception e) {
                throw new ElasticsearchParseException("could not read the current timestamp", e);
            }
            mathString = text.substring("now".length());
        } else {
            int index = text.indexOf("||");
            if (index == -1) {
                return parseDateTime(text, timeZone, roundUp);
            }
            time = parseDateTime(text.substring(0, index), timeZone, false);
            mathString = text.substring(index + 2);
        }

        return parseMath(mathString, time, roundUp, timeZone);
    }

    private long parseMath(final String mathString, final long time, final boolean roundUp,
                           ZoneId timeZone) throws ElasticsearchParseException {
        if (timeZone == null) {
            timeZone = ZoneOffset.UTC;
        }
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), timeZone);
        for (int i = 0; i < mathString.length(); ) {
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
            if (!Character.isDigit(mathString.charAt(i))) {
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
                    } else {
                        dateTime = dateTime.plusYears(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusYears(1);
                    }
                    break;
                case 'M':
                    if (round) {
                        dateTime = dateTime.withDayOfMonth(1).with(LocalTime.MIN);
                    } else {
                        dateTime = dateTime.plusMonths(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusMonths(1);
                    }
                    break;
                case 'w':
                    if (round) {
                        dateTime = dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).with(LocalTime.MIN);
                    } else {
                        dateTime = dateTime.plusWeeks(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusWeeks(1);
                    }
                    break;
                case 'd':
                    if (round) {
                        dateTime = dateTime.with(LocalTime.MIN);
                    } else {
                        dateTime = dateTime.plusDays(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusDays(1);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        dateTime = dateTime.withMinute(0).withSecond(0).withNano(0);
                    } else {
                        dateTime = dateTime.plusHours(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusHours(1);
                    }
                    break;
                case 'm':
                    if (round) {
                        dateTime = dateTime.withSecond(0).withNano(0);
                    } else {
                        dateTime = dateTime.plusMinutes(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusMinutes(1);
                    }
                    break;
                case 's':
                    if (round) {
                        dateTime = dateTime.withNano(0);
                    } else {
                        dateTime = dateTime.plusSeconds(sign * num);
                    }
                    if (roundUp) {
                        dateTime = dateTime.plusSeconds(1);
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("unit [{}] not supported for date math [{}]", unit, mathString);
            }
            if (roundUp) {
                dateTime = dateTime.minus(1, ChronoField.MILLI_OF_SECOND.getBaseUnit());
            }
        }
        return dateTime.toInstant().toEpochMilli();
    }

    private long parseDateTime(String value, ZoneId timeZone, boolean roundUpIfNoTime) {
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("cannot parse empty date");
        }

        DateFormatter formatter = roundUpIfNoTime ? this.roundupParser : this.formatter;
        try {
            if (timeZone == null) {
                return DateFormatters.from(formatter.parse(value)).toInstant().toEpochMilli();
            } else {
                TemporalAccessor accessor = formatter.parse(value);
                ZoneId zoneId = TemporalQueries.zone().queryFrom(accessor);
                if (zoneId != null) {
                    timeZone = zoneId;
                }

                return DateFormatters.from(accessor).withZoneSameLocal(timeZone).toInstant().toEpochMilli();
            }
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new ElasticsearchParseException("failed to parse date field [{}] with format [{}]: [{}]",
                e, value, format, e.getMessage());
        }
    }
}
