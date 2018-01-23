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

import org.elasticsearch.ElasticsearchParseException;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormatter;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A parser for date/time formatted text with optional date math.
 *
 * The format of the datetime is configurable, and unix timestamps can also be used. Datemath
 * is appended to a datetime with the following syntax:
 * <code>||[+-/](\d+)?[yMwdhHms]</code>.
 */
public class DateMathParser {

    private final FormatDateTimeFormatter dateTimeFormatter;

    public DateMathParser(FormatDateTimeFormatter dateTimeFormatter) {
        Objects.requireNonNull(dateTimeFormatter);
        this.dateTimeFormatter = dateTimeFormatter;
    }

    public long parse(String text, LongSupplier now) {
        return parse(text, now, false, (DateTimeZone) null);
    }

    public long parse(String text, LongSupplier now, boolean roundUp, ZoneId timeZone) {
        if (timeZone != null) {
            // special handling of UTC, because the text interpretation of "Z" does not work for joda time
            if (timeZone.equals(ZoneOffset.UTC)) {
                return parse(text, now, roundUp, DateTimeZone.UTC);
            } else {
                return parse(text, now, roundUp, DateTimeZone.forID(timeZone.getId()));
            }
        } else {
            return parse(text, now, roundUp, (DateTimeZone) null);
        }
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the request cache does not cache requests that make
    // use of `now`.
    public long parse(String text, LongSupplier now, boolean roundUp, DateTimeZone timeZone) {
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

    private long parseMath(String mathString, long time, boolean roundUp, DateTimeZone timeZone) throws ElasticsearchParseException {
        if (timeZone == null) {
            timeZone = DateTimeZone.UTC;
        }
        MutableDateTime dateTime = new MutableDateTime(time, timeZone);
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
            MutableDateTime.Property propertyToRound = null;
            switch (unit) {
                case 'y':
                    if (round) {
                        propertyToRound = dateTime.yearOfCentury();
                    } else {
                        dateTime.addYears(sign * num);
                    }
                    break;
                case 'M':
                    if (round) {
                        propertyToRound = dateTime.monthOfYear();
                    } else {
                        dateTime.addMonths(sign * num);
                    }
                    break;
                case 'w':
                    if (round) {
                        propertyToRound = dateTime.weekOfWeekyear();
                    } else {
                        dateTime.addWeeks(sign * num);
                    }
                    break;
                case 'd':
                    if (round) {
                        propertyToRound = dateTime.dayOfMonth();
                    } else {
                        dateTime.addDays(sign * num);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        propertyToRound = dateTime.hourOfDay();
                    } else {
                        dateTime.addHours(sign * num);
                    }
                    break;
                case 'm':
                    if (round) {
                        propertyToRound = dateTime.minuteOfHour();
                    } else {
                        dateTime.addMinutes(sign * num);
                    }
                    break;
                case 's':
                    if (round) {
                        propertyToRound = dateTime.secondOfMinute();
                    } else {
                        dateTime.addSeconds(sign * num);
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("unit [{}] not supported for date math [{}]", unit, mathString);
            }
            if (propertyToRound != null) {
                if (roundUp) {
                    // we want to go up to the next whole value, even if we are already on a rounded value
                    propertyToRound.add(1);
                    propertyToRound.roundFloor();
                    dateTime.addMillis(-1); // subtract 1 millisecond to get the largest inclusive value
                } else {
                    propertyToRound.roundFloor();
                }
            }
        }
        return dateTime.getMillis();
    }

    private long parseDateTime(String value, DateTimeZone timeZone, boolean roundUpIfNoTime) {
        DateTimeFormatter parser = dateTimeFormatter.parser();
        if (timeZone != null) {
            parser = parser.withZone(timeZone);
        }
        try {
            MutableDateTime date;
            // We use 01/01/1970 as a base date so that things keep working with date
            // fields that are filled with times without dates
            if (roundUpIfNoTime) {
                date = new MutableDateTime(1970, 1, 1, 23, 59, 59, 999, DateTimeZone.UTC);
            } else {
                date = new MutableDateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
            }
            final int end = parser.parseInto(date, value, 0);
            if (end < 0) {
                int position = ~end;
                throw new IllegalArgumentException("Parse failure at index [" + position + "] of [" + value + "]");
            } else if (end != value.length()) {
                throw new IllegalArgumentException("Unrecognized chars at the end of [" + value + "]: [" + value.substring(end) + "]");
            }
            return date.getMillis();
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchParseException("failed to parse date field [{}] with format [{}]", e, value, dateTimeFormatter.format());
        }
    }

}
