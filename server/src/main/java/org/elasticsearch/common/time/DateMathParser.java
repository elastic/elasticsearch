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
import org.elasticsearch.common.collect.Tuple;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

public class DateMathParser {

    // base fields which should be used for default parsing, when we round up
    private static final Map<TemporalField, Long> ROUND_UP_BASE_FIELDS = new HashMap<>(6);
    {
        ROUND_UP_BASE_FIELDS.put(ChronoField.MONTH_OF_YEAR, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.DAY_OF_MONTH, 1L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.HOUR_OF_DAY, 23L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MINUTE_OF_HOUR, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.SECOND_OF_MINUTE, 59L);
        ROUND_UP_BASE_FIELDS.put(ChronoField.MILLI_OF_SECOND, 999L);
    }

    private final CompoundDateTimeFormatter formatter;
    private final CompoundDateTimeFormatter roundUpFormatter;

    public DateMathParser(CompoundDateTimeFormatter formatter) {
        Objects.requireNonNull(formatter);
        this.formatter = formatter;
        this.roundUpFormatter = formatter.parseDefaulting(ROUND_UP_BASE_FIELDS);
    }

    public long parse(String text, LongSupplier now) {
        return parse(text, now, false, null);
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the request cache does not cache requests that make
    // use of `now`.
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

    private long parseMath(String mathString, long time, boolean roundUp, ZoneId timeZone) throws ElasticsearchParseException {
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
            ChronoField propertyToAdd = null;
            List<Tuple<ChronoField, Integer>> propertiesToRound = new ArrayList<>();
            switch (unit) {
                case 'y':
                    if (round) {
                        // TODO this used to be year of century, lets see if this works
                        propertyToAdd = ChronoField.YEAR;
                        propertiesToRound.add(Tuple.tuple(ChronoField.MONTH_OF_YEAR, 1));
                        propertiesToRound.add(Tuple.tuple(ChronoField.DAY_OF_MONTH, 1));
                        propertiesToRound.add(Tuple.tuple(ChronoField.HOUR_OF_DAY, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.MINUTE_OF_HOUR, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusYears(sign * num);
                    }
                    break;
                case 'M':
                    if (round) {
                        propertyToAdd = ChronoField.MONTH_OF_YEAR;
                        propertiesToRound.add(Tuple.tuple(ChronoField.DAY_OF_MONTH, 1));
                        propertiesToRound.add(Tuple.tuple(ChronoField.HOUR_OF_DAY, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.MINUTE_OF_HOUR, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusMonths(sign * num);
                    }
                    break;
                case 'w':
                    if (round) {
                        propertyToAdd = ChronoField.ALIGNED_WEEK_OF_YEAR;
                        propertiesToRound.add(Tuple.tuple(ChronoField.DAY_OF_WEEK, 1));
                        propertiesToRound.add(Tuple.tuple(ChronoField.HOUR_OF_DAY, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.MINUTE_OF_HOUR, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusWeeks(sign * num);
                    }
                    break;
                case 'd':
                    if (round) {
                        propertyToAdd = ChronoField.DAY_OF_MONTH;
                        propertiesToRound.add(Tuple.tuple(ChronoField.HOUR_OF_DAY, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.MINUTE_OF_HOUR, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusDays(sign * num);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        propertyToAdd = ChronoField.HOUR_OF_DAY;
                        propertiesToRound.add(Tuple.tuple(ChronoField.MINUTE_OF_HOUR, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusHours(sign * num);
                    }
                    break;
                case 'm':
                    if (round) {
                        propertyToAdd = ChronoField.MINUTE_OF_HOUR;
                        propertiesToRound.add(Tuple.tuple(ChronoField.SECOND_OF_MINUTE, 0));
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusMinutes(sign * num);
                    }
                    break;
                case 's':
                    if (round) {
                        propertyToAdd = ChronoField.SECOND_OF_MINUTE;
                        propertiesToRound.add(Tuple.tuple(ChronoField.NANO_OF_SECOND, 0));
                    } else {
                        dateTime = dateTime.plusSeconds(sign * num);
                    }
                    break;
                default:
                    throw new ElasticsearchParseException("unit [{}] not supported for date math [{}]", unit, mathString);
            }
            if (roundUp) {
                // we want to go up to the next whole value, even if we are already on a rounded value
                dateTime = dateTime.plus(1, propertyToAdd.getBaseUnit());
                for (Tuple<ChronoField, Integer> tuple : propertiesToRound) {
                    dateTime = dateTime.with(tuple.v1(), tuple.v2());
                }
                dateTime = dateTime.minus(1, ChronoField.MILLI_OF_SECOND.getBaseUnit());
            } else {
                for (Tuple<ChronoField, Integer> tuple : propertiesToRound) {
                    dateTime = dateTime.with(tuple.v1(), tuple.v2());
                }
            }
        }
        return dateTime.toInstant().toEpochMilli();
    }

    private long parseDateTime(String value, ZoneId timeZone, boolean roundUpIfNoTime) {
        CompoundDateTimeFormatter formatter = roundUpIfNoTime ? this.roundUpFormatter : this.formatter;
        try {
            if (timeZone == null) {
                return DateFormatters.toZonedDateTime(formatter.parse(value)).toInstant().toEpochMilli();
            } else {
                // if a timezone is specified in the formatter, then that timezone is used, no matter if one was specified
                // this requires us to parse the the twice actually, which is pretty inefficient
                // FIXME this currently parses dates twice to extract the timezone
                ZoneId zoneId = TemporalQueries.zone().queryFrom(formatter.parse(value));
                if (zoneId != null) {
                    timeZone = zoneId;
                }

                return DateFormatters.toZonedDateTime(formatter.withZone(timeZone).parse(value))
                    .withZoneSameLocal(timeZone)
                    .toInstant().toEpochMilli();
            }
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new ElasticsearchParseException("failed to parse date field [{}]: [{}]", e, value, e.getMessage());
        }
    }
}
