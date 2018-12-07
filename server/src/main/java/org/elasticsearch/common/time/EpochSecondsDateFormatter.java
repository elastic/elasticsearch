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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.regex.Pattern;

public class EpochSecondsDateFormatter implements DateFormatter {

    public static DateFormatter INSTANCE = new EpochSecondsDateFormatter();
    static final DateMathParser DATE_MATH_INSTANCE = new JavaDateMathParser(INSTANCE, INSTANCE);
    private static final Pattern SPLIT_BY_DOT_PATTERN = Pattern.compile("\\.");

    private EpochSecondsDateFormatter() {}

    @Override
    public TemporalAccessor parse(String input) {
        try {
            if (input.contains(".")) {
                String[] inputs = SPLIT_BY_DOT_PATTERN.split(input, 2);
                Long seconds = Long.valueOf(inputs[0]);
                if (inputs[1].length() == 0) {
                    // this is BWC compatible to joda time, nothing after the dot is allowed
                    return Instant.ofEpochSecond(seconds, 0).atZone(ZoneOffset.UTC);
                }
                // scientific notation it is!
                if (inputs[1].contains("e")) {
                    return Instant.ofEpochSecond(Double.valueOf(input).longValue()).atZone(ZoneOffset.UTC);
                }
                if (inputs[1].length() > 9) {
                    throw new DateTimeParseException("too much granularity after dot [" + input + "]", input, 0);
                }
                Long nanos = new BigDecimal(inputs[1]).movePointRight(9 - inputs[1].length()).longValueExact();
                if (seconds < 0) {
                    nanos = nanos * -1;
                }
                return Instant.ofEpochSecond(seconds, nanos).atZone(ZoneOffset.UTC);
            } else {
                return Instant.ofEpochSecond(Long.valueOf(input)).atZone(ZoneOffset.UTC);
            }
        } catch (NumberFormatException e) {
            throw new DateTimeParseException("invalid number [" + input + "]", input, 0, e);
        }
    }

    @Override
    public String format(TemporalAccessor accessor) {
        Instant instant = Instant.from(accessor);
        if (instant.getNano() != 0) {
            return String.valueOf(instant.getEpochSecond()) + "." + String.valueOf(instant.getNano()).replaceAll("0*$", "");
        }
        return String.valueOf(instant.getEpochSecond());
    }

    @Override
    public String pattern() {
        return "epoch_second";
    }

    @Override
    public Locale getLocale() {
        return Locale.ROOT;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public DateMathParser toDateMathParser() {
        return DATE_MATH_INSTANCE;
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        if (zoneId.equals(ZoneOffset.UTC) == false) {
            throw new IllegalArgumentException(pattern() + " date formatter can only be in zone offset UTC");
        }
        return this;
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        if (Locale.ROOT.equals(locale) == false) {
            throw new IllegalArgumentException(pattern() + " date formatter can only be in locale ROOT");
        }
        return this;
    }
}
