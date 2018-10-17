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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Locale;
import java.util.Map;

/**
 * This is a special formatter to parse the milliseconds since the epoch.
 * There is no way using a native java time date formatter to resemble
 * the required behaviour to parse negative milliseconds as well.
 *
 * This implementation simply tries to convert the input to a long and uses
 * this as the milliseconds since the epoch without involving any other
 * java time code
 */
class EpochMillisDateFormatter implements DateFormatter {

    public static DateFormatter INSTANCE = new EpochMillisDateFormatter();

    private EpochMillisDateFormatter() {
    }

    @Override
    public TemporalAccessor parse(String input) {
        try {
            return Instant.ofEpochMilli(Long.valueOf(input)).atZone(ZoneOffset.UTC);
        } catch (NumberFormatException e) {
            throw new DateTimeParseException("invalid number", input, 0, e);
        }
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        if (ZoneOffset.UTC.equals(zoneId) == false) {
            throw new IllegalArgumentException(pattern() + " date formatter can only be in zone offset UTC");
        }
        return INSTANCE;
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        if (Locale.ROOT.equals(locale) == false) {
            throw new IllegalArgumentException(pattern() + " date formatter can only be in locale ROOT");
        }
        return this;
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return String.valueOf(Instant.from(accessor).toEpochMilli());
    }

    @Override
    public String pattern() {
        return "epoch_millis";
    }

    @Override
    public DateFormatter parseDefaulting(Map<TemporalField, Long> fields) {
        return this;
    }

    @Override
    public Locale getLocale() {
        return Locale.ROOT;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }
}
