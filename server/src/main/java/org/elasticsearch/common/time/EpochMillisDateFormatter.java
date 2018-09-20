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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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

    public static DateFormatter INSTANCE = new EpochMillisDateFormatter(ZoneOffset.UTC, Locale.ROOT);

    private final ZoneId zoneId;
    private final Locale locale;

    private EpochMillisDateFormatter(ZoneId zoneId, Locale locale) {
        this.zoneId = zoneId;
        this.locale = locale;
    }

    @Override
    public TemporalAccessor parse(String input) {
        try {
            return Instant.ofEpochMilli(Long.valueOf(input)).atZone(ZoneOffset.UTC);
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("could not parse input [" + input + "] with date formatter [epoch_millis]", e);
        }
    }

    @Override
    public DateFormatter withZone(ZoneId newZoneId) {
        return new EpochMillisDateFormatter(newZoneId, locale);
    }

    @Override
    public DateFormatter withLocale(Locale newLocale) {
        return new EpochMillisDateFormatter(zoneId, newLocale);
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
    public Locale getLocale() {
        return locale;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public DateFormatter parseDefaulting(Map<TemporalField, Long> fields) {
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        EpochMillisDateFormatter other = (EpochMillisDateFormatter) obj;

        return Objects.equals(pattern(), other.pattern()) &&
               Objects.equals(zoneId, other.zoneId) &&
               Objects.equals(locale, other.locale);
    }
}
