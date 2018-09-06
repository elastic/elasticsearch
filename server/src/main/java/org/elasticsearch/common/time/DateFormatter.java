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

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public interface DateFormatter {

    TemporalAccessor parse(String input);

    DateFormatter withZone(ZoneId zoneId);

    String print(TemporalAccessor accessor);

    String format();

    DateFormatter parseDefaulting(Map<TemporalField, Long> fields);

    static DateFormatter merge(DateFormatter ... formatters) {
        return new MergedDateFormatter(formatters);
    }

    class MergedDateFormatter implements DateFormatter {

        private final String format;
        private final DateFormatter[] formatters;

        MergedDateFormatter(DateFormatter ... formatters) {
            this.formatters = formatters;
            this.format = Arrays.stream(formatters).map(DateFormatter::format).collect(Collectors.joining("||"));
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
        public String print(TemporalAccessor accessor) {
            return formatters[0].print(accessor);
        }

        @Override
        public String format() {
            return format;
        }

        @Override
        public DateFormatter parseDefaulting(Map<TemporalField, Long> fields) {
            return new MergedDateFormatter(Arrays.stream(formatters).map(f -> f.parseDefaulting(fields)).toArray(DateFormatter[]::new));
        }
    }
}
