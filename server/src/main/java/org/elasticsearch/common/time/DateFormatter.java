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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

/**
 * wrapper class around java.time.DateTimeFormatter that supports multiple formats for easier parsing
 */
public class DateFormatter {

    private DateTimeFormatter[] formatters;

    public DateFormatter(DateTimeFormatter ... formatters) {
        if (formatters.length == 0) {
            throw new IllegalArgumentException("DateFormatter requires at least one date time formatter");
        }
        this.formatters = formatters;
    }

    public TemporalAccessor parse(String input) {
        if (formatters.length > 1) {
            for (int i = 1; i < formatters.length; i++) {
                try {
                    return formatters[i].parse(input);
                } catch (DateTimeParseException e) {}
            }
        }

        return formatters[0].parse(input);
    }

    public DateFormatter withZone(ZoneId zoneId) {
        for (int i = 0; i < formatters.length; i++) {
            formatters[i] = formatters[i].withZone(zoneId);
        }

        return this;
    }

    public String format(TemporalAccessor accessor) {
        return formatters[0].format(accessor);
    }

    // for internal use only
    DateTimeFormatter[] formatters() {
        return formatters;
    }
}
