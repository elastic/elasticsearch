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
 * wrapper class around java.time.DateTimeFormatter that supports multiple formats for easier parsing,
 * and one specific format for printing
 */
public class CompoundDateTimeFormatter {

    final DateTimeFormatter printer;
    final DateTimeFormatter[] parsers;

    CompoundDateTimeFormatter(DateTimeFormatter ... parsers) {
        if (parsers.length == 0) {
            throw new IllegalArgumentException("at least one date time formatter is required");
        }
        this.printer = parsers[0];
        this.parsers = parsers;
    }

    public TemporalAccessor parse(String input) {
        DateTimeParseException failure = null;
        for (int i = 0; i < parsers.length; i++) {
            try {
                return parsers[i].parse(input);
            } catch (DateTimeParseException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        // ensure that all parsers exceptions are returned instead of only the last one
        throw failure;
    }

    public CompoundDateTimeFormatter withZone(ZoneId zoneId) {
        final DateTimeFormatter[] parsersWithZone = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            parsersWithZone[i] = parsers[i].withZone(zoneId);
        }

        return new CompoundDateTimeFormatter(parsersWithZone);
    }

    public String format(TemporalAccessor accessor) {
        return printer.format(accessor);
    }
}
