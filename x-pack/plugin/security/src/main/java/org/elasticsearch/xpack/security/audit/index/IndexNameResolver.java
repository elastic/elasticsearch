/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.function.Function;

public class IndexNameResolver {

    public enum Rollover {
        HOURLY  ("-yyyy.MM.dd.HH", d -> d.plusHours(1)),
        DAILY   ("-yyyy.MM.dd", d -> d.plusDays(1)),
        WEEKLY  ("-yyyy.w", d -> d.plusWeeks(1)),
        MONTHLY ("-yyyy.MM", d -> d.plusMonths(1));

        private final DateTimeFormatter formatter;
        private final Function<DateTime, DateTime> next;

        Rollover(String format, Function<DateTime, DateTime> next) {
            this.formatter = DateTimeFormat.forPattern(format);
            this.next = next;
        }

        DateTimeFormatter formatter() {
            return formatter;
        }

        Function<DateTime, DateTime> getNext() {
            return next;
        }
    }

    private IndexNameResolver() {}

    public static String resolve(DateTime timestamp, Rollover rollover) {
        return rollover.formatter().print(timestamp);
    }

    public static String resolveNext(String indexNamePrefix, DateTime timestamp, Rollover rollover) {
        return resolve(indexNamePrefix, rollover.getNext().apply(timestamp), rollover);
    }

    public static String resolve(String indexNamePrefix, DateTime timestamp, Rollover rollover) {
        return indexNamePrefix + resolve(timestamp, rollover);
    }
}
