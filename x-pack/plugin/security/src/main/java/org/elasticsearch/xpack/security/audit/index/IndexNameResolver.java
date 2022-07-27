/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.index;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class IndexNameResolver {

    public enum Rollover {
        HOURLY("-yyyy.MM.dd.HH"),
        DAILY("-yyyy.MM.dd"),
        WEEKLY("-yyyy.w"),
        MONTHLY("-yyyy.MM");

        private final DateTimeFormatter formatter;

        Rollover(String format) {
            this.formatter = DateTimeFormatter.ofPattern(format, Locale.ROOT);
        }

        DateTimeFormatter formatter() {
            return formatter;
        }
    }

    private IndexNameResolver() {}

    public static String resolve(ZonedDateTime timestamp, Rollover rollover) {
        return rollover.formatter().format(timestamp);
    }

    public static String resolve(String indexNamePrefix, ZonedDateTime timestamp, Rollover rollover) {
        return indexNamePrefix + resolve(timestamp, rollover);
    }
}
