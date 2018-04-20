/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class IndexNameResolver {

    public enum Rollover {
        HOURLY  ("-yyyy.MM.dd.HH"),
        DAILY   ("-yyyy.MM.dd"),
        WEEKLY  ("-yyyy.w"),
        MONTHLY ("-yyyy.MM");

        private final DateTimeFormatter formatter;

        Rollover(String format) {
            this.formatter = DateTimeFormat.forPattern(format);
        }

        DateTimeFormatter formatter() {
            return formatter;
        }
    }

    private IndexNameResolver() {}

    public static String resolve(DateTime timestamp, Rollover rollover) {
        return rollover.formatter().print(timestamp);
    }

    public static String resolve(String indexNamePrefix, DateTime timestamp, Rollover rollover) {
        return indexNamePrefix + resolve(timestamp, rollover);
    }
}
