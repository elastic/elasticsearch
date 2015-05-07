/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 *
 */
public class IndexNameResolver {

    private final DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG, Locale.ROOT);

    public enum Rollover {
        HOURLY  ("-yyyy-MM-dd-HH"),
        DAILY   ("-yyyy-MM-dd"),
        WEEKLY  ("-yyyy-w"),
        MONTHLY ("-yyyy-MM");

        private final String format;

        Rollover(String format) {
            this.format = format;
        }
    }

    public String resolve(long timestamp, Rollover rollover) {
        Date date = new Date(timestamp);
        ((SimpleDateFormat) formatter).applyPattern(rollover.format);
        return formatter.format(date);
    }

    public String resolve(String indexNamePrefix, long timestamp, Rollover rollover) {
        return indexNamePrefix + resolve(timestamp, rollover);
    }
}
