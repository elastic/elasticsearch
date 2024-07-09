/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

class JavaTimeDateTimePrinter implements DateTimePrinter {

    private final DateTimeFormatter formatter;

    JavaTimeDateTimePrinter(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    @Override
    public ZoneId getZone() {
        return formatter.getZone();
    }

    @Override
    public Locale getLocale() {
        return formatter.getLocale();
    }

    @Override
    public DateTimePrinter withZone(ZoneId zone) {
        return new JavaTimeDateTimePrinter(formatter.withZone(zone));
    }

    @Override
    public DateTimePrinter withLocale(Locale locale) {
        return new JavaTimeDateTimePrinter(formatter.withLocale(locale));
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return formatter.format(accessor);
    }
}
