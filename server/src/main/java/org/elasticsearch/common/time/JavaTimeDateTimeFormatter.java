/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Optional;

class JavaTimeDateTimeFormatter implements DateTimeFormatter {

    private final java.time.format.DateTimeFormatter formatter;

    JavaTimeDateTimeFormatter(java.time.format.DateTimeFormatter formatter) {
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
    public DateTimeFormatter withZone(ZoneId zoneId) {
        return new JavaTimeDateTimeFormatter(formatter.withZone(zoneId));
    }

    @Override
    public DateTimeFormatter withLocale(Locale locale) {
        return new JavaTimeDateTimeFormatter(formatter.withLocale(locale));
    }

    @Override
    public void applyToBuilder(DateTimeFormatterBuilder builder) {
        builder.append(formatter);
    }

    @Override
    public TemporalAccessor parse(CharSequence str) throws DateTimeParseException {
        return formatter.parse(str);
    }

    @Override
    public Optional<TemporalAccessor> tryParse(CharSequence str) {
        ParsePosition pos = new ParsePosition(0);
        return Optional.ofNullable((TemporalAccessor) formatter.toFormat().parseObject(str.toString(), pos))
            .filter(ta -> pos.getIndex() == str.length());
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return formatter.format(accessor);
    }
}
