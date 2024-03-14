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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Optional;

class JavaTimeDateTimeParser implements DateTimeParser {

    private final DateTimeFormatter formatter;

    JavaTimeDateTimeParser(DateTimeFormatter formatter) {
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
    public String getFormatString() {
        return formatter.toString();
    }

    @Override
    public DateTimeParser withZone(ZoneId zone) {
        return new JavaTimeDateTimeParser(formatter.withZone(zone));
    }

    @Override
    public DateTimeParser withLocale(Locale locale) {
        return new JavaTimeDateTimeParser(formatter.withLocale(locale));
    }

    @Override
    public void applyToBuilder(DateTimeFormatterBuilder builder) {
        builder.append(formatter);
    }

    @Override
    public TemporalAccessor parse(CharSequence str) {
        return formatter.parse(str);
    }

    @Override
    public Optional<TemporalAccessor> tryParse(CharSequence str) {
        ParsePosition pos = new ParsePosition(0);
        return Optional.ofNullable((TemporalAccessor) formatter.toFormat().parseObject(str.toString(), pos))
            .filter(ta -> pos.getIndex() == str.length());
    }
}
