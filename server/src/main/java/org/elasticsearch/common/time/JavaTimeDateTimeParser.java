/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

class JavaTimeDateTimeParser implements DateTimeParser {

    static UnaryOperator<JavaTimeDateTimeParser> createRoundUpParserGenerator(Consumer<DateTimeFormatterBuilder> modifyBuilder) {
        return p -> {
            var builder = new DateTimeFormatterBuilder();
            builder.append(p.formatter);
            modifyBuilder.accept(builder);
            return new JavaTimeDateTimeParser(builder.toFormatter(p.getLocale()));
        };
    }

    private final DateTimeFormatter formatter;

    JavaTimeDateTimeParser(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    DateTimeFormatter formatter() {
        return formatter;
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
    public DateTimeParser withZone(ZoneId zone) {
        return new JavaTimeDateTimeParser(formatter.withZone(zone));
    }

    @Override
    public DateTimeParser withLocale(Locale locale) {
        return new JavaTimeDateTimeParser(formatter.withLocale(locale));
    }

    @Override
    public TemporalAccessor parse(CharSequence str) {
        return formatter.parse(str);
    }

    @Override
    public ParseResult tryParse(CharSequence str) {
        ParsePosition pos = new ParsePosition(0);
        var result = (TemporalAccessor) formatter.toFormat().parseObject(str.toString(), pos);
        return pos.getIndex() == str.length() ? new ParseResult(result) : ParseResult.error(Math.max(pos.getErrorIndex(), pos.getIndex()));
    }
}
