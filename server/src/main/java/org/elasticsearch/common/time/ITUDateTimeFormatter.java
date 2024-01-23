/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import com.ethlo.time.ITU;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

class ITUDateTimeFormatter implements DateTimeFormatter {

    private final ZoneId zoneId;

    ITUDateTimeFormatter() {
        zoneId = null;
    }

    private ITUDateTimeFormatter(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Locale getLocale() {
        return Locale.getDefault();
    }

    @Override
    public String getFormatString() {
        return java.time.format.DateTimeFormatter.ISO_DATE_TIME.toString();
    }

    @Override
    public DateTimeFormatter withZone(ZoneId zoneId) {
        return Objects.equals(zoneId, this.zoneId) ? this : new ITUDateTimeFormatter(zoneId);
    }

    @Override
    public DateTimeFormatter withLocale(Locale locale) {
        //if (locale.equals(Locale.getDefault()) == false) throw new UnsupportedOperationException("Cannot change locale");
        return this;
    }

    @Override
    public void applyToBuilder(DateTimeFormatterBuilder builder) {
        builder.append(java.time.format.DateTimeFormatter.ISO_DATE_TIME);
    }

    @Override
    public TemporalAccessor parse(CharSequence str) throws DateTimeParseException {
        return new ITUAccessor(ITU.parseLenient(str.toString()));
    }

    @Override
    public Optional<TemporalAccessor> tryParse(CharSequence str) {
        try {
            return Optional.of(parse(str));
        } catch (DateTimeException e) {
            return Optional.empty();
        }
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return ITU.format(OffsetDateTime.from(accessor));
    }
}
