/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Optional;

class CustomDateTimeFormatter extends JavaTimeDateTimeFormatter {

    private final ZoneOffset defaultOffset;

    CustomDateTimeFormatter(java.time.format.DateTimeFormatter formatter) {
        super(formatter);

        var zone = getZone();
        defaultOffset = zone != null && zone.normalized() instanceof ZoneOffset zo ? zo : null;
    }

    @Override
    public DateTimeFormatter withZone(ZoneId zoneId) {
        return new CustomDateTimeFormatter(formatter.withZone(zoneId));
    }

    @Override
    public DateTimeFormatter withLocale(Locale locale) {
        return new CustomDateTimeFormatter(formatter.withLocale(locale));
    }

    @Override
    public TemporalAccessor parse(CharSequence str) throws DateTimeParseException {
        var result = DateTimeParser.tryParse(str, defaultOffset);
        if (result.result() == null) {
            throw new DateTimeParseException("Could not parse " + str, str, result.errorIndex());
        }
        return result.result();
    }

    @Override
    public Optional<TemporalAccessor> tryParse(CharSequence str) {
        return Optional.ofNullable(DateTimeParser.tryParse(str, defaultOffset).result());
    }
}
