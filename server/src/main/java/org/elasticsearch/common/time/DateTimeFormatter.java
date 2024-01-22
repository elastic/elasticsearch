/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Optional;

interface DateTimeFormatter {
    ZoneId getZone();

    Locale getLocale();

    DateTimeFormatter withZone(ZoneId zoneId);

    DateTimeFormatter withLocale(Locale locale);

    void applyToBuilder(DateTimeFormatterBuilder builder);

    TemporalAccessor parse(CharSequence str) throws DateTimeParseException;

    Optional<TemporalAccessor> tryParse(CharSequence str);

    String format(TemporalAccessor accessor);
}
