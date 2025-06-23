/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * An object that can parse strings into datetime objects
 */
interface DateTimeParser {
    ZoneId getZone();

    Locale getLocale();

    DateTimeParser withZone(ZoneId zone);

    DateTimeParser withLocale(Locale locale);

    /**
     * Parses the specified string.
     * <p>
     * The pattern must fully match, using the whole string.
     * If the string cannot be fully parsed, {@link DateTimeParseException} is thrown.
     * @throws DateTimeParseException   The string could not be fully parsed
     */
    TemporalAccessor parse(CharSequence str);

    /**
     * Try to parse the specified string.
     * <p>
     * The pattern must fully match, using the whole string. It must not throw exceptions if parsing fails.
     */
    ParseResult tryParse(CharSequence str);
}
