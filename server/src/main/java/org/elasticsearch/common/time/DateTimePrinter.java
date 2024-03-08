/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

interface DateTimePrinter {
    ZoneId getZone();

    Locale getLocale();

    DateTimePrinter withZone(ZoneId zone);

    DateTimePrinter withLocale(Locale locale);

    String format(TemporalAccessor accessor);
}
