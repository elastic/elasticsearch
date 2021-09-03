/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
 * An abstraction over date math parsing to allow different implementation for joda and java time.
 */
public interface DateMathParser {

    /**
     * Parse a date math expression without timezone info and rounding down.
     */
    default Instant parse(String text, LongSupplier now) {
        return parse(text, now, false, (ZoneId) null);
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the request cache does not cache requests that make
    // use of `now`.

    // exists for backcompat, do not use!
    @Deprecated
    default Instant parse(String text, LongSupplier now, boolean roundUpProperty, DateTimeZone tz) {
        return parse(text, now, roundUpProperty, tz == null ? null : ZoneId.of(tz.getID()));
    }

    /**
     * Parse text, that potentially contains date math into the milliseconds since the epoch
     *
     * Examples are
     *
     * <code>2014-11-18||-2y</code> subtracts two years from the input date
     * <code>now/m</code>           rounds the current time to minute granularity
     *
     * Supported rounding units are
     * y    year
     * M    month
     * w    week (beginning on a monday)
     * d    day
     * h/H  hour
     * m    minute
     * s    second
     *
     *
     * @param text              the input
     * @param now               a supplier to retrieve the current date in milliseconds, if needed for additions
     * @param roundUpProperty   should the result be rounded up with the granularity of the rounding (e.g. <code>now/M</code>)
     * @param tz                an optional timezone that should be applied before returning the milliseconds since the epoch
     * @return                  the parsed date as an Instant since the epoch
     */
    Instant parse(String text, LongSupplier now, boolean roundUpProperty, ZoneId tz);
}
