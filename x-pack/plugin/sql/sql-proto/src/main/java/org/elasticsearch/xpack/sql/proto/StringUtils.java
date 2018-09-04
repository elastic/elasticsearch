/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StringUtils {

    private static final int SECONDS_PER_MINUTE = 60;
    private static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    private static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    private StringUtils() {}

    public static String toString(Object value) {
        if (value == null) {
            return "null";
        }
        
        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            return ts.toInstant().toString();
        }

        // handle intervals
        // YEAR/MONTH/YEAR TO MONTH -> YEAR TO MONTH
        if (value instanceof Period) {
            // +yyy-mm - 7 chars
            StringBuilder sb = new StringBuilder(7);
            Period p = (Period) value;
            sb.append(p.isNegative() ? "-" : "+");
            sb.append(p.getYears());
            sb.append("-");
            sb.append(p.getMonths());
            return sb.toString();
        }

        // DAY/HOUR/MINUTE/SECOND (and variations) -> DAY_TO_SECOND
        if (value instanceof Duration) {
            // +ddd hh:mm:ss.mmmmmmmmm - 23 chars
            StringBuilder sb = new StringBuilder(23);
            Duration d = (Duration) value;
            sb.append(d.isNegative() ? "-" : "+");

            long durationInSec = d.getSeconds();

            sb.append(durationInSec / SECONDS_PER_DAY);
            sb.append(" ");
            durationInSec = durationInSec % SECONDS_PER_DAY;
            sb.append(indent(durationInSec / SECONDS_PER_HOUR));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_HOUR;
            sb.append(indent(durationInSec / SECONDS_PER_MINUTE));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_MINUTE;
            sb.append(indent(durationInSec));
            sb.append(".");
            sb.append(TimeUnit.NANOSECONDS.toMillis(d.getNano()));
            return sb.toString();
        }

        return Objects.toString(value);
    }

    private static String indent(long timeUnit) {
        return timeUnit < 10 ? "0" + timeUnit : Long.toString(timeUnit);
    }
}