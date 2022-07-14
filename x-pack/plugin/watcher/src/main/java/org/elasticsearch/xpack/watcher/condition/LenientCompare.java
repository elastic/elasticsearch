/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class LenientCompare {
    // this method performs lenient comparison, potentially between different types. The second argument
    // type (v2) determines the type of comparison (this is because the second argument is configured by the
    // user while the first argument is the dynamic path that is evaluated at runtime. That is, if the user configures
    // a number, it expects a number, therefore the comparison will be based on numeric comparison). If the
    // comparison is numeric, other types (e.g. strings) will converted to numbers if possible, if not, the comparison
    // will fail and `false` will be returned.
    //
    // may return `null` indicating v1 simply doesn't equal v2 (without any order association)
    @SuppressWarnings("unchecked")
    public static Integer compare(Object v1, Object v2) {
        if (Objects.equals(v1, v2)) {
            return 0;
        }
        if (v1 == null || v2 == null) {
            return null;
        }

        if (v1.equals(Double.NaN) || v2.equals(Double.NaN) || v1.equals(Float.NaN) || v2.equals(Float.NaN)) {
            return null;
        }

        // special case for numbers. If v1 is not a number, we'll try to convert it to a number
        if (v2 instanceof Number) {
            if ((v1 instanceof Number) == false) {
                try {
                    v1 = Double.valueOf(String.valueOf(v1));
                } catch (NumberFormatException nfe) {
                    // could not convert to number
                    return null;
                }
            }
            return ((Number) v1).doubleValue() > ((Number) v2).doubleValue() ? 1
                : ((Number) v1).doubleValue() < ((Number) v2).doubleValue() ? -1
                : 0;
        }

        // special case for strings. If v1 is not a string, we'll convert it to a string
        if (v2 instanceof String) {
            v1 = String.valueOf(v1);
            return ((String) v1).compareTo((String) v2);
        }

        // special case for date/times. If v1 is not a dateTime, we'll try to convert it to a datetime
        if (v2 instanceof ZonedDateTime) {
            if (v1 instanceof ZonedDateTime) {
                return ((ZonedDateTime) v1).compareTo((ZonedDateTime) v2);
            }
            if (v1 instanceof String) {
                try {
                    v1 = WatcherDateTimeUtils.parseDate((String) v1);
                } catch (Exception e) {
                    return null;
                }
            } else if (v1 instanceof Number) {
                v1 = Instant.ofEpochMilli(((Number) v1).longValue()).atZone(ZoneOffset.UTC);
            } else if (v1 instanceof JodaCompatibleZonedDateTime) {
                // this can only occur in versions prior to #78417
                v1 = ((JodaCompatibleZonedDateTime) v1).getZonedDateTime();
            } else {
                // cannot convert to date...
                return null;
            }
            return ((ZonedDateTime) v1).compareTo((ZonedDateTime) v2);
        }

        if (v1.getClass() != v2.getClass() || Comparable.class.isAssignableFrom(v1.getClass())) {
            return null;
        }

        try {
            return ((Comparable) v1).compareTo(v2);
        } catch (Exception e) {
            return null;
        }
    }

}
