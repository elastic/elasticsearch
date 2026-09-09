/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.core.TimeValue;

/**
 * Classifies a data age (milliseconds between a timestamp and now) into a discrete named bucket
 * using the same thresholds as {@code TIME_RANGE_FILTER_FROM_ATTRIBUTE} for search requests.
 *
 * <p>Negative ages (future timestamps) fall into the {@link #FifteenMinutes} bucket.
 * {@link #OlderThan14Days} is the catch-all last bucket.
 */
public enum TimeRangeBucket {
    FifteenMinutes(TimeValue.timeValueMinutes(15).getMillis(), "15_minutes"),
    OneHour(TimeValue.timeValueHours(1).getMillis(), "1_hour"),
    TwelveHours(TimeValue.timeValueHours(12).getMillis(), "12_hours"),
    OneDay(TimeValue.timeValueDays(1).getMillis(), "1_day"),
    ThreeDays(TimeValue.timeValueDays(3).getMillis(), "3_days"),
    SevenDays(TimeValue.timeValueDays(7).getMillis(), "7_days"),
    FourteenDays(TimeValue.timeValueDays(14).getMillis(), "14_days"),
    OlderThan14Days(Long.MAX_VALUE, "older_than_14_days");

    private static final TimeRangeBucket[] VALUES = values();

    private final long millis;
    private final String label;

    TimeRangeBucket(long millis, String label) {
        this.millis = millis;
        this.label = label;
    }

    /** The string label used as a metric attribute value for this bucket. */
    public String label() {
        return label;
    }

    /**
     * Returns the bucket label for the given age in milliseconds.
     */
    public static String resolve(long ageMillis) {
        for (TimeRangeBucket bucket : VALUES) {
            if (ageMillis <= bucket.millis) {
                return bucket.label;
            }
        }
        throw new AssertionError("unreachable: OlderThan14Days has threshold Long.MAX_VALUE");
    }
}
