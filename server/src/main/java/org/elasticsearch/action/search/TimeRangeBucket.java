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

import java.util.Arrays;
import java.util.List;

/**
 * Classifies a data age (milliseconds between a timestamp and now) into a discrete named bucket.
 * The thresholds defined here are the single source of truth for both the search-request
 * {@link SearchRequestAttributesExtractor#TIME_RANGE_FILTER_FROM_ATTRIBUTE}
 * (via {@link SearchRequestAttributesExtractor#introspectTimeRange})
 * and the blob-cache read/miss age histogram bucket boundaries.
 *
 * <p>{@link #resolve} maps negative ages (future timestamps) to {@link #FifteenMinutes} and
 * everything beyond 14 days to {@link #OlderThan14Days}. The same thresholds converted to hours
 * are passed to OpenTelemetry explicit-bucket histograms: values {@code <=} the first bound
 * (including negatives) land in the first bucket, and {@link #OlderThan14Days} uses
 * {@code Double.MAX_VALUE} so every larger finite age lands in the last explicit bucket rather
 * than an implicit overflow bucket that some export paths drop.
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
    private static final double MILLIS_PER_HOUR = TimeValue.timeValueHours(1).getMillis();
    private static final List<Double> HISTOGRAM_HOUR_BOUNDARIES = buildHistogramHourBoundaries();

    private final long millis;
    private final String label;

    TimeRangeBucket(long millis, String label) {
        this.millis = millis;
        this.label = label;
    }

    /** The string label used as the {@link SearchRequestAttributesExtractor#TIME_RANGE_FILTER_FROM_ATTRIBUTE} value for this bucket. */
    public String label() {
        return label;
    }

    /** Upper-inclusive age threshold for this bucket, in milliseconds. */
    public long millis() {
        return millis;
    }

    /**
     * Converts an age in milliseconds to hours for the blob-cache age histograms.
     */
    public static double toHours(long ageMillis) {
        return ageMillis / MILLIS_PER_HOUR;
    }

    /**
     * Explicit upper-inclusive histogram bucket boundaries in hours, matching every threshold.
     * {@link #OlderThan14Days} is {@code Double.MAX_VALUE} so implementations without an implicit
     * overflow bucket still have a last bucket.
     */
    public static List<Double> histogramHourBoundaries() {
        return HISTOGRAM_HOUR_BOUNDARIES;
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

    private static List<Double> buildHistogramHourBoundaries() {
        return Arrays.stream(VALUES).map(b -> b == OlderThan14Days ? Double.MAX_VALUE : toHours(b.millis)).toList();
    }
}
