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
 * Blob-cache age histograms are intended for search-node observability; indexing-tier
 * region timestamps are not fully supported, so dashboards should filter to search nodes.
 *
 * <p>{@link #resolve} maps negative ages (future timestamps) to {@link #FifteenMinutes} and
 * everything beyond 14 days (plus {@link #RESOLVE_TOLERANCE}) to {@link #OlderThan14Days}. Unlike the
 * histogram boundaries, {@link #resolve} counts an age up to {@link #RESOLVE_TOLERANCE} past a threshold
 * as within it. The same thresholds converted to hours
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

    /**
     * How far past a threshold an age may be and still be counted as within it by {@link #resolve}.
     *
     * <p>Clients such as Kibana resolve relative ranges like "Last 1 hour" to absolute timestamps before
     * sending the request, so by the time the age is computed, the lower bound is the threshold plus the
     * time it took the request to get here. Without a tolerance, the most common time picker presets would
     * land one bucket too old. Ranges written as date math such as {@code now-1h} don't need it, because they
     * resolve against the same now the age is computed against.
     *
     * <p>One minute covers network latency, request queueing, dashboard panels that are sent late, and
     * moderate clock skew between the browser and the cluster, while staying small next to the smallest
     * bucket (about 7% of 15 minutes). The cost is that a range that really is up to a minute older than
     * a threshold, such as "last 61 minutes", is counted in the bucket below it, which is acceptable given
     * how coarse the buckets are.
     *
     * <p>Bucketing by the length of the requested range instead would be exact for such presets, but it
     * would no longer describe how old the oldest requested data is, and would misclassify ranges that
     * end in the past.
     */
    public static final TimeValue RESOLVE_TOLERANCE = TimeValue.ONE_MINUTE;

    private static final TimeRangeBucket[] VALUES = values();
    private static final double MILLIS_PER_HOUR = TimeValue.ONE_HOUR.getMillis();
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
     * Returns the bucket label for the given age in milliseconds, counting an age up to
     * {@link #RESOLVE_TOLERANCE} past a threshold as within it.
     */
    public static String resolve(long ageMillis) {
        // Subtract from the age rather than add to the thresholds so OlderThan14Days's Long.MAX_VALUE cannot overflow.
        long toleranceMillis = RESOLVE_TOLERANCE.millis();
        long toleratedAgeMillis = Math.max(ageMillis, Long.MIN_VALUE + toleranceMillis) - toleranceMillis;
        for (TimeRangeBucket bucket : VALUES) {
            if (toleratedAgeMillis <= bucket.millis) {
                return bucket.label;
            }
        }
        throw new AssertionError("unreachable: OlderThan14Days has threshold Long.MAX_VALUE");
    }

    private static List<Double> buildHistogramHourBoundaries() {
        return Arrays.stream(VALUES).map(b -> b == OlderThan14Days ? Double.MAX_VALUE : toHours(b.millis)).toList();
    }
}
