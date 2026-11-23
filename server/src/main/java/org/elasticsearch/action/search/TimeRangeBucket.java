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
 * everything beyond 14 days (plus {@link #TELEMETRY_THRESHOLD_TOLERANCE}) to {@link #OlderThan14Days}.
 * Unlike the histogram boundaries, it counts an age up to {@link #TELEMETRY_THRESHOLD_TOLERANCE} past a
 * threshold as within it. The same thresholds converted to hours
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
     * How far past a threshold {@link #resolve} still counts an age as within it. This only affects the
     * {@code time_range_filter_from} telemetry attribute, never which documents a search or query matches.
     *
     * <p>Kibana sends its time picker presets, such as "Last 1 hour", as absolute timestamps, so request
     * latency pushes their age just past the threshold. One minute covers latency, queueing and moderate
     * clock skew while staying small next to the smallest bucket (15 minutes).
     */
    public static final TimeValue TELEMETRY_THRESHOLD_TOLERANCE = TimeValue.ONE_MINUTE;

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
     * {@link #TELEMETRY_THRESHOLD_TOLERANCE} past a threshold as within it. Only used to label telemetry:
     * the {@code time_range_filter_from} attribute of the search and ES|QL query metrics. It has no effect
     * on how searches or queries are executed.
     */
    public static String resolve(long ageMillis) {
        // Subtract from the age rather than add to the thresholds so OlderThan14Days's Long.MAX_VALUE cannot overflow.
        long toleranceMillis = TELEMETRY_THRESHOLD_TOLERANCE.millis();
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
