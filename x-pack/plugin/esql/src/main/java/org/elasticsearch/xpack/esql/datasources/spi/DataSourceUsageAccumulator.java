/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Plain accumulator for the ES|QL datasource phone-home (XPack usage) telemetry counters. Receives
 * the same events as {@link ExternalSourceMetrics} but stores them in {@link LongAdder} fields rather
 * than emitting to a {@link org.elasticsearch.telemetry.metric.MeterRegistry}, making the values
 * available for periodic collection by the usage API.
 * <p>
 * Deliberately has <em>zero</em> Elasticsearch or xpack-core dependencies so that the SPI classpath
 * seen by external datasource providers does not grow. Conversion of the accumulated values into a
 * {@link org.elasticsearch.xpack.core.watcher.common.stats.Counters} object is done by
 * {@code DataSourceCounters} in the plugin layer.
 * <p>
 * Thread-safe: all fields are {@link LongAdder} or equivalent, and recording methods are stateless
 * beyond incrementing those fields.
 */
public final class DataSourceUsageAccumulator {

    // ---- scheme vocabulary (closed set, mirrors ExternalSourceMetrics.canonicalScheme) ----

    public static final int SCHEME_S3 = 0;
    public static final int SCHEME_GCS = 1;
    public static final int SCHEME_AZURE = 2;
    public static final int SCHEME_HTTP = 3;
    public static final int SCHEME_FILE = 4;
    public static final int SCHEME_UNKNOWN = 5;
    public static final int SCHEME_COUNT = 6;
    public static final String[] SCHEME_NAMES = { "s3", "gcs", "azure", "http", "file", "unknown" };
    /** Set form of {@link #SCHEME_NAMES}, used by {@code ExternalSourceMetrics} to clamp before calling {@link #schemeIndex}. */
    public static final Set<String> SCHEME_NAMES_SET = Set.of(SCHEME_NAMES);

    // ---- outcome vocabulary ----

    public static final int OUTCOME_SUCCESS = 0;
    public static final int OUTCOME_FAILURE = 1;
    public static final int OUTCOME_CANCELLED = 2;
    public static final int OUTCOME_COUNT = 3;
    public static final String[] OUTCOME_NAMES = { "success", "failure", "cancelled" };
    /** Set form of {@link #OUTCOME_NAMES}, used by {@code ExternalSourceMetrics} to clamp before calling {@link #outcomeIndex}. */
    public static final Set<String> OUTCOME_NAMES_SET = Set.of(OUTCOME_NAMES);

    // ---- bucket definitions (10 buckets each, matching ThresholdBucketer conventions) ----

    /** Time ladder (ms), mirrors TookMetrics thresholds. */
    public static final long[] TIME_THRESHOLDS = { 10, 100, 1_000, 10_000, 60_000, 600_000, 3_600_000, 36_000_000, 86_400_000 };
    public static final String[] TIME_SUFFIXES = {
        "lt_10ms",
        "lt_100ms",
        "lt_1s",
        "lt_10s",
        "lt_1m",
        "lt_10m",
        "lt_1h",
        "lt_10h",
        "lt_1d",
        "gt_1d" };

    /** Count ladder (files, splits — log-10 anchored at 1). */
    public static final long[] COUNT_THRESHOLDS = { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000 };
    public static final String[] COUNT_SUFFIXES = {
        "lt_1",
        "lt_10",
        "lt_100",
        "lt_1k",
        "lt_10k",
        "lt_100k",
        "lt_1M",
        "lt_10M",
        "lt_100M",
        "gt_100M" };

    /** Bytes ladder (log-10 anchored at 1 byte). */
    public static final long[] BYTES_THRESHOLDS = { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000 };
    public static final String[] BYTES_SUFFIXES = {
        "lt_1b",
        "lt_10b",
        "lt_100b",
        "lt_1kb",
        "lt_10kb",
        "lt_100kb",
        "lt_1mb",
        "lt_10mb",
        "lt_100mb",
        "gt_100mb" };

    public static final int BUCKET_COUNT = 10;

    // ---- per-scheme counters ----

    private final LongAdder[] storageRequests = adders(SCHEME_COUNT);
    private final LongAdder[] storageBytesRead = adders(SCHEME_COUNT);
    private final LongAdder[] storageErrors = adders(SCHEME_COUNT);
    private final LongAdder[] storageThrottled = adders(SCHEME_COUNT);

    // ---- unattributed counters ----

    private final LongAdder storageRetries = new LongAdder();
    private final LongAdder queriesCancelled = new LongAdder();
    private final LongAdder queriesPartial = new LongAdder();
    private final LongAdder discoveryFailures = new LongAdder();
    private final LongAdder parseRows = new LongAdder();
    private final LongAdder readerPoolRejected = new LongAdder();
    private final LongAdder breakerTripped = new LongAdder();

    // ---- per-outcome query counter ----

    private final LongAdder[] queries = adders(OUTCOME_COUNT);

    // ---- histogram buckets (no attribute dimension for phone-home) ----

    private final LongAdder[] storageRequestDuration = adders(BUCKET_COUNT);
    private final LongAdder[] storageReadStallDuration = adders(BUCKET_COUNT);
    private final LongAdder[] queryDuration = adders(BUCKET_COUNT);
    private final LongAdder[] queryTimeToFirstRow = adders(BUCKET_COUNT);
    private final LongAdder[] discoveryDuration = adders(BUCKET_COUNT);
    private final LongAdder[] discoveryFilesScanned = adders(BUCKET_COUNT);
    private final LongAdder[] discoveryBytesScanned = adders(BUCKET_COUNT);
    private final LongAdder[] parseDuration = adders(BUCKET_COUNT);
    private final LongAdder[] parseSplitsScanned = adders(BUCKET_COUNT);

    // ---- recording methods (called from ExternalSourceMetrics with already-canonicalised values) ----

    /** @param canonicalScheme output of {@link ExternalSourceMetrics#canonicalScheme(String)} */
    public void recordRequest(String canonicalScheme, long durationMillis, long bytes) {
        int si = schemeIndex(canonicalScheme);
        storageRequests[si].increment();
        if (bytes > 0) {
            storageBytesRead[si].add(bytes);
        }
        bucketTime(storageRequestDuration, Math.max(0L, durationMillis));
    }

    public void recordRetry() {
        storageRetries.increment();
    }

    public void recordError(String canonicalScheme) {
        storageErrors[schemeIndex(canonicalScheme)].increment();
    }

    public void recordThrottled(String canonicalScheme) {
        storageThrottled[schemeIndex(canonicalScheme)].increment();
    }

    public void recordReadStall(long millis) {
        bucketTime(storageReadStallDuration, Math.max(0L, millis));
    }

    public void recordQuery(String outcome, long durationMillis, boolean partial) {
        int oi = outcomeIndex(outcome);
        queries[oi].increment();
        bucketTime(queryDuration, Math.max(0L, durationMillis));
        if (oi == OUTCOME_CANCELLED) {
            queriesCancelled.increment();
        }
        if (partial) {
            queriesPartial.increment();
        }
    }

    public void recordTimeToFirstRow(long millis) {
        bucketTime(queryTimeToFirstRow, Math.max(0L, millis));
    }

    public void recordDiscovery(long durationMillis, long filesScanned, long bytesScanned) {
        bucketTime(discoveryDuration, Math.max(0L, durationMillis));
        bucketCount(discoveryFilesScanned, Math.max(0L, filesScanned));
        bucketBytes(discoveryBytesScanned, Math.max(0L, bytesScanned));
    }

    public void recordDiscoveryFailure() {
        discoveryFailures.increment();
    }

    public void recordParse(long rows, long parseDurationMillis) {
        if (rows > 0) {
            parseRows.add(rows);
        }
        bucketTime(parseDuration, Math.max(0L, parseDurationMillis));
    }

    public void recordSplitsScanned(long splits) {
        bucketCount(parseSplitsScanned, Math.max(0L, splits));
    }

    public void recordPoolRejected() {
        readerPoolRejected.increment();
    }

    public void recordBreakerTripped() {
        breakerTripped.increment();
    }

    // ---- snapshot accessors (read by the stats/conversion layer) ----

    /** @param schemeIndex one of {@link #SCHEME_S3}, {@link #SCHEME_GCS}, {@link #SCHEME_AZURE}, {@link #SCHEME_HTTP}, {@link #SCHEME_FILE}, {@link #SCHEME_UNKNOWN} */
    public long storageRequests(int schemeIndex) {
        checkSchemeIndex(schemeIndex);
        return storageRequests[schemeIndex].sum();
    }

    /** @param schemeIndex one of {@link #SCHEME_S3}, {@link #SCHEME_GCS}, {@link #SCHEME_AZURE}, {@link #SCHEME_HTTP}, {@link #SCHEME_FILE}, {@link #SCHEME_UNKNOWN} */
    public long storageBytesRead(int schemeIndex) {
        checkSchemeIndex(schemeIndex);
        return storageBytesRead[schemeIndex].sum();
    }

    /** @param schemeIndex one of {@link #SCHEME_S3}, {@link #SCHEME_GCS}, {@link #SCHEME_AZURE}, {@link #SCHEME_HTTP}, {@link #SCHEME_FILE}, {@link #SCHEME_UNKNOWN} */
    public long storageErrors(int schemeIndex) {
        checkSchemeIndex(schemeIndex);
        return storageErrors[schemeIndex].sum();
    }

    /** @param schemeIndex one of {@link #SCHEME_S3}, {@link #SCHEME_GCS}, {@link #SCHEME_AZURE}, {@link #SCHEME_HTTP}, {@link #SCHEME_FILE}, {@link #SCHEME_UNKNOWN} */
    public long storageThrottled(int schemeIndex) {
        checkSchemeIndex(schemeIndex);
        return storageThrottled[schemeIndex].sum();
    }

    public long storageRetries() {
        return storageRetries.sum();
    }

    /** @param outcomeIndex one of {@link #OUTCOME_SUCCESS}, {@link #OUTCOME_FAILURE}, {@link #OUTCOME_CANCELLED} */
    public long queries(int outcomeIndex) {
        checkOutcomeIndex(outcomeIndex);
        return queries[outcomeIndex].sum();
    }

    public long queriesCancelled() {
        return queriesCancelled.sum();
    }

    public long queriesPartial() {
        return queriesPartial.sum();
    }

    public long discoveryFailures() {
        return discoveryFailures.sum();
    }

    public long parseRows() {
        return parseRows.sum();
    }

    public long readerPoolRejected() {
        return readerPoolRejected.sum();
    }

    public long breakerTripped() {
        return breakerTripped.sum();
    }

    public long storageRequestDuration(int bucket) {
        return storageRequestDuration[bucket].sum();
    }

    public long storageReadStallDuration(int bucket) {
        return storageReadStallDuration[bucket].sum();
    }

    public long queryDuration(int bucket) {
        return queryDuration[bucket].sum();
    }

    public long queryTimeToFirstRow(int bucket) {
        return queryTimeToFirstRow[bucket].sum();
    }

    public long discoveryDuration(int bucket) {
        return discoveryDuration[bucket].sum();
    }

    public long discoveryFilesScanned(int bucket) {
        return discoveryFilesScanned[bucket].sum();
    }

    public long discoveryBytesScanned(int bucket) {
        return discoveryBytesScanned[bucket].sum();
    }

    public long parseDuration(int bucket) {
        return parseDuration[bucket].sum();
    }

    public long parseSplitsScanned(int bucket) {
        return parseSplitsScanned[bucket].sum();
    }

    // ---- internal helpers ----

    static int schemeIndex(String canonicalScheme) {
        return switch (canonicalScheme) {
            case "s3" -> SCHEME_S3;
            case "gcs" -> SCHEME_GCS;
            case "azure" -> SCHEME_AZURE;
            case "http" -> SCHEME_HTTP;
            case "file" -> SCHEME_FILE;
            case "unknown" -> SCHEME_UNKNOWN;
            default -> throw new IllegalArgumentException("unexpected canonical scheme: " + canonicalScheme);
        };
    }

    static int outcomeIndex(String outcome) {
        return switch (outcome) {
            case "success" -> OUTCOME_SUCCESS;
            case "failure" -> OUTCOME_FAILURE;
            case "cancelled" -> OUTCOME_CANCELLED;
            default -> throw new IllegalArgumentException("unexpected outcome: " + outcome);
        };
    }

    private static void bucketTime(LongAdder[] buckets, long value) {
        bucket(buckets, TIME_THRESHOLDS, value);
    }

    private static void bucketCount(LongAdder[] buckets, long value) {
        bucket(buckets, COUNT_THRESHOLDS, value);
    }

    private static void bucketBytes(LongAdder[] buckets, long value) {
        bucket(buckets, BYTES_THRESHOLDS, value);
    }

    private static void bucket(LongAdder[] buckets, long[] thresholds, long value) {
        for (int i = 0; i < thresholds.length; i++) {
            if (value < thresholds[i]) {
                buckets[i].increment();
                return;
            }
        }
        buckets[thresholds.length].increment();
    }

    private static LongAdder[] adders(int size) {
        LongAdder[] arr = new LongAdder[size];
        for (int i = 0; i < size; i++) {
            arr[i] = new LongAdder();
        }
        return arr;
    }

    private static void checkSchemeIndex(int schemeIndex) {
        if (schemeIndex < 0 || schemeIndex >= SCHEME_COUNT) {
            throw new IllegalArgumentException(
                "schemeIndex out of range: " + schemeIndex + "; use SCHEME_* constants (0.." + (SCHEME_COUNT - 1) + ")"
            );
        }
    }

    private static void checkOutcomeIndex(int outcomeIndex) {
        if (outcomeIndex < 0 || outcomeIndex >= OUTCOME_COUNT) {
            throw new IllegalArgumentException(
                "outcomeIndex out of range: " + outcomeIndex + "; use OUTCOME_* constants (0.." + (OUTCOME_COUNT - 1) + ")"
            );
        }
    }
}
