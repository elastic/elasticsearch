/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;

import java.util.List;
import java.util.Objects;
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

    private static final int TYPE_COUNT = Type.values().length;

    // ---- outcome vocabulary ----

    public static final int OUTCOME_SUCCESS = 0;
    public static final int OUTCOME_FAILURE = 1;
    public static final int OUTCOME_CANCELLED = 2;
    public static final int OUTCOME_COUNT = 3;
    public static final List<String> OUTCOME_NAMES = List.of("success", "failure", "cancelled");

    // ---- format vocabulary (closed set for parse.rows.by_format phone-home keys) ----

    public static final int FORMAT_PARQUET = 0;
    public static final int FORMAT_CSV = 1;
    public static final int FORMAT_TSV = 2;
    public static final int FORMAT_NDJSON = 3;
    public static final int FORMAT_ORC = 4;
    public static final int FORMAT_OTHER = 5;
    public static final int FORMAT_UNRESOLVED = 6;
    public static final int FORMAT_COUNT = 7;
    public static final String FORMAT_OTHER_NAME = "other";
    public static final String FORMAT_UNRESOLVED_NAME = "unresolved";
    public static final List<String> FORMAT_NAMES = List.of(
        "parquet",
        "csv",
        "tsv",
        "ndjson",
        "orc",
        FORMAT_OTHER_NAME,
        FORMAT_UNRESOLVED_NAME
    );
    public static final Set<String> FORMAT_NAMES_SET = Set.copyOf(FORMAT_NAMES);

    // ---- config-change vocabulary (kind × op) ----

    public static final int KIND_DATASOURCE = 0;
    public static final int KIND_DATASET = 1;
    public static final int KIND_COUNT = 2;
    public static final List<String> KIND_NAMES = List.of("datasources", "datasets");

    public static final int OP_CREATED = 0;
    public static final int OP_UPDATED = 1;
    public static final int OP_DELETED = 2;
    public static final int OP_REJECTED = 3;
    public static final int OP_COUNT = 4;
    public static final List<String> OP_NAMES = List.of("created", "updated", "deleted", "rejected");

    // ---- failure error-type vocabulary (queries.failures.by_error_type / discovery.failures.by_error_type) ----

    public static final String ERROR_TYPE_VERIFICATION = "verification";
    public static final String ERROR_TYPE_STORAGE_AUTH = "storage_auth";
    public static final String ERROR_TYPE_STORAGE_NOT_FOUND = "storage_not_found";
    public static final String ERROR_TYPE_STORAGE_THROTTLED = "storage_throttled";
    public static final String ERROR_TYPE_STORAGE_UNAVAILABLE = "storage_unavailable";
    public static final String ERROR_TYPE_FORMAT = "format";
    public static final String ERROR_TYPE_DISCOVERY = "discovery";
    public static final String ERROR_TYPE_CIRCUIT_BREAKER = "circuit_breaker";
    public static final String ERROR_TYPE_TIMEOUT = "timeout";
    public static final String ERROR_TYPE_RESOURCE_LIMIT = "resource_limit";
    public static final String ERROR_TYPE_OTHER = "other";
    /** The closed set of failure categories. Anything else is a programming error, never a runtime value. */
    public static final List<String> ERROR_TYPE_NAMES = List.of(
        ERROR_TYPE_VERIFICATION,
        ERROR_TYPE_STORAGE_AUTH,
        ERROR_TYPE_STORAGE_NOT_FOUND,
        ERROR_TYPE_STORAGE_THROTTLED,
        ERROR_TYPE_STORAGE_UNAVAILABLE,
        ERROR_TYPE_FORMAT,
        ERROR_TYPE_DISCOVERY,
        ERROR_TYPE_CIRCUIT_BREAKER,
        ERROR_TYPE_TIMEOUT,
        ERROR_TYPE_RESOURCE_LIMIT,
        ERROR_TYPE_OTHER
    );
    public static final int ERROR_TYPE_COUNT = ERROR_TYPE_NAMES.size();

    // ---- config-change rejection reason vocabulary (mirrors ConfigChangeTelemetry#REASON_*) ----

    /** The closed set of rejection reasons for a rejected data-source / dataset change. */
    public static final List<String> REJECT_REASON_NAMES = List.of(
        "validation",
        "not_found",
        "already_exists",
        "max_count",
        "unknown_type",
        "unavailable",
        "has_dependents",
        "other"
    );
    public static final int REJECT_REASON_COUNT = REJECT_REASON_NAMES.size();

    // ---- CPU-component vocabulary ----

    public static final int CPU_EXECUTION = 0;
    public static final int CPU_READ = 1;
    public static final int CPU_PLANNING = 2;
    public static final int CPU_SPLIT_DISCOVERY = 3;
    public static final int CPU_COMPONENT_COUNT = 4;
    public static final List<String> CPU_COMPONENT_NAMES = List.of("execution", "read", "planning", "split_discovery");

    // ---- bucket definitions (10 buckets each, matching ThresholdBucketer conventions) ----

    /** Time ladder (ms), mirrors TookMetrics thresholds. */
    private static final long[] TIME_THRESHOLDS = { 10, 100, 1_000, 10_000, 60_000, 600_000, 3_600_000, 36_000_000, 86_400_000 };
    public static final List<String> TIME_SUFFIXES = List.of(
        "lt_10ms",
        "lt_100ms",
        "lt_1s",
        "lt_10s",
        "lt_1m",
        "lt_10m",
        "lt_1h",
        "lt_10h",
        "lt_1d",
        "gt_1d"
    );

    /** Count ladder (files, splits — log-10 anchored at 1). */
    private static final long[] COUNT_THRESHOLDS = { 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000 };
    public static final List<String> COUNT_SUFFIXES = List.of(
        "lt_1",
        "lt_10",
        "lt_100",
        "lt_1k",
        "lt_10k",
        "lt_100k",
        "lt_1M",
        "lt_10M",
        "lt_100M",
        "gt_100M"
    );

    public static final int BUCKET_COUNT = 10;

    static {
        assert TIME_THRESHOLDS.length == BUCKET_COUNT - 1 : "TIME_THRESHOLDS length mismatch";
        assert TIME_SUFFIXES.size() == BUCKET_COUNT : "TIME_SUFFIXES size mismatch";
        assert COUNT_THRESHOLDS.length == BUCKET_COUNT - 1 : "COUNT_THRESHOLDS length mismatch";
        assert COUNT_SUFFIXES.size() == BUCKET_COUNT : "COUNT_SUFFIXES size mismatch";
        assert FORMAT_NAMES.size() == FORMAT_COUNT : "FORMAT_NAMES size mismatch";
        assert CPU_COMPONENT_NAMES.size() == CPU_COMPONENT_COUNT : "CPU_COMPONENT_NAMES size mismatch";
        for (int i = 0; i < FORMAT_COUNT; i++) {
            assert formatIndex(FORMAT_NAMES.get(i)) == i : "FORMAT_NAMES[" + i + "]=" + FORMAT_NAMES.get(i) + " does not map to index " + i;
        }
    }

    // ---- per-type counters (indexed by {@link Type#ordinal()}) ----

    private final LongAdder[] storageRequests = adders(TYPE_COUNT);
    private final LongAdder[] storageBytesRead = adders(TYPE_COUNT);
    private final LongAdder[] storageErrors = adders(TYPE_COUNT);
    private final LongAdder[] storageThrottled = adders(TYPE_COUNT);

    // ---- unattributed counters ----

    private final LongAdder storageRetries = new LongAdder();
    private final LongAdder queriesCancelled = new LongAdder();
    private final LongAdder queriesPartial = new LongAdder();
    private final LongAdder discoveryFailures = new LongAdder();
    private final LongAdder parseRows = new LongAdder();
    private final LongAdder[] parseRowsByFormat = adders(FORMAT_COUNT);
    private final LongAdder readerPoolRejected = new LongAdder();
    private final LongAdder breakerTripped = new LongAdder();

    // ---- per-outcome query counter ----

    private final LongAdder[] queries = adders(OUTCOME_COUNT);

    // ---- failure counters by error_type (query failures; discovery failures) ----

    private final LongAdder[] queryFailuresByErrorType = adders(ERROR_TYPE_COUNT);
    private final LongAdder[] discoveryFailuresByErrorType = adders(ERROR_TYPE_COUNT);

    // ---- per-component CPU counter (ns, indexed by CPU_* constants) ----

    private final LongAdder[] queryCpuNanos = adders(CPU_COMPONENT_COUNT);

    private final LongAdder[][] configChanges = new LongAdder[KIND_COUNT][];
    /** Rejected changes by [kind][reason]. Sums to {@code configChanges[kind][OP_REJECTED]} when every rejection carries a reason. */
    private final LongAdder[][] configRejectedByReason = new LongAdder[KIND_COUNT][];
    /** All changes (any op, including rejected) by [kind][type ordinal]. */
    private final LongAdder[][] configChangesByType = new LongAdder[KIND_COUNT][];
    {
        for (int k = 0; k < KIND_COUNT; k++) {
            configChanges[k] = adders(OP_COUNT);
            configRejectedByReason[k] = adders(REJECT_REASON_COUNT);
            configChangesByType[k] = adders(TYPE_COUNT);
        }
    }

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

    public void recordRequest(Type type, long durationMillis, long bytes) {
        int si = index(type);
        storageRequests[si].increment();
        if (bytes > 0) {
            storageBytesRead[si].add(bytes);
        }
        bucketTime(storageRequestDuration, Math.max(0L, durationMillis));
    }

    /**
     * Adds received bytes without incrementing {@link #storageRequests}. Pair with a prior
     * {@link #recordRequest} that booked the GET at {@code bytes = 0}.
     */
    public void recordBytes(Type type, long bytes) {
        if (bytes <= 0) {
            return;
        }
        storageBytesRead[index(type)].add(bytes);
    }

    public void recordRetry() {
        storageRetries.increment();
    }

    public void recordError(Type type) {
        storageErrors[index(type)].increment();
    }

    public void recordThrottled(Type type) {
        storageThrottled[index(type)].increment();
    }

    public void recordReadStall(long millis) {
        bucketTime(storageReadStallDuration, Math.max(0L, millis));
    }

    public void recordQuery(String outcome, long durationMillis, boolean partial) {
        recordQuery(outcome, durationMillis, partial, null);
    }

    /**
     * @param errorType one of {@link #ERROR_TYPE_NAMES}, used only when {@code outcome} is {@code failure}; {@code null}
     *                  there counts as {@link #ERROR_TYPE_OTHER}, so the per-error-type counters always sum to the
     *                  {@code failure} outcome counter
     */
    public void recordQuery(String outcome, long durationMillis, boolean partial, String errorType) {
        int oi = outcomeIndex(outcome);
        if (oi == OUTCOME_FAILURE) {
            queryFailuresByErrorType[errorTypeIndex(errorType == null ? ERROR_TYPE_OTHER : errorType)].increment();
        }
        queries[oi].increment();
        bucketTime(queryDuration, Math.max(0L, durationMillis));
        if (oi == OUTCOME_CANCELLED) {
            queriesCancelled.increment();
        }
        if (partial) {
            queriesPartial.increment();
        }
    }

    /** Accumulates per-component CPU nanos for one successful external-source query. Clamps negatives to zero. */
    public void recordQueryCpu(long execution, long read, long planning, long splitDiscovery) {
        queryCpuNanos[CPU_EXECUTION].add(Math.max(0L, execution));
        queryCpuNanos[CPU_READ].add(Math.max(0L, read));
        queryCpuNanos[CPU_PLANNING].add(Math.max(0L, planning));
        queryCpuNanos[CPU_SPLIT_DISCOVERY].add(Math.max(0L, splitDiscovery));
    }

    public void recordTimeToFirstRow(long millis) {
        bucketTime(queryTimeToFirstRow, Math.max(0L, millis));
    }

    public void recordDiscovery(long durationMillis, long filesScanned, long bytesScanned) {
        bucketTime(discoveryDuration, Math.max(0L, durationMillis));
        bucketCount(discoveryFilesScanned, Math.max(0L, filesScanned));
        bucketCount(discoveryBytesScanned, Math.max(0L, bytesScanned));
    }

    public void recordDiscoveryFailure() {
        recordDiscoveryFailure(ERROR_TYPE_OTHER);
    }

    /** @param errorType one of {@link #ERROR_TYPE_NAMES} */
    public void recordDiscoveryFailure(String errorType) {
        // Resolve the category first: an unknown one throws, and must not leave the total counted without its breakdown.
        int errorTypeIndex = errorTypeIndex(errorType);
        discoveryFailures.increment();
        discoveryFailuresByErrorType[errorTypeIndex].increment();
    }

    /**
     * @param canonicalFormat one of {@link #FORMAT_NAMES}; anything else throws {@link IllegalArgumentException}
     */
    public void recordParse(long rows, long parseDurationMillis, String canonicalFormat) {
        int idx = formatIndex(canonicalFormat);
        if (rows > 0) {
            parseRows.add(rows);
            parseRowsByFormat[idx].add(rows);
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

    /**
     * @param kind {@code datasources} or {@code datasets}
     * @param op {@code created}, {@code updated}, {@code deleted}, or {@code rejected}
     */
    public void recordConfigChange(String kind, String op) {
        recordConfigChange(kind, op, Type.UNKNOWN, null);
    }

    /**
     * @param kind   {@code datasources} or {@code datasets}
     * @param op     {@code created}, {@code updated}, {@code deleted}, or {@code rejected}
     * @param type   the storage / data-source type the change applies to
     * @param reason one of {@link #REJECT_REASON_NAMES}; only used when {@code op} is {@code rejected}, where {@code null}
     *               counts as {@code other} so the per-reason counters always sum to the {@code rejected} op counter
     */
    public void recordConfigChange(String kind, String op, Type type, String reason) {
        // Resolve every index first: an unknown value throws, and must not leave some of the counters incremented.
        int ki = kindIndex(kind);
        int oi = opIndex(op);
        int ti = index(type);
        int ri = oi == OP_REJECTED ? rejectReasonIndex(reason == null ? "other" : reason) : -1;
        configChanges[ki][oi].increment();
        configChangesByType[ki][ti].increment();
        if (ri >= 0) {
            configRejectedByReason[ki][ri].increment();
        }
    }

    // ---- snapshot accessors (read by the stats/conversion layer) ----

    public long storageRequests(Type type) {
        return storageRequests[index(type)].sum();
    }

    public long storageBytesRead(Type type) {
        return storageBytesRead[index(type)].sum();
    }

    public long storageErrors(Type type) {
        return storageErrors[index(type)].sum();
    }

    public long storageThrottled(Type type) {
        return storageThrottled[index(type)].sum();
    }

    public long storageRetries() {
        return storageRetries.sum();
    }

    /** @param outcomeIndex one of the {@code OUTCOME_*} constants */
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

    /** @param errorTypeIndex index into {@link #ERROR_TYPE_NAMES} */
    public long queryFailures(int errorTypeIndex) {
        checkErrorTypeIndex(errorTypeIndex);
        return queryFailuresByErrorType[errorTypeIndex].sum();
    }

    /** @param errorTypeIndex index into {@link #ERROR_TYPE_NAMES} */
    public long discoveryFailures(int errorTypeIndex) {
        checkErrorTypeIndex(errorTypeIndex);
        return discoveryFailuresByErrorType[errorTypeIndex].sum();
    }

    public long parseRows() {
        return parseRows.sum();
    }

    /** @param formatIndex one of the {@code FORMAT_*} constants */
    public long parseRowsByFormat(int formatIndex) {
        checkFormatIndex(formatIndex);
        return parseRowsByFormat[formatIndex].sum();
    }

    public long readerPoolRejected() {
        return readerPoolRejected.sum();
    }

    public long breakerTripped() {
        return breakerTripped.sum();
    }

    /** @param componentIndex one of the {@code CPU_*} constants */
    public long queryCpuNanos(int componentIndex) {
        checkCpuComponentIndex(componentIndex);
        return queryCpuNanos[componentIndex].sum();
    }

    /** @param kindIndex one of the {@code KIND_*} constants; @param opIndex one of the {@code OP_*} constants */
    public long configChanges(int kindIndex, int opIndex) {
        checkKindIndex(kindIndex);
        checkOpIndex(opIndex);
        return configChanges[kindIndex][opIndex].sum();
    }

    /** @param kindIndex one of the {@code KIND_*} constants; @param reasonIndex index into {@link #REJECT_REASON_NAMES} */
    public long configRejected(int kindIndex, int reasonIndex) {
        checkKindIndex(kindIndex);
        checkRejectReasonIndex(reasonIndex);
        return configRejectedByReason[kindIndex][reasonIndex].sum();
    }

    /** @param kindIndex one of the {@code KIND_*} constants; counts every op, including rejected */
    public long configChanges(int kindIndex, Type type) {
        checkKindIndex(kindIndex);
        return configChangesByType[kindIndex][index(type)].sum();
    }

    public long storageRequestDuration(int bucket) {
        checkBucketIndex(bucket);
        return storageRequestDuration[bucket].sum();
    }

    public long storageReadStallDuration(int bucket) {
        checkBucketIndex(bucket);
        return storageReadStallDuration[bucket].sum();
    }

    public long queryDuration(int bucket) {
        checkBucketIndex(bucket);
        return queryDuration[bucket].sum();
    }

    public long queryTimeToFirstRow(int bucket) {
        checkBucketIndex(bucket);
        return queryTimeToFirstRow[bucket].sum();
    }

    public long discoveryDuration(int bucket) {
        checkBucketIndex(bucket);
        return discoveryDuration[bucket].sum();
    }

    public long discoveryFilesScanned(int bucket) {
        checkBucketIndex(bucket);
        return discoveryFilesScanned[bucket].sum();
    }

    public long discoveryBytesScanned(int bucket) {
        checkBucketIndex(bucket);
        return discoveryBytesScanned[bucket].sum();
    }

    public long parseDuration(int bucket) {
        checkBucketIndex(bucket);
        return parseDuration[bucket].sum();
    }

    public long parseSplitsScanned(int bucket) {
        checkBucketIndex(bucket);
        return parseSplitsScanned[bucket].sum();
    }

    // ---- internal helpers ----

    private static int index(Type type) {
        return Objects.requireNonNull(type, "type").ordinal();
    }

    static int kindIndex(String kind) {
        return switch (kind) {
            case "datasource", "datasources" -> KIND_DATASOURCE;
            case "dataset", "datasets" -> KIND_DATASET;
            default -> throw new IllegalArgumentException("unexpected kind: " + kind);
        };
    }

    static int opIndex(String op) {
        return switch (op) {
            case "created" -> OP_CREATED;
            case "updated" -> OP_UPDATED;
            case "deleted" -> OP_DELETED;
            case "rejected" -> OP_REJECTED;
            default -> throw new IllegalArgumentException("unexpected op: " + op);
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

    static int errorTypeIndex(String errorType) {
        int i = ERROR_TYPE_NAMES.indexOf(errorType);
        if (i < 0) {
            throw new IllegalArgumentException("unexpected error type: " + errorType);
        }
        return i;
    }

    static int rejectReasonIndex(String reason) {
        int i = REJECT_REASON_NAMES.indexOf(reason);
        if (i < 0) {
            throw new IllegalArgumentException("unexpected rejection reason: " + reason);
        }
        return i;
    }

    static int formatIndex(String canonicalFormat) {
        return switch (canonicalFormat) {
            case "parquet" -> FORMAT_PARQUET;
            case "csv" -> FORMAT_CSV;
            case "tsv" -> FORMAT_TSV;
            case "ndjson" -> FORMAT_NDJSON;
            case "orc" -> FORMAT_ORC;
            case FORMAT_OTHER_NAME -> FORMAT_OTHER;
            case FORMAT_UNRESOLVED_NAME -> FORMAT_UNRESOLVED;
            default -> throw new IllegalArgumentException("unexpected canonical format: " + canonicalFormat);
        };
    }

    private static void bucketTime(LongAdder[] buckets, long value) {
        bucket(buckets, TIME_THRESHOLDS, value);
    }

    private static void bucketCount(LongAdder[] buckets, long value) {
        bucket(buckets, COUNT_THRESHOLDS, value);
    }

    // Mirrors ThresholdBucketer.count() — intentional duplication: DataSourceUsageAccumulator avoids xpack-core's
    // Counters (which ThresholdBucketer depends on).
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

    private static void checkKindIndex(int kindIndex) {
        if (kindIndex < 0 || kindIndex >= KIND_COUNT) {
            throw new IllegalArgumentException(
                "kindIndex out of range: " + kindIndex + "; use KIND_* constants (0.." + (KIND_COUNT - 1) + ")"
            );
        }
    }

    private static void checkOpIndex(int opIndex) {
        if (opIndex < 0 || opIndex >= OP_COUNT) {
            throw new IllegalArgumentException("opIndex out of range: " + opIndex + "; use OP_* constants (0.." + (OP_COUNT - 1) + ")");
        }
    }

    private static void checkErrorTypeIndex(int errorTypeIndex) {
        if (errorTypeIndex < 0 || errorTypeIndex >= ERROR_TYPE_COUNT) {
            throw new IllegalArgumentException(
                "errorTypeIndex out of range: " + errorTypeIndex + "; valid range is 0.." + (ERROR_TYPE_COUNT - 1)
            );
        }
    }

    private static void checkRejectReasonIndex(int reasonIndex) {
        if (reasonIndex < 0 || reasonIndex >= REJECT_REASON_COUNT) {
            throw new IllegalArgumentException(
                "reasonIndex out of range: " + reasonIndex + "; valid range is 0.." + (REJECT_REASON_COUNT - 1)
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

    private static void checkFormatIndex(int formatIndex) {
        if (formatIndex < 0 || formatIndex >= FORMAT_COUNT) {
            throw new IllegalArgumentException(
                "formatIndex out of range: " + formatIndex + "; use FORMAT_* constants (0.." + (FORMAT_COUNT - 1) + ")"
            );
        }
    }

    private static void checkBucketIndex(int bucket) {
        if (bucket < 0 || bucket >= BUCKET_COUNT) {
            throw new IllegalArgumentException("bucket out of range: " + bucket + "; valid range is 0.." + (BUCKET_COUNT - 1));
        }
    }

    private static void checkCpuComponentIndex(int idx) {
        if (idx < 0 || idx >= CPU_COMPONENT_COUNT) {
            throw new IllegalArgumentException(
                "componentIndex out of range: " + idx + "; use CPU_* constants (0.." + (CPU_COMPONENT_COUNT - 1) + ")"
            );
        }
    }
}
