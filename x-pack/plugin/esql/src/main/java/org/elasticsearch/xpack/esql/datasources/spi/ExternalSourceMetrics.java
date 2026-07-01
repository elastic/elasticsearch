/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Locale;
import java.util.Map;

/**
 * Node-level holder for the ES|QL external-data-source operational metrics, published through the
 * node {@link MeterRegistry} (APM/OTLP) for serverless dashboards and alerts. Mirrors the
 * {@code RepositoriesMetrics} / {@code PlanTelemetryManager} idiom: instruments are registered once in
 * the constructor and recorded at the event.
 * <p>
 * This first cut covers the object-store read layer. The per-{@code StorageObject}
 * {@link StorageObjectMetricsCounters} already tracks request count, request nanos, bytes read and
 * retries for the query profile; this holder bridges those same events to the registry. The counters
 * call {@link #recordRequest} / {@link #recordRetry} once a metrics holder is attached to them (see
 * {@code StorageObject#attachMetrics}); when none is attached they use {@link #NOOP}.
 */
public final class ExternalSourceMetrics {

    /** One completed object-store read request on a scanned data object. */
    public static final String STORAGE_REQUESTS_TOTAL = "es.esql.datasources.storage.requests.total";

    /** Wall time of a single object-store read request, in milliseconds. */
    public static final String STORAGE_REQUEST_DURATION = "es.esql.datasources.storage.requests.duration.histogram";

    /** Bytes returned by object-store reads, before decompression. */
    public static final String STORAGE_BYTES_READ_TOTAL = "es.esql.datasources.storage.bytes_read.total";

    /** Automatic retries issued by the cross-provider retry decorator. */
    public static final String STORAGE_RETRIES_TOTAL = "es.esql.datasources.storage.retries.total";

    /** Object-store reads that exhausted the retry policy and gave up with a terminal failure. */
    public static final String STORAGE_ERRORS_TOTAL = "es.esql.datasources.storage.errors.total";

    /** Object-store reads whose terminal failure was a provider throttling / rate-limit response. */
    public static final String STORAGE_THROTTLED_TOTAL = "es.esql.datasources.storage.throttled.total";

    /** Time an object-store read spent sleeping in retry backoff before it completed or gave up, in milliseconds. */
    public static final String STORAGE_READ_STALL_DURATION = "es.esql.datasources.storage.read_stall.duration.histogram";

    /**
     * One completed external-source query at the coordinator (dimensioned by {@link #OUTCOME_ATTRIBUTE}). Counts
     * queries whose ANALYZED plan contained an external source; a query that fails DURING analysis (before the
     * external-source flag is set) is not attributed here — its discovery failure is captured by
     * {@link #DISCOVERY_FAILURES_TOTAL} instead.
     */
    public static final String QUERIES_TOTAL = "es.esql.datasources.queries.total";

    /** Wall time of a completed external-source query, in milliseconds. */
    public static final String QUERY_DURATION = "es.esql.datasources.query.duration.histogram";

    /** External-source queries that ended in cancellation. */
    public static final String QUERIES_CANCELLED_TOTAL = "es.esql.datasources.queries.cancelled.total";

    /** External-source queries that returned partial results. */
    public static final String QUERIES_PARTIAL_TOTAL = "es.esql.datasources.queries.partial.total";

    /**
     * Time from an external-source scan operator's start to its first emitted page, in milliseconds. A per-scan
     * proxy for time-to-first-row: it is measured per scan operator (captured at operator construction, after
     * planning and discovery), so a single query with several external-source scans records several observations.
     */
    public static final String QUERY_TIME_TO_FIRST_ROW = "es.esql.datasources.query.time_to_first_row.histogram";

    /** Wall time of external-source discovery (glob expansion / listing) for one query, in milliseconds. */
    public static final String DISCOVERY_DURATION = "es.esql.datasources.discovery.duration.histogram";

    /** Number of files discovered by external-source listing for one query. */
    public static final String DISCOVERY_FILES_SCANNED = "es.esql.datasources.discovery.files_scanned.histogram";

    /** Estimated bytes across the files discovered by external-source listing for one query. */
    public static final String DISCOVERY_BYTES_SCANNED = "es.esql.datasources.discovery.bytes_scanned.histogram";

    /** External-source discovery attempts that failed to resolve. */
    public static final String DISCOVERY_FAILURES_TOTAL = "es.esql.datasources.discovery.failures.total";

    /** Rows parsed out of a format reader by the external-source scan operator. */
    public static final String PARSE_ROWS_TOTAL = "es.esql.datasources.parse.rows.total";

    /** Wall time an external-source scan operator spent reading/parsing an object, in milliseconds. */
    public static final String PARSE_DURATION = "es.esql.datasources.parse.duration.histogram";

    /** Number of splits scanned by an external-source scan operator. */
    public static final String DISCOVERY_SPLITS_SCANNED = "es.esql.datasources.discovery.splits_scanned.histogram";

    /** Reader-pool submissions rejected because the parsing executor was saturated. */
    public static final String READER_POOL_REJECTED_TOTAL = "es.esql.datasources.reader.pool.rejected.total";

    /** External-source reads rejected by a circuit breaker. */
    public static final String BREAKER_TRIPPED_TOTAL = "es.esql.datasources.breaker.tripped.total";

    /**
     * Storage scheme dimension, normalised to one canonical token per provider via
     * {@link #canonicalScheme(String)}: {@code s3}, {@code gcs}, {@code azure}, {@code http}, {@code file}.
     */
    public static final String SCHEME_ATTRIBUTE = "es_datasource_scheme";

    /**
     * Query-outcome dimension, a closed low-cardinality set: {@code success}, {@code failure}, {@code cancelled}.
     */
    public static final String OUTCOME_ATTRIBUTE = "es_datasource_outcome";

    /** Successful query outcome. */
    public static final String OUTCOME_SUCCESS = "success";

    /** Failed query outcome (non-cancellation error). */
    public static final String OUTCOME_FAILURE = "failure";

    /** Cancelled query outcome. */
    public static final String OUTCOME_CANCELLED = "cancelled";

    /**
     * No-op holder backed by {@link MeterRegistry#NOOP}, used where no node registry is available
     * (decorators with no attached holder, tests) so call sites never branch on null.
     */
    public static final ExternalSourceMetrics NOOP = new ExternalSourceMetrics(MeterRegistry.NOOP);

    private final LongCounter requestsTotal;
    private final LongHistogram requestDuration;
    private final LongCounter bytesReadTotal;
    private final LongCounter retriesTotal;
    private final LongCounter errorsTotal;
    private final LongCounter throttledTotal;
    private final LongHistogram readStallDuration;
    private final LongCounter queriesTotal;
    private final LongHistogram queryDuration;
    private final LongCounter queriesCancelledTotal;
    private final LongCounter queriesPartialTotal;
    private final LongHistogram queryTimeToFirstRow;
    private final LongHistogram discoveryDuration;
    private final LongHistogram discoveryFilesScanned;
    private final LongHistogram discoveryBytesScanned;
    private final LongCounter discoveryFailuresTotal;
    private final LongCounter parseRowsTotal;
    private final LongHistogram parseDuration;
    private final LongHistogram discoverySplitsScanned;
    private final LongCounter readerPoolRejectedTotal;
    private final LongCounter breakerTrippedTotal;

    public ExternalSourceMetrics(MeterRegistry meterRegistry) {
        this.requestsTotal = meterRegistry.registerLongCounter(
            STORAGE_REQUESTS_TOTAL,
            "Object-store read requests on data objects scanned by ES|QL external data sources "
                + "(excludes resolution, listing and metadata calls)",
            "unit"
        );
        this.requestDuration = meterRegistry.registerLongHistogram(
            STORAGE_REQUEST_DURATION,
            "Wall time of a single ES|QL external object-store read request",
            "ms"
        );
        this.bytesReadTotal = meterRegistry.registerLongCounter(
            STORAGE_BYTES_READ_TOTAL,
            "Bytes read from object storage by ES|QL external data sources, before decompression",
            "bytes"
        );
        this.retriesTotal = meterRegistry.registerLongCounter(
            STORAGE_RETRIES_TOTAL,
            "Automatic retries of object-store reads by ES|QL external data sources",
            "unit"
        );
        this.errorsTotal = meterRegistry.registerLongCounter(
            STORAGE_ERRORS_TOTAL,
            "Object-store reads by ES|QL external data sources that exhausted retries and gave up",
            "unit"
        );
        this.throttledTotal = meterRegistry.registerLongCounter(
            STORAGE_THROTTLED_TOTAL,
            "Object-store reads by ES|QL external data sources that terminally failed with a provider throttling response",
            "unit"
        );
        this.readStallDuration = meterRegistry.registerLongHistogram(
            STORAGE_READ_STALL_DURATION,
            "Time an ES|QL external object-store read spent sleeping in retry backoff",
            "ms"
        );
        this.queriesTotal = meterRegistry.registerLongCounter(
            QUERIES_TOTAL,
            "ES|QL queries that scanned an external data source, dimensioned by outcome",
            "unit"
        );
        this.queryDuration = meterRegistry.registerLongHistogram(
            QUERY_DURATION,
            "Wall time of an ES|QL query that scanned an external data source",
            "ms"
        );
        this.queriesCancelledTotal = meterRegistry.registerLongCounter(
            QUERIES_CANCELLED_TOTAL,
            "ES|QL external-data-source queries that ended in cancellation",
            "unit"
        );
        this.queriesPartialTotal = meterRegistry.registerLongCounter(
            QUERIES_PARTIAL_TOTAL,
            "ES|QL external-data-source queries that returned partial results",
            "unit"
        );
        this.queryTimeToFirstRow = meterRegistry.registerLongHistogram(
            QUERY_TIME_TO_FIRST_ROW,
            "Time from an ES|QL external-data-source scan operator's start to its first emitted page",
            "ms"
        );
        this.discoveryDuration = meterRegistry.registerLongHistogram(
            DISCOVERY_DURATION,
            "Wall time of ES|QL external-data-source discovery (glob expansion / listing) for one query",
            "ms"
        );
        this.discoveryFilesScanned = meterRegistry.registerLongHistogram(
            DISCOVERY_FILES_SCANNED,
            "Files discovered by ES|QL external-data-source listing for one query",
            "unit"
        );
        this.discoveryBytesScanned = meterRegistry.registerLongHistogram(
            DISCOVERY_BYTES_SCANNED,
            "Estimated bytes across the files discovered by ES|QL external-data-source listing for one query",
            "bytes"
        );
        this.discoveryFailuresTotal = meterRegistry.registerLongCounter(
            DISCOVERY_FAILURES_TOTAL,
            "ES|QL external-data-source discovery attempts that failed to resolve",
            "unit"
        );
        this.parseRowsTotal = meterRegistry.registerLongCounter(
            PARSE_ROWS_TOTAL,
            "Rows parsed out of a format reader by an ES|QL external-data-source scan operator",
            "unit"
        );
        this.parseDuration = meterRegistry.registerLongHistogram(
            PARSE_DURATION,
            "Wall time an ES|QL external-data-source scan operator spent reading and parsing an object",
            "ms"
        );
        this.discoverySplitsScanned = meterRegistry.registerLongHistogram(
            DISCOVERY_SPLITS_SCANNED,
            "Splits scanned by an ES|QL external-data-source scan operator",
            "unit"
        );
        this.readerPoolRejectedTotal = meterRegistry.registerLongCounter(
            READER_POOL_REJECTED_TOTAL,
            "ES|QL external-data-source parsing submissions rejected because the reader pool was saturated",
            "unit"
        );
        this.breakerTrippedTotal = meterRegistry.registerLongCounter(
            BREAKER_TRIPPED_TOTAL,
            "ES|QL external-data-source reads rejected by a circuit breaker",
            "unit"
        );
    }

    /**
     * Records one completed read request: increments the request count, adds the bytes read, and
     * observes the request duration. {@code scheme} is the low-cardinality storage scheme dimension.
     */
    public void recordRequest(long durationMillis, long bytes, String scheme) {
        Map<String, Object> attributes = Map.of(SCHEME_ATTRIBUTE, scheme);
        requestsTotal.incrementBy(1, attributes);
        if (bytes > 0) {
            bytesReadTotal.incrementBy(bytes, attributes);
        }
        requestDuration.record(Math.max(0L, durationMillis), attributes);
    }

    /** Records one automatic retry against the given storage {@code scheme}. */
    public void recordRetry(String scheme) {
        retriesTotal.incrementBy(1, Map.of(SCHEME_ATTRIBUTE, scheme));
    }

    /** Records one object-store read that exhausted retries and gave up terminally on the given {@code scheme}. */
    public void recordError(String scheme) {
        errorsTotal.incrementBy(1, Map.of(SCHEME_ATTRIBUTE, scheme));
    }

    /** Records one object-store read whose terminal failure was a provider throttling response on the given {@code scheme}. */
    public void recordThrottled(String scheme) {
        throttledTotal.incrementBy(1, Map.of(SCHEME_ATTRIBUTE, scheme));
    }

    /** Records the total time an object-store read spent in retry backoff on the given {@code scheme}, in milliseconds. */
    public void recordReadStall(long millis, String scheme) {
        readStallDuration.record(Math.max(0L, millis), Map.of(SCHEME_ATTRIBUTE, scheme));
    }

    /**
     * Records one completed external-source query: increments {@link #QUERIES_TOTAL} tagged with {@code outcome},
     * observes {@link #QUERY_DURATION} carrying the same {@code outcome} (so latency can be split by
     * success/failure/cancelled), and increments {@link #QUERIES_CANCELLED_TOTAL} when the outcome is
     * {@link #OUTCOME_CANCELLED} and {@link #QUERIES_PARTIAL_TOTAL} when {@code partial} is set.
     * <p>
     * Attribution scope: this is only reached for queries whose ANALYZED plan contained an external source. A
     * query that fails during analysis (before the external-source flag is set) is not counted here; its discovery
     * failure is captured by {@link #recordDiscoveryFailure()} / {@link #DISCOVERY_FAILURES_TOTAL}.
     */
    public void recordQuery(String outcome, long durationMillis, boolean partial) {
        Map<String, Object> attributes = Map.of(OUTCOME_ATTRIBUTE, outcome);
        queriesTotal.incrementBy(1, attributes);
        queryDuration.record(Math.max(0L, durationMillis), attributes);
        if (OUTCOME_CANCELLED.equals(outcome)) {
            queriesCancelledTotal.incrementBy(1);
        }
        if (partial) {
            queriesPartialTotal.incrementBy(1);
        }
    }

    /**
     * Records the time from an external-source scan operator's start to its first emitted page, in milliseconds,
     * on the given storage {@code scheme}.
     */
    public void recordTimeToFirstRow(long millis, String scheme) {
        queryTimeToFirstRow.record(Math.max(0L, millis), schemeAttributes(scheme));
    }

    /**
     * Records one external-source discovery pass: its wall time, the file count and the estimated byte total, on
     * the given storage {@code scheme}.
     */
    public void recordDiscovery(long durationMillis, long filesScanned, long bytesScanned, String scheme) {
        Map<String, Object> attributes = schemeAttributes(scheme);
        discoveryDuration.record(Math.max(0L, durationMillis), attributes);
        discoveryFilesScanned.record(Math.max(0L, filesScanned), attributes);
        discoveryBytesScanned.record(Math.max(0L, bytesScanned), attributes);
    }

    /** Records one external-source discovery attempt that failed to resolve. */
    public void recordDiscoveryFailure() {
        discoveryFailuresTotal.incrementBy(1);
    }

    /**
     * Records the rows parsed and the read/parse wall time of one external-source scan operator, in milliseconds,
     * on the given storage {@code scheme}.
     */
    public void recordParse(long rows, long parseDurationMillis, String scheme) {
        Map<String, Object> attributes = schemeAttributes(scheme);
        if (rows > 0) {
            parseRowsTotal.incrementBy(rows, attributes);
        }
        parseDuration.record(Math.max(0L, parseDurationMillis), attributes);
    }

    /** Records the number of splits scanned by one external-source scan operator, on the given storage {@code scheme}. */
    public void recordSplitsScanned(long splits, String scheme) {
        discoverySplitsScanned.record(Math.max(0L, splits), schemeAttributes(scheme));
    }

    /** Records one reader-pool submission rejected because the parsing executor was saturated. */
    public void recordPoolRejected() {
        readerPoolRejectedTotal.incrementBy(1);
    }

    /** Records one external-source read rejected by a circuit breaker. */
    public void recordBreakerTripped() {
        breakerTrippedTotal.incrementBy(1);
    }

    /** Builds the single-entry {@link #SCHEME_ATTRIBUTE} attribute map, canonicalising the raw {@code scheme}. */
    private static Map<String, Object> schemeAttributes(String scheme) {
        return Map.of(SCHEME_ATTRIBUTE, canonicalScheme(scheme));
    }

    /**
     * Folds a raw {@link StoragePath#scheme() storage-path scheme} into the single canonical token used
     * for the {@link #SCHEME_ATTRIBUTE} dimension, so provider aliases ({@code s3a}/{@code s3n},
     * {@code wasb}/{@code wasbs}, {@code https}) and the bucket-prefix form ({@code gs}) do not fragment a
     * provider across multiple metric series. Unknown schemes pass through lower-cased.
     */
    public static String canonicalScheme(String scheme) {
        if (scheme == null) {
            return "unknown";
        }
        String lower = scheme.toLowerCase(Locale.ROOT);
        return switch (lower) {
            case "s3", "s3a", "s3n" -> "s3";
            case "gs", "gcs" -> "gcs";
            case "wasb", "wasbs", "azure" -> "azure";
            case "http", "https" -> "http";
            case "file" -> "file";
            default -> lower;
        };
    }
}
