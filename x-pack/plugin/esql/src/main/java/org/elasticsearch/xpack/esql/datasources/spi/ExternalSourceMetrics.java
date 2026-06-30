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

    /**
     * Storage scheme dimension, normalised to one canonical token per provider via
     * {@link #canonicalScheme(String)}: {@code s3}, {@code gcs}, {@code azure}, {@code http}, {@code file}.
     */
    public static final String SCHEME_ATTRIBUTE = "es_datasource_scheme";

    /**
     * No-op holder backed by {@link MeterRegistry#NOOP}, used where no node registry is available
     * (decorators with no attached holder, tests) so call sites never branch on null.
     */
    public static final ExternalSourceMetrics NOOP = new ExternalSourceMetrics(MeterRegistry.NOOP);

    private final LongCounter requestsTotal;
    private final LongHistogram requestDuration;
    private final LongCounter bytesReadTotal;
    private final LongCounter retriesTotal;

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
