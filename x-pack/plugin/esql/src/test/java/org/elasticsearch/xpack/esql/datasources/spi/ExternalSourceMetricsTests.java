/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ExternalSourceMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private ExternalSourceMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new RecordingMeterRegistry();
        metrics = new ExternalSourceMetrics(registry);
    }

    public void testRecordRequestEmitsCountBytesAndDuration() {
        metrics.recordRequest(12L, 2048L, "s3");

        Measurement requests = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL);
        assertThat(requests.getLong(), equalTo(1L));
        assertThat(requests.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));

        Measurement bytes = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL);
        assertThat(bytes.getLong(), equalTo(2048L));
        assertThat(bytes.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));

        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUEST_DURATION);
        assertThat(duration.getLong(), equalTo(12L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));
    }

    public void testRecordRequestWithZeroBytesSkipsByteCounterButStillCountsAndTimes() {
        metrics.recordRequest(5L, 0L, "gcs");

        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL).getLong(), equalTo(1L));
        // The bytes counter is not touched for a zero-byte read, so no measurement is recorded.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL), hasSize(0));
        assertThat(single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUEST_DURATION).getLong(), equalTo(5L));
    }

    public void testRecordRetry() {
        metrics.recordRetry("azure");

        Measurement retry = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL);
        assertThat(retry.getLong(), equalTo(1L));
        assertThat(retry.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("azure"));
    }

    public void testCountersBridgeToRegistryOnceAttached() {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "http");

        long durationNanos = TimeUnit.MILLISECONDS.toNanos(7);
        counters.addRequest(durationNanos, 4096L);
        counters.addRetry();

        // Profile snapshot keeps working unchanged.
        assertThat(counters.snapshot().requestCount(), equalTo(1L));
        assertThat(counters.snapshot().bytesRead(), equalTo(4096L));
        assertThat(counters.snapshot().retryCount(), equalTo(1L));

        // ...and the same events reach the registry, with nanos converted to ms and the scheme attribute set.
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL).getLong(), equalTo(1L));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL).getLong(), equalTo(4096L));
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUEST_DURATION);
        assertThat(duration.getLong(), equalTo(7L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("http"));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL).getLong(), equalTo(1L));
    }

    public void testReAttachUpdatesScheme() {
        // The same object can be reused across splits and re-attached; the latest scheme must win.
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        counters.addRequest(1L, 1L);
        counters.attach(metrics, "gcs");
        counters.addRequest(1L, 1L);

        List<Measurement> requests = measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL);
        assertThat(requests, hasSize(2));
        assertThat(requests.get(0).attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));
        assertThat(requests.get(1).attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("gcs"));
    }

    public void testCountersDoNotEmitWhenNotAttached() {
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.addRequest(1234L, 4096L);
        counters.addRetry();

        // Profile snapshot still records, but nothing is published to the registry (default NOOP sink).
        assertThat(counters.snapshot().requestCount(), equalTo(1L));
        assertThat(counters.snapshot().retryCount(), equalTo(1L));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL), hasSize(0));
    }

    public void testNegativeDurationClampsToZero() {
        metrics.recordRequest(-3L, 1L, "file");
        assertThat(single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUEST_DURATION).getLong(), is(0L));
    }

    public void testCanonicalSchemeFoldsProviderAliases() {
        // The raw StoragePath scheme is what storage providers register; fold the aliases so one provider
        // is one metric series (s3a/s3n->s3, gs->gcs, wasb/wasbs->azure, https->http).
        assertThat(ExternalSourceMetrics.canonicalScheme("s3"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("s3a"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("s3n"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("gs"), equalTo("gcs"));
        assertThat(ExternalSourceMetrics.canonicalScheme("wasb"), equalTo("azure"));
        assertThat(ExternalSourceMetrics.canonicalScheme("wasbs"), equalTo("azure"));
        assertThat(ExternalSourceMetrics.canonicalScheme("http"), equalTo("http"));
        assertThat(ExternalSourceMetrics.canonicalScheme("https"), equalTo("http"));
        assertThat(ExternalSourceMetrics.canonicalScheme("file"), equalTo("file"));
        assertThat(ExternalSourceMetrics.canonicalScheme("S3A"), equalTo("s3"));
        // Unknown schemes pass through lower-cased; null is mapped to a stable sentinel.
        assertThat(ExternalSourceMetrics.canonicalScheme("ftp"), equalTo("ftp"));
        assertThat(ExternalSourceMetrics.canonicalScheme(null), equalTo("unknown"));
    }

    private List<Measurement> measurements(InstrumentType type, String name) {
        return registry.getRecorder().getMeasurements(type, name);
    }

    private Measurement single(InstrumentType type, String name) {
        List<Measurement> found = measurements(type, name);
        assertThat("expected exactly one measurement for [" + name + "]", found, hasSize(1));
        return found.get(0);
    }
}
