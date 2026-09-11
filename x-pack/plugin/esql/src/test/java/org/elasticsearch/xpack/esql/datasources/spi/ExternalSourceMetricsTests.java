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
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ExternalSourceMetricsTests extends ESTestCase {

    private RecordingMeterRegistry registry;
    private ExternalSourceMetrics metrics;

    @Before
    public void initMetrics() {
        registry = new RecordingMeterRegistry();
        metrics = new ExternalSourceMetrics(registry);
    }

    public void testRecordRequestEmitsCountBytesAndDuration() {
        metrics.recordRequest(12L, 2048L, "s3");

        Measurement requests = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL);
        assertThat(requests.getLong(), equalTo(1L));
        assertThat(requests.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));

        Measurement bytes = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL);
        assertThat(bytes.getLong(), equalTo(2048L));
        assertThat(bytes.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));

        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUESTS_DURATION);
        assertThat(duration.getLong(), equalTo(12L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
    }

    public void testRecordRequestWithZeroBytesSkipsByteCounterButStillCountsAndTimes() {
        metrics.recordRequest(5L, 0L, "gcs");

        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL).getLong(), equalTo(1L));
        // The bytes counter is not touched for a zero-byte read, so no measurement is recorded.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL), hasSize(0));
        assertThat(single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUESTS_DURATION).getLong(), equalTo(5L));
    }

    public void testRecordRetry() {
        metrics.recordRetry("azure");

        Measurement retry = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_RETRIES_TOTAL);
        assertThat(retry.getLong(), equalTo(1L));
        assertThat(retry.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("azure"));
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
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUESTS_DURATION);
        assertThat(duration.getLong(), equalTo(7L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("http"));
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
        assertThat(requests.get(0).attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(requests.get(1).attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("gcs"));
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
        assertThat(single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_REQUESTS_DURATION).getLong(), is(0L));
    }

    public void testRecordError() {
        metrics.recordError("s3");
        Measurement m = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL);
        assertThat(m.getLong(), equalTo(1L));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
    }

    public void testRecordThrottled() {
        metrics.recordThrottled("gcs");
        Measurement m = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL);
        assertThat(m.getLong(), equalTo(1L));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("gcs"));
    }

    public void testRecordReadStall() {
        metrics.recordReadStall(1200L, "azure");
        Measurement m = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION);
        assertThat(m.getLong(), equalTo(1200L));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("azure"));
    }

    public void testRecordReadStallClampsNegative() {
        metrics.recordReadStall(-5L, "s3");
        assertThat(single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION).getLong(), is(0L));
    }

    public void testRecordQuerySuccess() {
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 340L, false);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.getLong(), equalTo(1L));
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo("success"));
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_DURATION);
        assertThat(duration.getLong(), equalTo(340L));
        // The duration histogram carries the same outcome dimension as the counter, so latency can be split by outcome.
        assertThat(duration.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo("success"));
        // Neither the cancelled nor the partial counter is touched for a plain success.
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_PARTIAL_TOTAL), hasSize(0));
    }

    public void testRecordQueryCancelledAlsoBumpsCancelledCounter() {
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_CANCELLED, 10L, false);

        Measurement total = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL);
        assertThat(total.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE), equalTo("cancelled"));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL).getLong(), equalTo(1L));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_PARTIAL_TOTAL), hasSize(0));
    }

    public void testRecordQueryPartialAlsoBumpsPartialCounter() {
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 55L, true);

        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_PARTIAL_TOTAL).getLong(), equalTo(1L));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_CANCELLED_TOTAL), hasSize(0));
    }

    public void testRecordTimeToFirstRow() {
        metrics.recordTimeToFirstRow(42L, "s3", "parquet");
        Measurement m = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_TIME_TO_FIRST_ROW);
        assertThat(m.getLong(), equalTo(42L));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(m.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("parquet"));
    }

    public void testRecordDiscovery() {
        // Raw "s3a" folds to the canonical "s3" series inside the record method.
        metrics.recordDiscovery(75L, 12L, 4096L, "s3a");
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_DURATION);
        assertThat(duration.getLong(), equalTo(75L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        Measurement files = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_FILES_SCANNED);
        assertThat(files.getLong(), equalTo(12L));
        assertThat(files.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        Measurement bytes = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_BYTES_SCANNED);
        assertThat(bytes.getLong(), equalTo(4096L));
        assertThat(bytes.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
    }

    public void testRecordDiscoveryFailure() {
        metrics.recordDiscoveryFailure();
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.DISCOVERY_FAILURES_TOTAL).getLong(), equalTo(1L));
    }

    public void testRecordParse() {
        metrics.recordParse(1000L, 88L, "gcs", "csv");
        Measurement rows = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.PARSE_ROWS_TOTAL);
        assertThat(rows.getLong(), equalTo(1000L));
        assertThat(rows.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("gcs"));
        assertThat(rows.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("csv"));
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.PARSE_DURATION);
        assertThat(duration.getLong(), equalTo(88L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("gcs"));
        assertThat(duration.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("csv"));
    }

    public void testRecordParseWithZeroRowsSkipsRowCounterButStillTimes() {
        metrics.recordParse(0L, 9L, "file", "csv");
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.PARSE_ROWS_TOTAL), hasSize(0));
        Measurement duration = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.PARSE_DURATION);
        assertThat(duration.getLong(), equalTo(9L));
        assertThat(duration.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("local"));
        assertThat(duration.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("csv"));
    }

    public void testRecordSplitsScanned() {
        metrics.recordSplitsScanned(7L, "azure", "orc");
        Measurement m = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.PARSE_SPLITS_SCANNED);
        assertThat(m.getLong(), equalTo(7L));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("azure"));
        assertThat(m.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("orc"));
    }

    public void testRecordPoolRejected() {
        metrics.recordPoolRejected();
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.READER_POOL_REJECTED_TOTAL).getLong(), equalTo(1L));
    }

    public void testRecordBreakerTripped() {
        metrics.recordBreakerTripped();
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.BREAKER_TRIPPED_TOTAL).getLong(), equalTo(1L));
    }

    public void testStorageCountersBridgeErrorThrottledAndStall() {
        // The storage recorder (StorageObjectMetricsCounters) publishes the terminal give-up / throttle / stall
        // events to the registry once a sink is attached, mirroring the request/retry bridge.
        StorageObjectMetricsCounters counters = new StorageObjectMetricsCounters();
        counters.attach(metrics, "s3");
        counters.addError();
        counters.addThrottled();
        counters.addReadStall(900L);

        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL).getLong(), equalTo(1L));
        assertThat(single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL).getLong(), equalTo(1L));
        Measurement stall = single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION);
        assertThat(stall.getLong(), equalTo(900L));
        assertThat(stall.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
    }

    public void testStorageCountersSkipZeroStallAndEmitNothingWhenUnattached() {
        StorageObjectMetricsCounters attached = new StorageObjectMetricsCounters();
        attached.attach(metrics, "gcs");
        // A read that never backed off records no stall (avoids flooding the histogram with zeros).
        attached.addReadStall(0L);
        assertThat(measurements(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION), hasSize(0));

        // With no sink attached nothing reaches the registry.
        StorageObjectMetricsCounters unattached = new StorageObjectMetricsCounters();
        unattached.addError();
        unattached.addThrottled();
        unattached.addReadStall(50L);
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_ERRORS_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL), hasSize(0));
        assertThat(measurements(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.STORAGE_READ_STALL_DURATION), hasSize(0));
    }

    public void testRecordConfigChangeCreatedHasNoReason() {
        metrics.recordConfigChange("datasource", "created", "s3", null);
        Measurement m = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.CONFIG_CHANGES_TOTAL);
        assertThat(m.getLong(), equalTo(1L));
        assertThat(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE), equalTo("datasource"));
        assertThat(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE), equalTo("created"));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("s3"));
        assertThat(m.attributes().containsKey(ExternalSourceMetrics.REASON_ATTRIBUTE), equalTo(false));
    }

    public void testRecordConfigChangeRejectedCarriesReason() {
        metrics.recordConfigChange("dataset", "rejected", "unknown", "validation");
        Measurement m = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.CONFIG_CHANGES_TOTAL);
        assertThat(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE), equalTo("dataset"));
        assertThat(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE), equalTo("rejected"));
        assertThat(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("unknown"));
        assertThat(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE), equalTo("validation"));
    }

    public void testCanonicalSchemeFoldsProviderAliases() {
        // The raw StoragePath scheme is what storage providers register; fold the aliases so one provider
        // is one metric series (s3a/s3n->s3, gs->gcs, wasb/wasbs->azure, https->http, file->local).
        assertThat(ExternalSourceMetrics.canonicalScheme("s3"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("s3a"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("s3n"), equalTo("s3"));
        assertThat(ExternalSourceMetrics.canonicalScheme("gs"), equalTo("gcs"));
        assertThat(ExternalSourceMetrics.canonicalScheme("wasb"), equalTo("azure"));
        assertThat(ExternalSourceMetrics.canonicalScheme("wasbs"), equalTo("azure"));
        assertThat(ExternalSourceMetrics.canonicalScheme("http"), equalTo("http"));
        assertThat(ExternalSourceMetrics.canonicalScheme("https"), equalTo("http"));
        assertThat(ExternalSourceMetrics.canonicalScheme("file"), equalTo("local"));
        assertThat(ExternalSourceMetrics.canonicalScheme("S3A"), equalTo("s3"));
        // Unknown schemes clamp to the closed unknown token; null is the same sentinel.
        assertThat(ExternalSourceMetrics.canonicalScheme("ftp"), equalTo("unknown"));
        assertThat(ExternalSourceMetrics.canonicalScheme(null), equalTo("unknown"));
    }

    public void testTypeAttributeFoldsFileToLocal() {
        // Both APM and phone-home fold file → local via DataSourceTelemetryVocabulary.Type.
        assertThat(ExternalSourceMetrics.canonicalScheme("file"), equalTo("local"));
        assertThat(ExternalSourceMetrics.canonicalScheme("FILE"), equalTo("local"));
        assertThat(ExternalSourceMetrics.canonicalScheme("s3a"), equalTo("s3"));

        metrics.recordRequest(1L, 1L, "file");
        Measurement request = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL);
        assertThat(request.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo("local"));
        assertThat(request.attributes().containsKey("es_datasource_scheme"), equalTo(false));
    }

    public void testCanonicalFormatClampsUnknownAndBlank() {
        assertThat(ExternalSourceMetrics.canonicalFormat("csv"), equalTo("csv"));
        assertThat(ExternalSourceMetrics.canonicalFormat("CSV"), equalTo("csv"));
        assertThat(ExternalSourceMetrics.canonicalFormat("parquet"), equalTo("parquet"));
        assertThat(ExternalSourceMetrics.canonicalFormat("gz"), equalTo("other"));
        assertThat(ExternalSourceMetrics.canonicalFormat(null), equalTo("unresolved"));
        assertThat(ExternalSourceMetrics.canonicalFormat("  "), equalTo("unresolved"));
    }

    public void testStorageDiscoveryAndQueriesDoNotCarryFormat() {
        metrics.recordRequest(1L, 1L, "s3");
        metrics.recordDiscovery(1L, 1L, 1L, "s3");
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 1L, false);
        assertThat(
            single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL).attributes()
                .containsKey(ExternalSourceMetrics.FORMAT_ATTRIBUTE),
            equalTo(false)
        );
        assertThat(
            single(InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_DURATION).attributes()
                .containsKey(ExternalSourceMetrics.FORMAT_ATTRIBUTE),
            equalTo(false)
        );
        assertThat(
            single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.QUERIES_TOTAL).attributes()
                .containsKey(ExternalSourceMetrics.FORMAT_ATTRIBUTE),
            equalTo(false)
        );
    }

    public void testRecordParseClampsUnknownFormatToOther() {
        metrics.recordParse(3L, 1L, "s3", "gz");
        Measurement rows = single(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.PARSE_ROWS_TOTAL);
        assertThat(rows.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE), equalTo("other"));
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
