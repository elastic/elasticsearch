/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.DataSourceCounters;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;

import static org.hamcrest.Matchers.equalTo;

public class DataSourceUsageAccumulatorTests extends ESTestCase {

    public void testRecordRequestIncrementsByScheme() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest(Type.S3, 50L, 1024L);
        acc.recordRequest(Type.S3, 10L, 0L);
        acc.recordRequest(Type.GCS, 20L, 2048L);

        assertThat(acc.storageRequests(Type.S3), equalTo(2L));
        assertThat(acc.storageBytesRead(Type.S3), equalTo(1024L));
        assertThat(acc.storageRequests(Type.GCS), equalTo(1L));
        assertThat(acc.storageBytesRead(Type.GCS), equalTo(2048L));
        assertThat(acc.storageRequests(Type.UNKNOWN), equalTo(0L));
    }

    public void testRecordRequestZeroBytesDoesNotIncrementBytesRead() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest(Type.LOCAL, 5L, 0L);
        assertThat(acc.storageBytesRead(Type.LOCAL), equalTo(0L));
        assertThat(acc.storageRequests(Type.LOCAL), equalTo(1L));
    }

    public void testRecordRequestPopulatesTimeBucket() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest(Type.S3, 5L, 0L);  // < 10ms → bucket 0
        assertThat(acc.storageRequestDuration(0), equalTo(1L));
        for (int b = 1; b < DataSourceUsageAccumulator.BUCKET_COUNT; b++) {
            assertThat(acc.storageRequestDuration(b), equalTo(0L));
        }
    }

    public void testRecordRetryAndErrorAndThrottled() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRetry();
        acc.recordRetry();
        acc.recordError(Type.S3);
        acc.recordThrottled(Type.GCS);

        assertThat(acc.storageRetries(), equalTo(2L));
        assertThat(acc.storageErrors(Type.S3), equalTo(1L));
        assertThat(acc.storageThrottled(Type.GCS), equalTo(1L));
    }

    public void testRecordQueryByOutcome() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordQuery("success", 100L, false);
        acc.recordQuery("failure", 200L, false);
        acc.recordQuery("cancelled", 50L, false);

        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS), equalTo(1L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_FAILURE), equalTo(1L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_CANCELLED), equalTo(1L));
        assertThat(acc.queriesCancelled(), equalTo(1L));
        assertThat(acc.queriesPartial(), equalTo(0L));
    }

    public void testRecordQueryPartial() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordQuery("success", 100L, true);
        assertThat(acc.queriesPartial(), equalTo(1L));
        assertThat(acc.queriesCancelled(), equalTo(0L));
    }

    public void testRecordDiscoveryPopulatesThreeFamilies() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordDiscovery(30L, 5L, 512L);
        // duration 30ms → bucket 2 (lt_100ms, index 1)
        assertThat(acc.discoveryDuration(1), equalTo(1L));
        // files 5 → lt_10 bucket (index 1)
        assertThat(acc.discoveryFilesScanned(1), equalTo(1L));
        // bytes 512 → lt_1k bucket (index 3)
        assertThat(acc.discoveryBytesScanned(3), equalTo(1L));
    }

    public void testRecordDiscoveryFailure() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordDiscoveryFailure();
        acc.recordDiscoveryFailure();
        assertThat(acc.discoveryFailures(), equalTo(2L));
    }

    public void testRecordParse() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordParse(1000L, 50L);
        assertThat(acc.parseRows(), equalTo(1000L));
        assertThat(acc.parseDuration(1), equalTo(1L)); // 50ms → lt_100ms bucket (index 1)
    }

    public void testRecordParseZeroRowsSkipsCounter() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordParse(0L, 10L);
        assertThat(acc.parseRows(), equalTo(0L));
    }

    public void testRecordSplitsScanned() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordSplitsScanned(50L);  // 50 → lt_100 bucket (index 2)
        assertThat(acc.parseSplitsScanned(2), equalTo(1L));
    }

    public void testRecordPoolRejectedAndBreakerTripped() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordPoolRejected();
        acc.recordBreakerTripped();
        acc.recordBreakerTripped();
        assertThat(acc.readerPoolRejected(), equalTo(1L));
        assertThat(acc.breakerTripped(), equalTo(2L));
    }

    public void testUnknownTypeRoutedToUnknownBucket() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest(Type.UNKNOWN, 10L, 100L);
        assertThat(acc.storageRequests(Type.UNKNOWN), equalTo(1L));
    }

    public void testNullTypeThrows() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        expectThrows(NullPointerException.class, () -> acc.recordRequest(null, 10L, 100L));
    }

    public void testUnexpectedOutcomeThrows() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        expectThrows(IllegalArgumentException.class, () -> acc.recordQuery("weird_outcome", 10L, false));
    }

    public void testOutcomeIndexOutOfRangeThrowsOnAccessor() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        expectThrows(IllegalArgumentException.class, () -> acc.queries(DataSourceUsageAccumulator.OUTCOME_COUNT));
        expectThrows(IllegalArgumentException.class, () -> acc.queries(-1));
    }

    public void testUnrecognizedOutcomeIsSwallowedByRecordQuery() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(MeterRegistry.NOOP, acc);
        // An unrecognized outcome is a programming error — recordQuery() passes it straight to the
        // accumulator, which throws IllegalArgumentException; the try-catch swallows it.
        // No accumulator counter should change.
        metrics.recordQuery("unexpected_outcome", 100L, false);
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS), equalTo(0L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_FAILURE), equalTo(0L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_CANCELLED), equalTo(0L));
    }

    public void testDataSourceCountersPopulatesAllKeyFamilies() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest(Type.S3, 5L, 1024L);
        acc.recordRequest(Type.LOCAL, 5L, 0L);
        acc.recordQuery("success", 100L, false);
        acc.recordDiscovery(30L, 5L, 512L);
        acc.recordParse(500L, 50L);
        acc.recordSplitsScanned(3L);
        acc.recordDiscoveryFailure();
        acc.recordBreakerTripped();

        Counters counters = new Counters();
        DataSourceCounters.populate(acc, counters);

        // spot-check a few keys
        assertThat(counters.get("datasources.storage.requests.total.s3"), equalTo(1L));
        assertThat(counters.get("datasources.storage.requests.total.local"), equalTo(1L));
        assertThat(counters.get("datasources.storage.bytes_read.total.s3"), equalTo(1024L));
        assertThat(counters.get("datasources.queries.by_outcome.success"), equalTo(1L));
        assertThat(counters.get("datasources.discovery.failures.total"), equalTo(1L));
        assertThat(counters.get("datasources.breaker.tripped.total"), equalTo(1L));
        assertThat(counters.get("datasources.parse.rows.total"), equalTo(500L));

        // verify one populated bucket per histogram family (exact bucket derived from input values above)
        assertThat(counters.get("datasources.storage.requests.duration.lt_10ms"), equalTo(2L)); // two 5ms requests
        assertThat(counters.get("datasources.discovery.files_scanned.lt_10"), equalTo(1L));     // 5 files → index 1
        assertThat(counters.get("datasources.discovery.bytes_scanned.lt_1k"), equalTo(1L));     // 512 bytes → index 3
        assertThat(counters.get("datasources.parse.splits_scanned.lt_10"), equalTo(1L));        // 3 splits → index 1
    }

    public void testExternalSourceMetricsDualSink() {
        // Verify that ExternalSourceMetrics with an attached accumulator forwards all events
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(MeterRegistry.NOOP, acc);

        metrics.recordRequest(50L, 2048L, "s3");
        metrics.recordRetry("gcs");
        metrics.recordError("azure");
        metrics.recordThrottled("http");
        metrics.recordReadStall(100L, "file");
        metrics.recordRequest(10L, 100L, "file");
        metrics.recordRequest(10L, 50L, "ftp");
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 200L, false);
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_CANCELLED, 10L, false);
        metrics.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 50L, true);
        metrics.recordTimeToFirstRow(30L, "s3");
        metrics.recordDiscovery(20L, 3L, 4096L, "s3");
        metrics.recordDiscoveryFailure();
        metrics.recordParse(100L, 40L, "gcs");
        metrics.recordSplitsScanned(2L, "s3");
        metrics.recordPoolRejected();
        metrics.recordBreakerTripped();

        assertThat(acc.storageRequests(Type.S3), equalTo(1L));
        assertThat(acc.storageBytesRead(Type.S3), equalTo(2048L));
        assertThat(acc.storageRetries(), equalTo(1L));
        assertThat(acc.storageErrors(Type.AZURE), equalTo(1L));
        assertThat(acc.storageThrottled(Type.HTTP), equalTo(1L));
        assertThat(acc.storageRequests(Type.LOCAL), equalTo(1L));
        assertThat(acc.storageRequests(Type.UNKNOWN), equalTo(1L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS), equalTo(2L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_CANCELLED), equalTo(1L));
        assertThat(acc.queriesCancelled(), equalTo(1L));
        assertThat(acc.queriesPartial(), equalTo(1L));
        assertThat(acc.discoveryFailures(), equalTo(1L));
        assertThat(acc.parseRows(), equalTo(100L));
        assertThat(acc.readerPoolRejected(), equalTo(1L));
        assertThat(acc.breakerTripped(), equalTo(1L));
    }

    public void testNoopHasNullAccumulator() {
        assertThat(ExternalSourceMetrics.NOOP.usageAccumulator(), equalTo(null));
    }

    public void testNoopRecordDoesNotAccumulateSharedState() {
        // NOOP is a shared singleton — its usageAccumulator must be null so record* calls are no-ops.
        assertNull(ExternalSourceMetrics.NOOP.usageAccumulator());
        // These calls must not throw and must not acquire a non-null accumulator.
        ExternalSourceMetrics.NOOP.recordRequest(10L, 100L, "s3");
        ExternalSourceMetrics.NOOP.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 50L, false);
        assertNull(ExternalSourceMetrics.NOOP.usageAccumulator());
    }
}
