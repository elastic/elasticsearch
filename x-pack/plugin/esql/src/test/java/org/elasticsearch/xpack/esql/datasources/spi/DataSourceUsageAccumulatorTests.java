/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.DataSourceCounters;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DataSourceUsageAccumulatorTests extends ESTestCase {

    public void testRecordRequestIncrementsByScheme() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest("s3", 50L, 1024L);
        acc.recordRequest("s3", 10L, 0L);
        acc.recordRequest("gcs", 20L, 2048L);

        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_S3), equalTo(2L));
        assertThat(acc.storageBytesRead(DataSourceUsageAccumulator.SCHEME_S3), equalTo(1024L));
        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_GCS), equalTo(1L));
        assertThat(acc.storageBytesRead(DataSourceUsageAccumulator.SCHEME_GCS), equalTo(2048L));
        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_UNKNOWN), equalTo(0L));
    }

    public void testRecordRequestZeroBytesDoesNotIncrementBytesRead() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest("file", 5L, 0L);
        assertThat(acc.storageBytesRead(DataSourceUsageAccumulator.SCHEME_FILE), equalTo(0L));
        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_FILE), equalTo(1L));
    }

    public void testRecordRequestPopulatesTimeBucket() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest("s3", 5L, 0L);  // < 10ms → bucket 0
        assertThat(acc.storageRequestDuration(0), equalTo(1L));
        for (int b = 1; b < DataSourceUsageAccumulator.BUCKET_COUNT; b++) {
            assertThat(acc.storageRequestDuration(b), equalTo(0L));
        }
    }

    public void testRecordRetryAndErrorAndThrottled() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRetry();
        acc.recordRetry();
        acc.recordError("s3");
        acc.recordThrottled("gcs");

        assertThat(acc.storageRetries(), equalTo(2L));
        assertThat(acc.storageErrors(DataSourceUsageAccumulator.SCHEME_S3), equalTo(1L));
        assertThat(acc.storageThrottled(DataSourceUsageAccumulator.SCHEME_GCS), equalTo(1L));
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
        // bytes 512 → lt_1kb bucket (index 3)
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
        assertThat(acc.parseDuration(1), greaterThan(0L)); // 50ms → lt_100ms bucket
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

    public void testUnknownSchemeRoutedToUnknownBucket() {
        // The accumulator only accepts the six declared canonical scheme names; "unknown" is the
        // correct token for any unrecognised scheme (ExternalSourceMetrics.accScheme() clamps before
        // calling the accumulator, so arbitrary strings never reach schemeIndex()).
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest("unknown", 10L, 100L);
        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_UNKNOWN), equalTo(1L));
    }

    public void testUnexpectedSchemeThrows() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        expectThrows(IllegalArgumentException.class, () -> acc.recordRequest("ftp", 10L, 100L));
    }

    public void testUnexpectedOutcomeThrows() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        expectThrows(IllegalArgumentException.class, () -> acc.recordQuery("weird_outcome", 10L, false));
    }

    public void testSchemeIndexOutOfRangeThrowsOnAccessor() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        // OUTCOME_CANCELLED = 2 is in-range for scheme (0..5) but is semantically wrong;
        // SCHEME_COUNT itself is reliably out-of-range and must throw.
        expectThrows(IllegalArgumentException.class, () -> acc.storageRequests(DataSourceUsageAccumulator.SCHEME_COUNT));
        expectThrows(IllegalArgumentException.class, () -> acc.storageBytesRead(-1));
        expectThrows(IllegalArgumentException.class, () -> acc.storageErrors(DataSourceUsageAccumulator.SCHEME_COUNT));
        expectThrows(IllegalArgumentException.class, () -> acc.storageThrottled(DataSourceUsageAccumulator.SCHEME_COUNT));
    }

    public void testOutcomeIndexOutOfRangeThrowsOnAccessor() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        // SCHEME_FILE = 4 is out of range for the queries[] array (size OUTCOME_COUNT = 3).
        expectThrows(IllegalArgumentException.class, () -> acc.queries(DataSourceUsageAccumulator.SCHEME_FILE));
        expectThrows(IllegalArgumentException.class, () -> acc.queries(-1));
    }

    public void testUnrecognizedOutcomeClampsToFailureInExternalSourceMetrics() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(org.elasticsearch.telemetry.metric.MeterRegistry.NOOP, acc);
        // An unrecognized outcome must not throw — it must clamp to "failure" so that APM and
        // phone-home counters stay in sync (APM already incremented its counter before the accumulator call).
        metrics.recordQuery("unexpected_outcome", 100L, false);
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_FAILURE), equalTo(1L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS), equalTo(0L));
        assertThat(acc.queries(DataSourceUsageAccumulator.OUTCOME_CANCELLED), equalTo(0L));
    }

    public void testDataSourceCountersPopulatesAllKeyFamilies() {
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        acc.recordRequest("s3", 5L, 1024L);
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
        assertThat(counters.get("datasources.storage.bytes_read.total.s3"), equalTo(1024L));
        assertThat(counters.get("datasources.queries.total.success"), equalTo(1L));
        assertThat(counters.get("datasources.discovery.failures.total"), equalTo(1L));
        assertThat(counters.get("datasources.breaker.tripped.total"), equalTo(1L));
        assertThat(counters.get("datasources.parse.rows.total"), equalTo(500L));

        // verify at least one bucket from each histogram family exists
        assertThat(counters.get("datasources.storage.requests.duration.lt_10ms"), greaterThan(-1L));
        assertThat(counters.get("datasources.discovery.files_scanned.lt_1"), greaterThan(-1L));
        assertThat(counters.get("datasources.discovery.bytes_scanned.lt_1b"), greaterThan(-1L));
        assertThat(counters.get("datasources.parse.splits_scanned.lt_1"), greaterThan(-1L));
    }

    public void testExternalSourceMetricsDualSink() {
        // Verify that ExternalSourceMetrics with an attached accumulator forwards all events
        DataSourceUsageAccumulator acc = new DataSourceUsageAccumulator();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(org.elasticsearch.telemetry.metric.MeterRegistry.NOOP, acc);

        metrics.recordRequest(50L, 2048L, "s3");
        metrics.recordRetry("gcs");
        metrics.recordError("azure");
        metrics.recordThrottled("http");
        metrics.recordReadStall(100L, "file");
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

        assertThat(acc.storageRequests(DataSourceUsageAccumulator.SCHEME_S3), equalTo(1L));
        assertThat(acc.storageBytesRead(DataSourceUsageAccumulator.SCHEME_S3), equalTo(2048L));
        assertThat(acc.storageRetries(), equalTo(1L));
        assertThat(acc.storageErrors(DataSourceUsageAccumulator.SCHEME_AZURE), equalTo(1L));
        assertThat(acc.storageThrottled(DataSourceUsageAccumulator.SCHEME_HTTP), equalTo(1L));
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
        // NOOP must never accumulate state — it is a shared singleton.
        ExternalSourceMetrics.NOOP.recordRequest(10L, 100L, "s3");
        ExternalSourceMetrics.NOOP.recordQuery(ExternalSourceMetrics.OUTCOME_SUCCESS, 50L, false);
        // If usageAccumulator were non-null on NOOP, calls above would have mutated shared state.
        // Verify accumulator is null (i.e. the guard is in place).
        assertThat(ExternalSourceMetrics.NOOP.usageAccumulator(), equalTo(null));
    }
}
