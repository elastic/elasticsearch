/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

/**
 * {@link ExternalSourceResolver#mapResolveFailure} is the one place every failed discovery attempt goes through. These
 * tests pin that each failure is recorded once, with the category and status of the exception the client receives and
 * the storage type of the failing path, on both the APM and the phone-home sink.
 */
public class ExternalSourceResolverDiscoveryFailureTelemetryTests extends ESTestCase {

    private static final String PATH_STRING = "s3://bucket/dir/file.parquet";
    private static final StoragePath PATH = StoragePath.of(PATH_STRING);

    private RecordingMeterRegistry registry;
    private DataSourceUsageAccumulator accumulator;
    private ExternalSourceResolver resolver;

    @Before
    public void createResolver() {
        registry = new RecordingMeterRegistry();
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        DataSourceModule module = new DataSourceModule(
            List.of(),
            DataSourceCapabilities.build(List.of()),
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(mock(EncryptionService.class)),
            () -> false,
            null,
            null,
            null,
            registry,
            LocalFileAccess.UNRESTRICTED,
            null,
            null
        );
        accumulator = module.externalSourceMetrics().usageAccumulator();
        resolver = new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
    }

    public void testRetryableUnavailableBehindFactoryWrapper() {
        Exception failure = new IllegalArgumentException(
            "factory failed",
            new ExternalUnavailableException(Condition.STORE_UNAVAILABLE, PATH, "HTTP 503", "", false, 0L)
        );

        resolver.mapResolveFailure(PATH_STRING, failure);

        assertRecorded("s3", "storage_unavailable", "503");
    }

    public void testThrottledIsDistinctFromUnavailable() {
        resolver.mapResolveFailure(
            PATH_STRING,
            new ExternalUnavailableException(Condition.STORE_THROTTLED, PATH, "HTTP 429", "", true, 100L)
        );

        assertRecorded("s3", "storage_throttled", "503");
    }

    public void testObjectNotFound() {
        resolver.mapResolveFailure("gs://bucket/missing.csv", new ExternalClientException(Condition.OBJECT_NOT_FOUND, PATH, "", ""));

        assertRecorded("gcs", "storage_not_found", "400");
    }

    public void testExpiredCredentials() {
        resolver.mapResolveFailure(
            "wasbs://container@account/data.csv",
            new ExecutionException(new ExternalCredentialsExpiredException(PATH, "ExpiredToken", ""))
        );

        assertRecorded("azure", "storage_auth", "400");
    }

    /** The plain I/O failure of the file-metadata rail is reported to the client as a metadata failure, and counted as one. */
    public void testPlainIoFailureIsADiscoveryFailure() {
        resolver.mapResolveFailure(PATH_STRING, new ExecutionException(new IOException("could not read the footer")));

        assertRecorded("s3", "discovery", "400");
    }

    public void testUntypedClientErrorIsOtherWithItsStatus() {
        resolver.mapResolveFailure(PATH_STRING, new IllegalArgumentException("Glob pattern discovered too many files"));

        assertRecorded("s3", "other", "400");
    }

    public void testUnexpectedFailureIsOtherServerError() {
        resolver.mapResolveFailure(PATH_STRING, new IllegalStateException("bug"));

        assertRecorded("s3", "other", "500");
    }

    public void testNodeLevelRejectionAndBreaker() {
        resolver.mapResolveFailure(PATH_STRING, new IllegalArgumentException("wrapped", new EsRejectedExecutionException("saturated")));
        assertRecorded("s3", "resource_limit", "429");

        resolver.mapResolveFailure(
            PATH_STRING,
            new IllegalArgumentException("wrapped", new CircuitBreakingException("too big", CircuitBreaker.Durability.TRANSIENT))
        );
        List<Measurement> failures = failures();
        assertThat(failures, hasSize(2));
        assertThat(failures.get(1).attributes().get(ExternalSourceMetrics.ERROR_TYPE_ATTRIBUTE), equalTo("circuit_breaker"));
        assertThat(failures.get(1).attributes().get(ExternalSourceMetrics.STATUS_ATTRIBUTE), equalTo("429"));
        assertThat(accumulator.discoveryFailures(), equalTo(2L));
    }

    public void testCancellationIsNotADiscoveryFailure() {
        resolver.mapResolveFailure(PATH_STRING, new TaskCancelledException("cancelled"));

        assertThat(failures(), hasSize(0));
        assertThat(accumulator.discoveryFailures(), equalTo(0L));
    }

    public void testPathWithoutSchemeIsUnknownType() {
        resolver.mapResolveFailure("not-a-uri", new IllegalArgumentException("bad path"));

        assertRecorded("unknown", "other", "400");
    }

    /** The phone-home per-type counters always add up to the total, so a dashboard can rely on it. */
    public void testPhoneHomeTotalIsTheSumOfTheCategories() {
        resolver.mapResolveFailure(PATH_STRING, new IllegalStateException("bug"));
        resolver.mapResolveFailure(PATH_STRING, new ExternalClientException(Condition.ACCESS_DENIED, PATH, "", ""));
        resolver.mapResolveFailure(PATH_STRING, new ExternalClientException(Condition.ACCESS_DENIED, PATH, "", ""));

        long sum = 0;
        for (int i = 0; i < DataSourceUsageAccumulator.ERROR_TYPE_COUNT; i++) {
            sum += accumulator.discoveryFailures(i);
        }
        assertThat(sum, equalTo(3L));
        assertThat(accumulator.discoveryFailures(), equalTo(3L));
        assertThat(
            accumulator.discoveryFailures(
                DataSourceUsageAccumulator.ERROR_TYPE_NAMES.indexOf(DataSourceUsageAccumulator.ERROR_TYPE_STORAGE_AUTH)
            ),
            equalTo(2L)
        );
    }

    private List<Measurement> failures() {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.DISCOVERY_FAILURES_TOTAL);
    }

    private void assertRecorded(String type, String errorType, String status) {
        List<Measurement> failures = failures();
        assertThat(failures, hasSize(1));
        Measurement failure = failures.get(0);
        assertThat(failure.getLong(), equalTo(1L));
        assertThat(failure.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE), equalTo(type));
        assertThat(failure.attributes().get(ExternalSourceMetrics.ERROR_TYPE_ATTRIBUTE), equalTo(errorType));
        assertThat(failure.attributes().get(ExternalSourceMetrics.STATUS_ATTRIBUTE), equalTo(status));
        assertThat(accumulator.discoveryFailures(), equalTo(1L));
        assertThat(accumulator.discoveryFailures(DataSourceUsageAccumulator.ERROR_TYPE_NAMES.indexOf(errorType)), equalTo(1L));
    }
}
