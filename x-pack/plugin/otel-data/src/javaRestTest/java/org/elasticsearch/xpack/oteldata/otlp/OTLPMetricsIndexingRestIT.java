/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.internal.FailedExportException;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OTLPMetricsIndexingRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";
    private static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));
    private static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");
    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void beforeTest() throws Exception {
        exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/metrics")
            .addHeader("Authorization", basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray())))
            .build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));
        boolean otlpEndpointEnabled = false;
        try {
            otlpEndpointEnabled = RestStatus.isSuccessful(
                client().performRequest(new Request("POST", "/_otlp/v1/metrics")).getStatusLine().getStatusCode()
            );
        } catch (Exception ignore) {}
        assumeTrue("Requires otlp_metrics feature flag to be enabled", otlpEndpointEnabled);
    }

    @Override
    public void tearDown() throws Exception {
        meterProvider.close();
        super.tearDown();
    }

    public void testIngestMetricDataViaMetricExporter() throws Exception {
        MetricData jvmMemoryMetricData = createDoubleGauge(
            TEST_RESOURCE,
            Attributes.empty(),
            "jvm.memory.total",
            Runtime.getRuntime().totalMemory(),
            "By",
            Clock.getDefault().now()
        );

        FailedExportException.HttpExportException exception = assertThrows(
            FailedExportException.HttpExportException.class,
            () -> export(List.of(jvmMemoryMetricData))
        );
        assertThat(exception.getResponse().statusCode(), equalTo(RestStatus.NOT_IMPLEMENTED.getStatus()));
    }

    private void export(List<MetricData> metrics) throws Exception {
        CompletableResultCode result = exporter.export(metrics).join(10, TimeUnit.SECONDS);
        Throwable failure = result.getFailureThrowable();
        if (failure instanceof Exception e) {
            throw e;
        } else if (failure != null) {
            throw new RuntimeException("Failed to export metrics", failure);
        }
        assertThat(result.isSuccess(), is(true));
        assertOK(client().performRequest(new Request("GET", "_refresh/metrics-*")));
    }

    private static MetricData createDoubleGauge(
        Resource resource,
        Attributes attributes,
        String name,
        double value,
        String unit,
        long timeEpochNanos
    ) {
        return ImmutableMetricData.createDoubleGauge(
            resource,
            TEST_SCOPE,
            name,
            "Your description could be here.",
            unit,
            ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(timeEpochNanos, timeEpochNanos, attributes, value)))
        );
    }
}
