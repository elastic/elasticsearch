/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Reproduces Prometheus labels/metadata behaviour against real OTel-ingested TSDS data streams.
 *
 * <p>Uses a dedicated HTTP cluster (no HTTP SSL) so the OTel SDK can ingest via OTLP/HTTP and
 * Prometheus read endpoints are exercised with {@code GET} only — form-encoded {@code POST} is
 * rejected when {@code xpack.security.http.ssl.enabled} is {@code false}.
 */
public class PrometheusOtelLabelsMetadataRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private static final String HOSTMETRICS_SCOPE =
        "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper";
    private static final String GENERIC_SCOPE = "io.opentelemetry.example.metrics";

    private static final String HOSTMETRICS_STREAM = "metrics-hostmetricsreceiver.otel-default";
    private static final String GENERIC_STREAM = "metrics-generic.otel-default";

    private static final String HOSTMETRICS_METRIC = "system.cpu.utilization";
    private static final String GENERIC_METRIC = "custom.otel.labels.test";

    private static final String HOSTMETRICS_LABEL = "attributes.stream_a_label";
    private static final String GENERIC_LABEL = "attributes.stream_b_label";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user(USER, PASS, "superuser", false)
        .build();

    private String otlpWriteApiKey;
    private String prometheusReadApiKey;
    private OtlpHttpMetricExporter exporter;
    private SdkMeterProvider meterProvider;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));
        otlpWriteApiKey = createApiKey("otel-write-key", "metrics-*", "create_doc", "auto_configure");
        prometheusReadApiKey = createApiKey("prometheus-read-key", "metrics-*", "read");
        initMeterProvider();
        ingestOtelGauge(HOSTMETRICS_SCOPE, HOSTMETRICS_STREAM, HOSTMETRICS_METRIC, "stream_a_label", "value_a");
        ingestOtelGauge(GENERIC_SCOPE, GENERIC_STREAM, GENERIC_METRIC, "stream_b_label", "value_b");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (meterProvider != null) {
            meterProvider.close();
            meterProvider = null;
        }
        super.tearDown();
    }

    public void testLabelsWithCommaSeparatedOtelStreamsReturnsCombinedLabels() throws Exception {
        String index = HOSTMETRICS_STREAM + "," + GENERIC_STREAM;
        List<String> data = queryLabels(index);

        assertThat(data, hasItem("__name__"));
        assertThat(data, hasItem(HOSTMETRICS_LABEL));
        assertThat(data, hasItem(GENERIC_LABEL));
    }

    public void testMetadataWithCommaSeparatedOtelStreamsReturnsCombinedMetrics() throws Exception {
        String index = HOSTMETRICS_STREAM + "," + GENERIC_STREAM;
        ObjectPath op = queryMetadata(index);

        assertThat(op.evaluate("status"), equalTo("success"));
        assertThat(op.evaluate("data." + escapeDots(HOSTMETRICS_METRIC)), notNullValue());
        assertThat(op.evaluate("data." + escapeDots(GENERIC_METRIC)), notNullValue());
    }

    public void testLabelsWithDefaultMetricsWildcardReturnsOtelLabels() throws Exception {
        List<String> data = queryLabels(null);

        assertThat(data, hasItem("__name__"));
        assertThat(data, hasItem(HOSTMETRICS_LABEL));
        assertThat(data, hasItem(GENERIC_LABEL));
    }

    public void testMetadataWithDefaultMetricsWildcardReturnsOtelMetrics() throws Exception {
        ObjectPath op = queryMetadata(null);

        assertThat(op.evaluate("status"), equalTo("success"));
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) op.evaluate("data");
        assertThat(data.size(), greaterThan(0));
        assertThat(data.keySet(), hasItem(HOSTMETRICS_METRIC));
        assertThat(data.keySet(), hasItem(GENERIC_METRIC));
    }

    private void initMeterProvider() {
        exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(getClusterHosts().getFirst().toURI() + "/_otlp/v1/metrics")
            .addHeader("Authorization", "ApiKey " + otlpWriteApiKey)
            .build();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter)
                    .setExecutor(Executors.newScheduledThreadPool(0))
                    .setInterval(Duration.ofNanos(Long.MAX_VALUE))
                    .build()
            )
            .build();
    }

    private void ingestOtelGauge(String scopeName, String expectedStream, String metricName, String labelKey, String labelValue)
        throws Exception {
        Meter meter = meterProvider.get(scopeName);
        meter.gaugeBuilder(metricName)
            .setUnit("1")
            .buildWithCallback(result -> result.record(42.0, Attributes.of(stringKey(labelKey), labelValue)));
        assertThat(meterProvider.shutdown().isSuccess(), is(true));
        refreshMetricsIndices();

        ObjectPath search = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/" + expectedStream + "/_search")));
        assertThat(((Number) search.evaluate("hits.total.value")).intValue(), equalTo(1));

        initMeterProvider();
    }

    private static void refreshMetricsIndices() throws IOException {
        assertOK(client().performRequest(new Request("GET", "metrics-*/_refresh")));
    }

    @SuppressWarnings("unchecked")
    private List<String> queryLabels(String index) throws IOException {
        String path = index == null ? "/_prometheus/api/v1/labels" : "/_prometheus/" + index + "/api/v1/labels";
        Response response = client().performRequest(prometheusGetRequest(path));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return (List<String>) entityAsMap(response).get("data");
    }

    private ObjectPath queryMetadata(String index) throws IOException {
        String path = index == null ? "/_prometheus/api/v1/metadata" : "/_prometheus/" + index + "/api/v1/metadata";
        return ObjectPath.createFromResponse(client().performRequest(prometheusGetRequest(path)));
    }

    private Request prometheusGetRequest(String path) {
        Request request = new Request("GET", path);
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + prometheusReadApiKey).build());
        return request;
    }

    private static String escapeDots(String metricName) {
        return metricName.replace(".", "\\.");
    }

    private static String createApiKey(String name, String indexPattern, String... privileges) throws IOException {
        StringBuilder privilegeArray = new StringBuilder();
        for (int i = 0; i < privileges.length; i++) {
            if (i > 0) {
                privilegeArray.append("\", \"");
            }
            privilegeArray.append(privileges[i]);
        }
        Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity("""
            {
              "name": "$NAME",
              "role_descriptors": {
                "role": {
                  "index": [
                    {
                      "names": ["$INDEX_PATTERN"],
                      "privileges": ["$PRIVILEGES"]
                    }
                  ]
                }
              }
            }
            """.replace("$NAME", name).replace("$INDEX_PATTERN", indexPattern).replace("$PRIVILEGES", privilegeArray));
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        return response.evaluate("encoded");
    }
}
