/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class MetricsOTelSDKIT extends ESRestTestCase {

    private static final List<String> EXPECTED_METRICS = List.of(
        "system.memory.total",
        "system.memory.actual.free",
        "system.process.memory.size",
        "jvm.memory.used",
        "jvm.memory.committed",
        "jvm.gc.alloc",
        "jvm.gc.count",
        "jvm.gc.time",
        "jvm.cpu.count",
        "jvm.cpu.time",
        "jvm.thread.count",
        "jvm.class.loaded",
        "jvm.class.unloaded",
        "jvm.class.count",
        "system.cpu.total.norm.pct",
        "jvm.file_descriptor.count",
        "jvm.file_descriptor.limit"
    );

    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", "http://127.0.0.1:" + mockApmServer.getPort() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "1s")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testOTelMetricsInAPM() throws Exception {
        Set<String> collectedMetrics = ConcurrentHashMap.newKeySet();
        Consumer<String> messageConsumer = collectedMetrics::add;
        mockApmServer.addMessageConsumer(messageConsumer);
        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        assertBusy(
            () -> { assertThat("Missing expected metrics", EXPECTED_METRICS, everyItem(is(in(collectedMetrics)))); },
            30,
            TimeUnit.SECONDS
        );
    }
}
