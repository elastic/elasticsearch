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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TracesApmIT extends ESRestTestCase {
    final String traceIdValue = "0af7651916cd43dd8448eb211c80319c";
    final String traceParentValue = "00-" + traceIdValue + "-b7ad6b7169203331-01";

    public RecordingApmServer mockApmServer = new RecordingApmServer();

    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "false")
        .setting("telemetry.tracing.enabled", "true")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", () -> "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Rule
    public TestRule ruleChain = RuleChain.outerRule(mockApmServer).around(cluster).around((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            try {
                base.evaluate();
            } finally {
                try {
                    closeClients();
                } catch (IOException e) {
                    throw new AssertionError("failed to close REST clients after test", e);
                }
            }
        }
    });

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApmIntegration() throws Exception {
        CountDownLatch finished = new CountDownLatch(1);

        Consumer<ReceivedTelemetry> messageConsumer = (ReceivedTelemetry msg) -> {
            if (msg instanceof ReceivedTelemetry.ReceivedSpan s
                && s.parentSpanId().isEmpty()
                && "GET /_nodes/stats".equals(s.name())
                && traceIdValue.equals(s.traceId())) {
                logger.info("Apm root span (transaction) received: {}", s);
                finished.countDown();
            }
        };

        mockApmServer.addMessageConsumer(messageConsumer);

        Request nodeStatsRequest = new Request("GET", "/_nodes/stats");
        nodeStatsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue).build());

        client().performRequest(nodeStatsRequest);

        var completed = finished.await(30, TimeUnit.SECONDS);
        assertTrue("Timeout when waiting for assertions to complete", completed);
    }
}
