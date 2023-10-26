/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;

public class ApmIT extends ESRestTestCase {
    @ClassRule
    public static RecordingApmServer mockApmServer = new RecordingApmServer();

    @Rule
    public ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-apm-integration")
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .setting("tracing.apm.agent.metrics_interval", "1s")
        .setting("tracing.apm.agent.server_url", "http://127.0.0.1:" + mockApmServer.getPort())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApmIntegration() throws Exception {
        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        // TestMeterUsages for code that is registering & using metrics
        assertBusy(() -> {
            var usageRecords = mockApmServer.getMessages();
            assertThat(
                usageRecords,
                Matchers.hasItems(
                    Matchers.containsString("\"testDoubleCounter\":{\"value\":1.0}"),
                    Matchers.containsString("\"testLongCounter\":{\"value\":1.0}"),
                    Matchers.containsString("\"testDoubleHistogram\":{\"values\":["),// value?
                    Matchers.containsString("\"testLongHistogram\":{\"values\":["),// value?
                    Matchers.containsString("\"testDoubleGauge\":{\"value\":1.0}"),
                    Matchers.containsString("\"testLongGauge\":{\"value\":1}")
                )
            );

        });
    }
}
