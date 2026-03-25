/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * Test metrics exported by Elasticsearch directly using the OTel SDK
 */
public class OtelMetricsIT extends AbstractMetricsIT {

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .systemProperty("telemetry.otel.metrics.enabled", "true")
        .setting("telemetry.otel.metrics.endpoint", () -> "http://" + recordingApmServer.getHttpAddress() + "/v1/metrics")
        .setting("telemetry.otel.metrics.interval", "10m") // one giant batch instead of multiple small ones with deltas we need to sum
        .build();

    @ClassRule
    public static TestRule ruleChain = AbstractMetricsIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
