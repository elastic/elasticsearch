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
 * Runs the shared {@link AbstractTracesIT} test suite against the APM agent export path.
 *
 * The APM Java agent intercepts {@code GlobalOpenTelemetry} and exports spans via the
 * APM intake protocol. Child span filtering is done by the agent ({@code transaction_max_spans=0})
 * and stack-trace suppression by ({@code stack_trace_limit=0}), both set as defaults in
 * {@code APMJvmOptions.CONFIG_DEFAULTS}.
 */
public class ApmAgentTracesIT extends AbstractTracesIT {

    public static ElasticsearchCluster cluster = baseTracesClusterBuilder().systemProperty("telemetry.otel.traces.enabled", "false")
        .setting("telemetry.agent.metrics_interval", "1s")
        .setting("telemetry.agent.server_url", () -> "http://" + recordingApmServer.getHttpAddress())
        .build();

    @ClassRule
    public static TestRule ruleChain = buildTracesRuleChain(recordingApmServer, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

}
