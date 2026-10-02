/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

public class OTelMetricsShutdownFlushIT extends AbstractTelemetryIT {

    public static RecordingApmServer recordingApmServer = new RecordingApmServer();

    public static ElasticsearchCluster cluster = AbstractMetricsIT.baseClusterBuilder()
        .setting("telemetry.export.endpoint", () -> recordingApmServer.getGrpcEndpoint())
        // Long enough that only the shutdown flush can export the recorded metrics.
        .setting("telemetry.export.interval", "1h")
        .setting("telemetry.metrics.buffer.disk_size", "0b")
        .build();

    @ClassRule
    public static TestRule ruleChain = AbstractTelemetryIT.buildRuleChain(recordingApmServer, cluster);

    @Override
    protected RecordingApmServer apmServer() {
        return recordingApmServer;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testMetricsFlushedOnShutdown() throws Exception {
        assumeFalse("graceful JVM shutdown is not deliverable on Windows by the test cluster framework", Constants.WINDOWS);
        client().performRequest(new Request("GET", "/_use_apm_metrics"));

        recordingApmServer.await(
            ReceivedTelemetry.ReceivedMetricSet.class,
            m -> m.samples().containsKey("es.test.long_counter.total"),
            TELEMETRY_TIMEOUT,
            () -> {
                cluster.stop(false);
                closeClients();
            }
        );
    }
}
