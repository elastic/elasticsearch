/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.apmintegration.GrpcThreadsFilter;
import org.elasticsearch.test.apmintegration.ReceivedTelemetry;
import org.elasticsearch.test.apmintegration.RecordingApmServer;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@ThreadLeakFilters(filters = { GrpcThreadsFilter.class })
public class RemoteClusterSecurityWithApmTracingRestIT extends AbstractRemoteClusterSecurityTestCase {
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    final String traceIdValue = "0af7651916cd43dd8448eb211c80319c";
    final String traceParentValue = "00-" + traceIdValue + "-b7ad6b7169203331-01";

    private static final RecordingApmServer mockApmServer = new RecordingApmServer();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("telemetry.metrics.enabled", "false")
            .setting("telemetry.tracing.enabled", "true")
            .setting("telemetry.export.endpoint", () -> mockApmServer.getGrpcEndpoint())
            // to ensure tracestate header is always set to cover RCS 2.0 handling of the tracestate header
            .setting("telemetry.tracing.sample_rate", "1.0")
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("telemetry.metrics.enabled", "false")
            .setting("telemetry.tracing.enabled", "true")
            // to ensure tracestate header is always set to cover RCS 2.0 handling of the tracestate header
            .setting("telemetry.tracing.sample_rate", "1.0")
            .setting("telemetry.export.endpoint", () -> mockApmServer.getGrpcEndpoint())
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                            "search": [
                              {
                                "names": ["*"]
                              }
                            ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(mockApmServer).around(fulfillingCluster).around(queryCluster);

    /**
     * Verifies that an externally supplied {@code traceparent} header is honoured by the REST controller and propagated through a
     * cross-cluster request. Since #130607, transport actions are not auto-traced by {@link org.elasticsearch.tasks.TaskManager} unless
     * a parent APM context already exists locally, so the fulfilling cluster does not produce its own transaction for the cross-cluster
     * transport entry point. We therefore assert only on the query cluster's REST span, which captures the propagated trace id.
     */
    public void testTracingCrossCluster() throws Exception {
        assumeTrue("requires test-apm-integration which is only loaded in snapshot builds", Build.current().isSnapshot());
        configureRemoteCluster();

        mockApmServer.await(
            ReceivedTelemetry.ReceivedSpan.class,
            s -> traceIdValue.equals(s.traceId()) && "GET /_resolve/cluster/{name}".equals(s.name()),
            30,
            () -> {
                // Trigger an action that we know will cross clusters -- doesn't much matter which one
                final Request resolveRequest = new Request("GET", "/_resolve/cluster/my_remote_cluster:*");
                resolveRequest.setOptions(
                    RequestOptions.DEFAULT.toBuilder()
                        .addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
                        .addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue)
                );
                final Response response = client().performRequest(resolveRequest);
                assertOK(response);

                // Force the query cluster to flush so the test does not depend on the exporter's batch interval.
                assertOK(client().performRequest(new Request("GET", "/_flush_telemetry")));
            }
        );
    }
}
