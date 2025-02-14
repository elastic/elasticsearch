/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityWithApmTracingRestIT extends AbstractRemoteClusterSecurityTestCase {
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();
    final String traceIdValue = "0af7651916cd43dd8448eb211c80319c";
    final String traceParentValue = "00-" + traceIdValue + "-b7ad6b7169203331-01";

    private static final ConsumingTestServer mockApmServer = new ConsumingTestServer();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("telemetry.metrics.enabled", "false")
            .setting("telemetry.tracing.enabled", "true")
            .setting("telemetry.agent.metrics_interval", "1s")
            .setting("telemetry.agent.server_url", () -> "http://127.0.0.1:" + mockApmServer.getPort())
            // to ensure tracestate header is always set to cover RCS 2.0 handling of the tracestate header
            .setting("telemetry.agent.transaction_sample_rate", "1.0")
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
            .setting("telemetry.agent.transaction_sample_rate", "1.0")
            .setting("telemetry.agent.metrics_interval", "1s")
            .setting("telemetry.agent.server_url", () -> "http://127.0.0.1:" + mockApmServer.getPort())
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

    @SuppressWarnings("unchecked")
    public void testTracingCrossCluster() throws Exception {
        configureRemoteCluster();
        Set<Predicate<Map<String, Object>>> assertions = new HashSet<>(
            Set.of(
                // REST action on query cluster
                allTrue(
                    transactionValue("name", equalTo("GET /_resolve/cluster/{name}")),
                    transactionValue("trace_id", equalTo(traceIdValue))
                ),
                // transport action on fulfilling cluster
                allTrue(
                    transactionValue("name", equalTo("indices:admin/resolve/cluster")),
                    transactionValue("trace_id", equalTo(traceIdValue))
                )
            )
        );

        CountDownLatch finished = new CountDownLatch(1);

        // a consumer that will remove the assertions from a map once it matched
        Consumer<String> messageConsumer = (String message) -> {
            var apmMessage = parseMap(message);
            if (isTransactionTraceMessage(apmMessage)) {
                logger.info("Apm transaction message received: {}", message);
                assertions.removeIf(e -> e.test(apmMessage));
            }

            if (assertions.isEmpty()) {
                finished.countDown();
            }
        };

        mockApmServer.addMessageConsumer(messageConsumer);

        // Trigger an action that we know will cross clusters -- doesn't much matter which one
        final Request resolveRequest = new Request("GET", "/_resolve/cluster/my_remote_cluster:*");
        resolveRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
                .addHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParentValue)
        );
        final Response response = client().performRequest(resolveRequest);
        assertOK(response);

        finished.await(30, TimeUnit.SECONDS);
        assertThat(assertions, equalTo(Collections.emptySet()));
    }

    private boolean isTransactionTraceMessage(Map<String, Object> apmMessage) {
        return apmMessage.containsKey("transaction");
    }

    @SuppressWarnings("unchecked")
    private Predicate<Map<String, Object>> allTrue(Predicate<Map<String, Object>>... predicates) {
        var allTrueTest = Arrays.stream(predicates).reduce(v -> true, Predicate::and);
        return new Predicate<>() {
            @Override
            public boolean test(Map<String, Object> map) {
                return allTrueTest.test(map);
            }

            @Override
            public String toString() {
                return Arrays.stream(predicates).map(Object::toString).collect(Collectors.joining(" and "));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Predicate<Map<String, Object>> transactionValue(String path, Matcher<T> expected) {
        return new Predicate<>() {
            @Override
            public boolean test(Map<String, Object> map) {
                var transaction = (Map<String, Object>) map.get("transaction");
                var value = XContentMapValues.extractValue(path, transaction);
                return expected.matches((T) value);
            }

            @Override
            public String toString() {
                StringDescription matcherDescription = new StringDescription();
                expected.describeTo(matcherDescription);
                return path + " " + matcherDescription;
            }
        };
    }

    private Map<String, Object> parseMap(String message) {
        try (XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, message)) {
            return parser.map();
        } catch (IOException e) {
            fail(e);
            return Collections.emptyMap();
        }
    }
}
