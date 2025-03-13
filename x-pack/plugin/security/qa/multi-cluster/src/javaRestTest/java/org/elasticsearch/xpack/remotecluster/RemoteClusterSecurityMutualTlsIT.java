/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityMutualTlsIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<String> VERIFICATION_MODE = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.remote_cluster_server.ssl.client_authentication", "required")
            .setting("xpack.security.remote_cluster_server.ssl.verification_mode", () -> String.valueOf(VERIFICATION_MODE.get()))
            .setting("xpack.security.remote_cluster_server.ssl.certificate_authorities", "remote-cluster-client-ca.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.key", "remote-cluster-client.key")
            .setting("xpack.security.remote_cluster_client.ssl.certificate", "remote-cluster-client.crt")
            .setting("xpack.security.remote_cluster_client.ssl.client_authentication", "required") // no actual effect
            .setting("xpack.security.remote_cluster_client.ssl.verification_mode", () -> String.valueOf(VERIFICATION_MODE.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("xpack.security.remote_cluster_client.ssl.secure_key_passphrase", "remote-cluster-client-password")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["shared-metrics"]
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
    // `SSL_ENABLED_REF` is used to control the SSL-enabled setting on the test clusters
    // We set it here, since randomization methods are not available in the static initialize context above
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> {
        VERIFICATION_MODE.set(randomFrom("full", "certificate", "none"));
    })).around(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearch() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster
        {
            // Check remote metric users can search shared-metrics
            final var metricSearchRequest = new Request(
                "GET",
                String.format(Locale.ROOT, "/my_remote_cluster:*/_search?ccs_minimize_roundtrips=%s", randomBoolean())
            );
            final SearchResponse metricSearchResponse = SearchResponseUtils.parseSearchResponse(
                responseAsParser(performRequestWithRemoteMetricUser(metricSearchRequest))
            );
            try {
                assertThat(metricSearchResponse.getHits().getTotalHits().value(), equalTo(4L));
                assertThat(
                    Arrays.stream(metricSearchResponse.getHits().getHits()).map(SearchHit::getIndex).collect(Collectors.toSet()),
                    containsInAnyOrder("shared-metrics")
                );
            } finally {
                metricSearchResponse.decRef();
            }
        }
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
        );
        return client().performRequest(request);
    }
}
