/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityWithSearchAndReplicationDlsRestIT extends AbstractRemoteClusterSecurityDlsAndFlsRestIT {

    private static final String REMOTE_CLUSTER_DLS = REMOTE_CLUSTER_ALIAS + "_dls";

    private static final AtomicReference<Map<String, Object>> API_KEY_REFERENCE = new AtomicReference<>();

    private static final String API_KEY_ACCESS = """
        {
            "search": [
              {
                  "names": ["test_index1"],
                  "query": {"term" : {"field1" : "value1"}}
              }
            ],
            "replication": [
              {
                  "names": ["test_index1"]
              }
            ]
        }""";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore(
                "cluster.remote." + REMOTE_CLUSTER_DLS + ".credentials",
                () -> createCrossClusterAccessApiKey(API_KEY_ACCESS, API_KEY_REFERENCE)
            )
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void test() throws Exception {
        setupRemoteClusterTestCase(REMOTE_CLUSTER_DLS);
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "test_index1" } }
            { "field1": "value1" }
            { "index": { "_index": "test_index1" } }
            { "field1": "value2" }\n""");
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        final Request searchRequest = new Request("GET", Strings.format("/%s:test_index1/_search", REMOTE_CLUSTER_DLS, randomBoolean()));
        final Response response = performRequestWithUser(searchRequest, REMOTE_SEARCH_USER_ALL);
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            assertThat(actualIndices, containsInAnyOrder("test_index1"));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        } finally {
            searchResponse.decRef();
        }
    }
}
