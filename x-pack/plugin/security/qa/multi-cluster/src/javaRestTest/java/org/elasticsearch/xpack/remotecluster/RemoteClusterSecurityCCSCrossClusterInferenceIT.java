/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;

public class RemoteClusterSecurityCCSCrossClusterInferenceIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            // Enable RCS 2.0 (remote cluster server)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .module("x-pack-ilm")
            .module("x-pack-ml")
            .module("x-pack-inference")
            .plugin("inference-service-test")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .module("x-pack-ilm")
            .module("x-pack-ml")
            .module("x-pack-inference")
            .plugin("inference-service-test")
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
            .user(REMOTE_SEARCH_USER, PASS.toString(), "remote_search_cross_cluster_inference", false)
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    private final Set<String> indices = new HashSet<>();
    private final Set<String> inferenceIds = new HashSet<>();

    @After
    public void cleanUpIndicesAndInferenceEndpoints() throws Exception {
        for (String index : indices) {
            deleteIndexOnFulfillingCluster(index);
        }

        for (String inferenceId : inferenceIds) {
            deleteInferenceEndpointOnFulfillingCluster(inferenceId, true);
        }
    }

    public void testCrossClusterInference() throws Exception {
        configureRemoteCluster();

        // Create an inference endpoint on the remote cluster
        final String inferenceId = createTextEmbeddingInferenceEndpointOnFulfillingCluster();

        // Create an index on the remote cluster with a semantic_text field that uses the inference endpoint
        createSemanticTextIndexOnFulfillingCluster("test-index", inferenceId);
        indexDocOnFulfillingCluster("test-index", "1", "test document for cross cluster search");

        // Execute a basic match query with ccs_minimize_roundtrips=false.
        // This will be intercepted by the inference plugin and rewritten to a semantic query on the content field, which will trigger
        // a cross-cluster inference action.
        Response response = performSearchRequest(List.of("my_remote_cluster:test-index"), "test");
        ObjectPath objectPath = assertOKAndCreateObjectPath(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(1));
        assertThat(objectPath.evaluate("hits.hits.0._id"), equalTo("1"));
    }

    public void testQueryUnauthorizedIndex() throws Exception {
        configureRemoteCluster();

        // Create an inference endpoint on the remote cluster
        final String inferenceId1 = createTextEmbeddingInferenceEndpointOnFulfillingCluster();
        final String inferenceId2 = createTextEmbeddingInferenceEndpointOnFulfillingCluster();

        // Create two indices on the remote cluster with a semantic_text field that uses the inference endpoint.
        // The user is only authorized to query one of these indices.
        createSemanticTextIndexOnFulfillingCluster("test-index", inferenceId1);
        createSemanticTextIndexOnFulfillingCluster("unauthorized-index", inferenceId2);

        indexDocOnFulfillingCluster("test-index", "1", "test document for cross cluster search");
        indexDocOnFulfillingCluster("unauthorized-index", "2", "another test document for cross cluster");

        // Execute a query and verify that results are returned only for the authorized index
        Response response = performSearchRequest(List.of("my_remote_cluster:*-index"), "test");
        ObjectPath objectPath = assertOKAndCreateObjectPath(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(1));
        assertThat(objectPath.evaluate("hits.hits.0._id"), equalTo("1"));
    }

    private String createTextEmbeddingInferenceEndpointOnFulfillingCluster() throws Exception {
        final String inferenceId = randomIdentifier();
        Request createInferenceEndpointRequest = new Request("PUT", "/_inference/text_embedding/" + inferenceId);
        createInferenceEndpointRequest.setJsonEntity("""
            {
              "service": "text_embedding_test_service",
              "service_settings": {
                "model": "my_model",
                "dimensions": 256,
                "similarity": "cosine",
                "api_key": "abc64"
              }
            }
            """);
        performRequestAgainstFulfillingCluster(createInferenceEndpointRequest);

        inferenceIds.add(inferenceId);
        return inferenceId;
    }

    private void createSemanticTextIndexOnFulfillingCluster(String indexName, String inferenceId) throws Exception {
        Request createIndexRequest = new Request("PUT", "/" + indexName);
        createIndexRequest.setJsonEntity(Strings.format("""
            {
              "mappings": {
                "properties": {
                  "content": {
                    "type": "semantic_text",
                    "inference_id": "%s"
                  }
                }
              }
            }
            """, inferenceId));
        performRequestAgainstFulfillingCluster(createIndexRequest);
        indices.add(indexName);
    }

    private static void indexDocOnFulfillingCluster(String indexName, String id, String content) throws Exception {
        Request indexDocRequest = new Request("POST", Strings.format("/%s/_doc/%s?refresh=true", indexName, id));
        indexDocRequest.setJsonEntity(Strings.format("""
            {
              "content": "%s"
            }
            """, content));
        performRequestAgainstFulfillingCluster(indexDocRequest);
    }

    private static Response performSearchRequest(List<String> indexNames, String query) throws Exception {
        Request searchRequest = new Request("GET", Strings.format("/%s/_search", String.join(",", indexNames)));
        searchRequest.addParameter("ccs_minimize_roundtrips", "false");
        searchRequest.setJsonEntity(Strings.format("""
            {
              "query": {
                "match": {
                  "content": "%s"
                }
              }
            }
            """, query));
        searchRequest.setOptions(
            searchRequest.getOptions().toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );

        return client().performRequest(searchRequest);
    }

    private static void deleteIndexOnFulfillingCluster(String indexName) throws Exception {
        Request deleteIndexRequest = new Request("DELETE", "/" + indexName);
        performRequestAgainstFulfillingCluster(deleteIndexRequest);
    }

    private static void deleteInferenceEndpointOnFulfillingCluster(String inferenceId, boolean force) throws Exception {
        Request deleteInferenceEndpointRequest = new Request("DELETE", "/_inference/" + inferenceId + "?force=" + force);
        performRequestAgainstFulfillingCluster(deleteInferenceEndpointRequest);
    }
}
