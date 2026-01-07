/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteClusterSecurityQueryRewriteIT extends AbstractRemoteClusterSecurityTestCase {

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
            // Add inference module - this is the key: with inference module loaded,
            // the bug is triggered for ALL CCS queries
            .module("x-pack-inference")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            // Add inference module
            .module("x-pack-inference")
            // API key-based credentials for cross-cluster access (RCS 2.0) - triggers the bug
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                              "names": ["test-*"]
                            }
                          ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_SEARCH_USER, PASS.toString(), "remote_search", false)
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    /**
     * Test that reproduces bug #140193: ANY cross-cluster search fails when the inference
     * module is loaded and remote clusters use API key-based authentication.
     *
     * <p>Even a simple match query on a regular text field (NOT a semantic_text field) triggers
     * the bug because the inference module intercepts all search requests to check for
     * inference fields.
     */
    public void testBasicMatchQueryFailsWithInferenceModuleAndApiKeyAuth() throws Exception {
        configureRemoteCluster();

        // Create a simple test index on fulfilling (remote) cluster
        // Note: This is a regular text field, NOT a semantic_text field!
        Request createIndexRequest = new Request("PUT", "/test-index");
        createIndexRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "content": { "type": "text" }
                }
              }
            }
            """);
        performRequestAgainstFulfillingCluster(createIndexRequest);

        // Index a document
        Request indexDocRequest = new Request("POST", "/test-index/_doc?refresh=true");
        indexDocRequest.setJsonEntity("""
            {
              "content": "test document for cross cluster search"
            }
            """);
        performRequestAgainstFulfillingCluster(indexDocRequest);

        // Execute a basic match query with ccs_minimize_roundtrips=false
        // This should be a simple search, but the inference module intercepts it
        Request searchRequest = new Request("GET", "/my_remote_cluster:test-*/_search");
        searchRequest.addParameter("ccs_minimize_roundtrips", "false");
        searchRequest.setJsonEntity("""
            {
              "query": {
                "match": {
                  "content": "test"
                }
              }
            }
            """);
        searchRequest.setOptions(
            searchRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );

        Response response = client().performRequest(searchRequest);
        assertOK(response);
    }
}
