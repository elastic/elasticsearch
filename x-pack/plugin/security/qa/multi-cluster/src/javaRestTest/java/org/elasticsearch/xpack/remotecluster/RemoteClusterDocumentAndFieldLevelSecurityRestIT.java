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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class RemoteClusterDocumentAndFieldLevelSecurityRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final String REMOTE_CLUSTER_NO_DLS_FLS = REMOTE_CLUSTER_ALIAS + "_no_dls_fls";
    private static final String REMOTE_CLUSTER_DLS_FLS = REMOTE_CLUSTER_ALIAS + "_dls_fls";
    private static final String REMOTE_CLUSTER_DLS = REMOTE_CLUSTER_ALIAS + "_dls";
    private static final String REMOTE_CLUSTER_FLS = REMOTE_CLUSTER_ALIAS + "_fls";

    protected static final String REMOTE_SEARCH_USER_NO_DLS_FLS = REMOTE_SEARCH_USER + "_no_dls_fls";
    protected static final String REMOTE_SEARCH_USER_DLS_FLS = REMOTE_SEARCH_USER + "_dls_fls";
    protected static final String REMOTE_SEARCH_USER_DLS = REMOTE_SEARCH_USER + "_dls";
    protected static final String REMOTE_SEARCH_USER_FLS = REMOTE_SEARCH_USER + "_fls";

    private static final Map<String, AtomicReference<Map<String, Object>>> API_KEY_REFERENCES = Map.ofEntries(
        Map.entry(REMOTE_CLUSTER_NO_DLS_FLS, new AtomicReference<>()),
        Map.entry(REMOTE_CLUSTER_DLS_FLS, new AtomicReference<>()),
        Map.entry(REMOTE_CLUSTER_DLS, new AtomicReference<>()),
        Map.entry(REMOTE_CLUSTER_FLS, new AtomicReference<>())
    );

    private static final Map<String, String> API_KEY_ROLES = Map.ofEntries(Map.entry(REMOTE_CLUSTER_NO_DLS_FLS, """
        {
          "role": {
            "cluster": ["cross_cluster_access"],
            "index": [
              {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"]
              }
            ]
          }
        }"""), Map.entry(REMOTE_CLUSTER_DLS_FLS, """
        {
          "role": {
            "cluster": ["cross_cluster_access"],
            "index": [
              {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "query": {"bool": { "must_not": { "term" : {"field2" : "value2"}}}},
                  "field_security": {"grant": [ "field1", "field2" ]}
              }
            ]
          }
        }"""), Map.entry(REMOTE_CLUSTER_DLS, """
        {
          "role": {
            "cluster": ["cross_cluster_access"],
            "index": [
              {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "query": {"term" : {"field1" : "value1"}}
              }
            ]
          }
        }"""), Map.entry(REMOTE_CLUSTER_FLS, """
        {
          "role": {
            "cluster": ["cross_cluster_access"],
            "index": [
              {
                  "names": ["remote_index2", "remote_index2"],
                  "privileges": ["read", "read_cross_cluster"],
                  "field_security": {"grant": [ "field3" ]}
              }
            ]
          }
        }"""));

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
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
                "cluster.remote." + REMOTE_CLUSTER_NO_DLS_FLS + ".credentials",
                () -> createApiKeyForRemoteCluster(REMOTE_CLUSTER_NO_DLS_FLS)
            )
            .keystore(
                "cluster.remote." + REMOTE_CLUSTER_DLS_FLS + ".credentials",
                () -> createApiKeyForRemoteCluster(REMOTE_CLUSTER_DLS_FLS)
            )
            .keystore("cluster.remote." + REMOTE_CLUSTER_DLS + ".credentials", () -> createApiKeyForRemoteCluster(REMOTE_CLUSTER_DLS))
            .keystore("cluster.remote." + REMOTE_CLUSTER_FLS + ".credentials", () -> createApiKeyForRemoteCluster(REMOTE_CLUSTER_FLS))
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    /**
     * Creates remote search users on querying cluster where each has access to all remote clusters but with different DLS/FLS restrictions.
     *
     * @throws IOException in case of an I/O errors
     */
    private void createRemoteSearchUsers() throws IOException {

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_NO_DLS_FLS, REMOTE_SEARCH_ROLE + "_no_dls_fls", """
            {
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"]
                }
              ]
            }""");

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_DLS_FLS, REMOTE_SEARCH_ROLE + "_dls_fls", """
            {
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "query": {
                     "bool": {
                        "should" : [
                           { "term" : { "field1" : "value1" } },
                           { "term" : { "field2" : "value2" } }
                        ],
                        "minimum_should_match" : 1
                      }
                  },
                  "field_security": {"grant": [ "field1", "field2" ]}
                }
              ]
            }""");

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_DLS, REMOTE_SEARCH_ROLE + "_dls", """
            {
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "query": {"bool": { "must_not": { "term" : {"field1" : "value1"}}}}
                }
              ]
            }""");

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_FLS, REMOTE_SEARCH_ROLE + "_fls", """
            {
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "field_security": {"grant": [ "field1", "field2", "field3" ], "except": ["field2"]}
                }
              ]
            }""");
    }

    private void createIndicesOnFulfillingCluster() throws IOException {
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "remote_index1" } }
            { "field1": "value1", "field2": "value1", "field3": "value1" }
            { "index": { "_index": "remote_index2" } }
            { "field1": "value2", "field2": "value2", "field3": "value2" }
            { "index": { "_index": "remote_index3" } }
            { "field1": "value3", "field2": "value3", "field3": "value3" }
            { "index": { "_index": "remote_index4" } }
            { "field1": "value4", "field2": "value4", "field3": "value4" }
            { "index": { "_index": "not-shared-index1" } }
            { "name": "foo" }
            { "index": { "_index": "not-shared-index2" } }
            { "name": "bar" }
            { "index": { "_index": "not-shared-index3" } }
            { "name": "baz" }\n""");
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
    }

    public void testCrossClusterSearch() throws Exception {
        // Configure remote clusters on querying cluster where each remote cluster uses
        // different API key that has (or doesn't) various DLS/FLS restrictions.
        configureRemoteCluster(REMOTE_CLUSTER_NO_DLS_FLS);
        configureRemoteCluster(REMOTE_CLUSTER_DLS_FLS);
        configureRemoteCluster(REMOTE_CLUSTER_DLS);
        configureRemoteCluster(REMOTE_CLUSTER_FLS);

        // Insert some documents on the fulfilling cluster, so we can attempt to search it from the querying cluster.
        createIndicesOnFulfillingCluster();

        // Create remote search users (on querying cluster) with combinations of DLS/FLS restrictions.
        createRemoteSearchUsers();

        testCrossClusterSearchUsingApiKeyWithoutDlsAndFls();
    }

    /**
     * Tests cross cluster search with different users against fulfilling cluster using API key without DLS and FLS restrictions.
     * Expectation is that only user defined DLS and FLS restrictions will be respected.
     */
    private void testCrossClusterSearchUsingApiKeyWithoutDlsAndFls() throws IOException {
        final Request searchRequest = new Request(
            "GET",
            Strings.format(
                "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                randomFrom(REMOTE_CLUSTER_NO_DLS_FLS),
                randomFrom("remote_index*", "*"),
                randomBoolean()
            )
        );

        // Running a CCS request with a user without DLS/FLS should return all remote indices and their fields.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_NO_DLS_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1", "remote_index2", "remote_index3", "remote_index4" },
                new String[] { "field1", "field2", "field3" }
            );
        }

        // Running a CCS request with a user with DLS and FLS.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_DLS_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1", "remote_index2" },
                new String[] { "field1", "field2" }
            );
        }

        // Running a CCS request with a user with DLS only.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_DLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index2", "remote_index3", "remote_index4" },
                new String[] { "field1", "field2", "field3" }
            );
        }

        // Running a CCS request with a user with FLS only.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1", "remote_index2", "remote_index3", "remote_index4" },
                new String[] { "field1", "field3" }
            );
        }

    }

    private void assertSearchResponseContainsExpectedIndicesAndFields(
        Response searchResponse,
        String[] expectedRemoteIndices,
        String[] expectedFields
    ) throws IOException {
        final var searchResult = readIndicesAndFields(SearchResponse.fromXContent(responseAsParser(searchResponse)));
        assertThat(searchResult.keySet(), containsInAnyOrder(expectedRemoteIndices));
        for (String remoteIndex : expectedRemoteIndices) {
            assertThat(searchResult.get(remoteIndex), containsInAnyOrder(expectedFields));
        }
    }

    private void createRemoteSearchUserAndRole(String username, String roleName, String roleJson) throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(roleJson);
        assertOK(adminClient().performRequest(putRoleRequest));

        final var putUserRequest = new Request("PUT", "/_security/user/" + username);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", PASS, roleName));
        assertOK(adminClient().performRequest(putUserRequest));
    }

    private Response performRequestAgainstQueryingCluster(final Request request, final String username) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(username, PASS)));
        return client().performRequest(request);
    }

    private Map<String, Set<String>> readIndicesAndFields(SearchResponse response) {
        final Map<String, Set<String>> result = new HashMap<>();
        for (var hit : response.getHits().getHits()) {
            result.put(hit.getIndex(), hit.getSourceAsMap().keySet());
        }
        return result;
    }

    private static String createApiKeyForRemoteCluster(String clusterAlias) {
        final var apiKeyRef = API_KEY_REFERENCES.get(clusterAlias);
        if (apiKeyRef.get() == null) {
            apiKeyRef.set(createCrossClusterAccessApiKey(API_KEY_ROLES.get(clusterAlias)));
        }
        return (String) apiKeyRef.get().get("encoded");
    }

}
