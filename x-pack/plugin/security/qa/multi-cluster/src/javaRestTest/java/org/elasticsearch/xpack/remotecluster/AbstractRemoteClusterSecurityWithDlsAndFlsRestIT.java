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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

public abstract class AbstractRemoteClusterSecurityWithDlsAndFlsRestIT extends AbstractRemoteClusterSecurityTestCase {

    protected static final String REMOTE_SEARCH_USER_NO_DLS_FLS = REMOTE_SEARCH_USER + "_no_dls_fls";
    protected static final String REMOTE_SEARCH_USER_DLS_FLS = REMOTE_SEARCH_USER + "_dls_fls";
    protected static final String REMOTE_SEARCH_USER_DLS = REMOTE_SEARCH_USER + "_dls";
    protected static final String REMOTE_SEARCH_USER_FLS = REMOTE_SEARCH_USER + "_fls";

    /**
     * Creates remote search users where each has access to all remote clusters but with different DLS/FLS restrictions.
     *
     * @throws IOException in case of an I/O errors
     */
    private void createRemoteSearchUsers() throws IOException {

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_NO_DLS_FLS, REMOTE_SEARCH_ROLE + "_no_dls_fls", """
            {
              "cluster": ["manage_own_api_key"],
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
              "cluster": ["manage_own_api_key"],
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
                  "field_security": {"grant": [ "field2" ]}
                },
                {
                  "names": ["remote_index1", "remote_index2", "remote_index3"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster*"],
                  "query": {
                     "bool": {
                        "should" : [
                           { "term" : { "field2" : "value1" } },
                           { "term" : { "field1" : "value2" } }
                        ],
                        "minimum_should_match" : 1
                      }
                  },
                  "field_security": {"grant": [ "field1" ]}
                }
              ]
            }""");

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_DLS, REMOTE_SEARCH_ROLE + "_dls", """
            {
              "cluster": ["manage_own_api_key"],
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "query": {"bool": { "must_not": { "term" : {"field1" : "value1"}}}}
                },
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "query": {"bool": { "must_not": { "term" : {"field2" : "value1"}}}}
                }
              ]
            }""");

        createRemoteSearchUserAndRole(REMOTE_SEARCH_USER_FLS, REMOTE_SEARCH_ROLE + "_fls", """
            {
              "cluster": ["manage_own_api_key"],
              "remote_indices": [
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "field_security": {"grant": [ "field1", "field2" ], "except": ["field2"]}
                },
                {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_*_cluster*"],
                  "field_security": {"grant": [ "field3" ]}
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

    protected void setupRemoteClusterTestCase(String clusterAlias) throws Exception {
        // Configures one of pre-defined remote clusters on querying cluster side where each remote cluster uses
        // a different API key that has (or doesn't) various DLS/FLS restrictions.
        configureRemoteCluster(clusterAlias);

        // Insert some documents on the fulfilling cluster, so we can attempt to search it from the querying cluster.
        createIndicesOnFulfillingCluster();

        // Create remote search users (on querying cluster) with combinations of DLS/FLS restrictions.
        createRemoteSearchUsers();
    }

    protected void assertSearchResponseContainsExpectedIndicesAndFields(
        Response searchResponse,
        String[] expectedRemoteIndices,
        String[] expectedFields
    ) throws IOException {
        final var searchResult = Arrays.stream(SearchResponse.fromXContent(responseAsParser(searchResponse)).getHits().getHits())
            .collect(Collectors.toMap(SearchHit::getIndex, SearchHit::getSourceAsMap));

        assertThat(searchResult.keySet(), containsInAnyOrder(expectedRemoteIndices));
        for (String remoteIndex : expectedRemoteIndices) {
            assertThat(searchResult.get(remoteIndex).keySet(), containsInAnyOrder(expectedFields));
        }
    }

    protected Response performRequestAgainstQueryingCluster(final Request request, final String username) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(username, PASS)));
        return client().performRequest(request);
    }

    protected static String createApiKeyForRemoteCluster(String roleDescriptorsJson, AtomicReference<Map<String, Object>> apiKeyRef) {
        if (apiKeyRef.get() == null) {
            apiKeyRef.set(createCrossClusterAccessApiKey(roleDescriptorsJson));
        }
        return (String) apiKeyRef.get().get("encoded");
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

}
