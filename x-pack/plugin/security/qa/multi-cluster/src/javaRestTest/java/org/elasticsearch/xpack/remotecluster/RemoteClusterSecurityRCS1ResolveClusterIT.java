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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.search.SearchResponse.LOCAL_CLUSTER_NAME_REPRESENTATION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the _resolve/cluster API under RCS1.0 security model
 */
public class RemoteClusterSecurityRCS1ResolveClusterIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").nodes(3).apply(commonClusterConfig).build();

        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @SuppressWarnings("unchecked")
    public void testResolveClusterUnderRCS1() throws Exception {
        // Setup RCS 1.0 (basicSecurity=true)
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());

        {
            // Query cluster -> add role for test user - do not give any privileges for remote_indices
            var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["local_index"],
                      "privileges": ["read"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));

            // Query cluster -> create user and assign role
            var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Query cluster -> create test index
            var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Fulfilling cluster -> create test indices
            Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "secretindex" } }
                { "bar": "foo" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }
        {
            // TEST CASE 1: Query cluster -> try to resolve local and remote star patterns (no access to remote cluster)
            Request starResolveRequest = new Request("GET", "_resolve/cluster/*,my_remote_cluster:*");
            Response response = performRequestWithRemoteSearchUser(starResolveRequest);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertLocalMatching(responseMap);

            Map<String, ?> remoteClusterResponse = (Map<String, ?>) responseMap.get("my_remote_cluster");
            assertThat((Boolean) remoteClusterResponse.get("connected"), equalTo(true));
            assertThat((String) remoteClusterResponse.get("error"), containsString("unauthorized for user [remote_search_user]"));

            // TEST CASE 2: Query cluster -> add user role and user on remote cluster and try resolve again
            var putRoleOnRemoteClusterRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleOnRemoteClusterRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["index*"],
                      "privileges": ["read", "read_cross_cluster"]
                    }
                  ]
                }""");
            assertOK(performRequestAgainstFulfillingCluster(putRoleOnRemoteClusterRequest));

            var putUserOnRemoteClusterRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserOnRemoteClusterRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(performRequestAgainstFulfillingCluster(putUserOnRemoteClusterRequest));

            // Query cluster -> resolve local and remote with proper access
            response = performRequestWithRemoteSearchUser(starResolveRequest);
            assertOK(response);
            responseMap = responseAsMap(response);
            assertLocalMatching(responseMap);
            assertRemoteMatching(responseMap);
        }
        {
            // TEST CASE 3: Query cluster -> resolve index1 for local index without any local privilege
            Request localOnly1 = new Request("GET", "_resolve/cluster/index1");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(localOnly1));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(
                exc.getMessage(),
                containsString(
                    "action [indices:admin/resolve/cluster] is unauthorized for user "
                        + "[remote_search_user] with effective roles [remote_search] on indices [index1]"
                )
            );
        }
        {
            // TEST CASE 4: Query cluster -> resolve local for local index without any local privilege using wildcard
            Request localOnlyWildcard1 = new Request("GET", "_resolve/cluster/index1*");
            Response response = performRequestWithRemoteSearchUser(localOnlyWildcard1);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertMatching((Map<String, Object>) responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), false);
        }
        {
            // TEST CASE 5: Query cluster -> resolve remote and local without permission where using wildcard 'index1*'
            Request localNoPermsRemoteWithPerms = new Request("GET", "_resolve/cluster/index1*,my_remote_cluster:index1");
            Response response = performRequestWithRemoteSearchUser(localNoPermsRemoteWithPerms);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertMatching((Map<String, Object>) responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), false);
            assertRemoteMatching(responseMap);
        }
        {
            // TEST CASE 6: Query cluster -> resolve remote only for existing and privileged index
            Request remoteOnly1 = new Request("GET", "_resolve/cluster/my_remote_cluster:index1");
            Response response = performRequestWithRemoteSearchUser(remoteOnly1);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), nullValue());
            assertRemoteMatching(responseMap);
        }
        {
            // TEST CASE 7: Query cluster -> resolve remote only for existing but non-privileged index
            Request remoteOnly2 = new Request("GET", "_resolve/cluster/my_remote_cluster:secretindex");
            Response response = performRequestWithRemoteSearchUser(remoteOnly2);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), nullValue());
            Map<String, ?> remoteClusterResponse = (Map<String, ?>) responseMap.get("my_remote_cluster");
            assertThat((Boolean) remoteClusterResponse.get("connected"), equalTo(true));
            assertThat((String) remoteClusterResponse.get("error"), containsString("unauthorized for user [remote_search_user]"));
            assertThat((String) remoteClusterResponse.get("error"), containsString("on indices [secretindex]"));
        }
        {
            // TEST CASE 8: Query cluster -> resolve remote only for non-existing and non-privileged index
            Request remoteOnly3 = new Request("GET", "_resolve/cluster/my_remote_cluster:doesnotexist");
            Response response = performRequestWithRemoteSearchUser(remoteOnly3);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), nullValue());
            Map<String, ?> remoteClusterResponse = (Map<String, ?>) responseMap.get("my_remote_cluster");
            assertThat((Boolean) remoteClusterResponse.get("connected"), equalTo(true));
            assertThat((String) remoteClusterResponse.get("error"), containsString("unauthorized for user [remote_search_user]"));
            assertThat((String) remoteClusterResponse.get("error"), containsString("on indices [doesnotexist]"));
        }
        {
            // TEST CASE 9: Query cluster -> resolve remote only for non-existing but privileged (by index pattern) index
            Request remoteOnly4 = new Request("GET", "_resolve/cluster/my_remote_cluster:index99");
            Response response = performRequestWithRemoteSearchUser(remoteOnly4);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), nullValue());
            Map<String, ?> remoteClusterResponse = (Map<String, ?>) responseMap.get("my_remote_cluster");
            assertThat((Boolean) remoteClusterResponse.get("connected"), equalTo(true));
            assertThat((String) remoteClusterResponse.get("error"), containsString("no such index [index99]"));
        }
        {
            // TEST CASE 10: Query cluster -> resolve remote only for some existing/privileged,
            // non-existing/privileged, existing/non-privileged
            Request remoteOnly5 = new Request(
                "GET",
                "_resolve/cluster/my_remote_cluster:index1,my_remote_cluster:secretindex,my_remote_cluster:index99"
            );
            Response response = performRequestWithRemoteSearchUser(remoteOnly5);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), nullValue());
            Map<String, ?> remoteClusterResponse = (Map<String, ?>) responseMap.get("my_remote_cluster");
            assertThat((Boolean) remoteClusterResponse.get("connected"), equalTo(true));
            assertThat((String) remoteClusterResponse.get("error"), containsString("unauthorized for user [remote_search_user]"));
            assertThat((String) remoteClusterResponse.get("error"), containsString("on indices [secretindex]"));
        }
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private void assertLocalMatching(Map<String, Object> responseMap) {
        assertMatching((Map<String, Object>) responseMap.get(LOCAL_CLUSTER_NAME_REPRESENTATION), true);
    }

    @SuppressWarnings("unchecked")
    private void assertRemoteMatching(Map<String, Object> responseMap) {
        assertMatching((Map<String, Object>) responseMap.get("my_remote_cluster"), true);
    }

    private void assertMatching(Map<String, Object> perClusterResponse, boolean matching) {
        assertThat((Boolean) perClusterResponse.get("connected"), equalTo(true));
        assertThat((Boolean) perClusterResponse.get("matching_indices"), equalTo(matching));
        assertThat(perClusterResponse.get("version"), notNullValue());
    }
}
