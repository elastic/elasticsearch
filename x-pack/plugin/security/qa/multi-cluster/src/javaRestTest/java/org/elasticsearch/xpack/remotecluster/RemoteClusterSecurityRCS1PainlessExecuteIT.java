/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.util.EntityUtils;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests cross-cluster painless/execute API under RCS1.0 security model
 */
public class RemoteClusterSecurityRCS1PainlessExecuteIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").nodes(3).apply(commonClusterConfig).build();

        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @SuppressWarnings({ "unchecked", "checkstyle:LineLength" })
    public void testPainlessExecute() throws Exception {
        // Setup RCS 1.0 (basicSecurity=true)
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());
        {
            // Query cluster -> add role for test user - do not give any privileges for remote_indices
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["local_index", "my_local*"],
                      "privileges": ["read"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));

            // Query cluster -> create user and assign role
            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Query cluster -> create test index
            final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Fulfilling cluster -> create test indices
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "secretindex" } }
                { "bar": "foo" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        {
            // TEST CASE 1: Query local cluster for local_index - should work since role has read perms for it
            Request painlessExecuteLocal = createPainlessExecuteRequest("local_index");
            Response response = performRequestWithRemoteSearchUser(painlessExecuteLocal);
            assertOK(response);
            String responseBody = EntityUtils.toString(response.getEntity());
            assertThat(responseBody, equalTo("{\"result\":[\"test\"]}"));
        }
        {
            // TEST CASE 2: Query remote cluster for index1 - should fail since no permissions granted for remote clusters yet
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:index1");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteRemote));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(403));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("unauthorized for user [remote_search_user]"));
            assertThat(errorResponseBody, containsString("on indices [index1]"));
            assertThat(errorResponseBody, containsString("\"type\":\"security_exception\""));
        }
        {
            // add user role and user on remote cluster
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
        }
        {
            // TEST CASE 3: Query remote cluster for secretindex - should fail since no perms granted for it
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:secretindex");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteRemote));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(errorResponseBody, containsString("unauthorized for user [remote_search_user]"));
            assertThat(errorResponseBody, containsString("on indices [secretindex]"));
            assertThat(errorResponseBody, containsString("\"type\":\"security_exception\""));
        }
        {
            // TEST CASE 4: Query remote cluster for index1 - should succeed since read and cross-cluster-read perms granted
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:index1");
            Response response = performRequestWithRemoteSearchUser(painlessExecuteRemote);
            String responseBody = EntityUtils.toString(response.getEntity());
            assertOK(response);
            assertThat(responseBody, equalTo("{\"result\":[\"test\"]}"));
        }
        {
            // TEST CASE 5: Query local cluster for not_present index - should fail with 403 since role does not have perms for this index
            Request painlessExecuteLocal = createPainlessExecuteRequest("index_not_present");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteLocal));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(403));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("unauthorized for user [remote_search_user]"));
            assertThat(errorResponseBody, containsString("on indices [index_not_present]"));
            assertThat(errorResponseBody, containsString("\"type\":\"security_exception\""));
        }
        {
            // TEST CASE 6: Query local cluster for my_local_123 index - role has perms for this pattern, but index does not exist, so 404
            Request painlessExecuteLocal = createPainlessExecuteRequest("my_local_123");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteLocal));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(404));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("\"type\":\"index_not_found_exception\""));
        }
        {
            // TEST CASE 7: Query local cluster for my_local* index - painless/execute does not allow wildcards, so fails with 400
            Request painlessExecuteLocal = createPainlessExecuteRequest("my_local*");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteLocal));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(400));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("indices:data/read/scripts/painless/execute does not support wildcards"));
            assertThat(errorResponseBody, containsString("\"type\":\"illegal_argument_exception\""));
        }
        {
            // TEST CASE 8: Query remote cluster for cluster that does not exist, and user does not have perms for that pattern - 403 ???
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:abc123");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteRemote));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(403));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("unauthorized for user [remote_search_user]"));
            assertThat(errorResponseBody, containsString("on indices [abc123]"));
            assertThat(errorResponseBody, containsString("\"type\":\"security_exception\""));
        }
        {
            // TEST CASE 9: Query remote cluster for cluster that does not exist, but has permissions for the index pattern - 404
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:index123");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteRemote));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(404));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("\"type\":\"index_not_found_exception\""));
        }
        {
            // TEST CASE 10: Query remote cluster with wildcard in index - painless/execute does not allow wildcards, so fails with 400
            Request painlessExecuteRemote = createPainlessExecuteRequest("my_remote_cluster:index*");
            ResponseException exc = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(painlessExecuteRemote));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), is(400));
            String errorResponseBody = EntityUtils.toString(exc.getResponse().getEntity());
            assertThat(errorResponseBody, containsString("indices:data/read/scripts/painless/execute does not support wildcards"));
            assertThat(errorResponseBody, containsString("\"type\":\"illegal_argument_exception\""));
        }
    }

    private static Request createPainlessExecuteRequest(String indexExpression) {
        Request painlessExecuteLocal = new Request("POST", "_scripts/painless/_execute");
        String body = """
            {
                "script": {
                    "source": "emit(\\"test\\")"
                },
                "context": "keyword_field",
                "context_setup": {
                    "index": "INDEX_EXPRESSION_HERE",
                    "document": {
                        "@timestamp": "2023-05-06T16:22:22.000Z"
                    }
                }
            }""".replace("INDEX_EXPRESSION_HERE", indexExpression);
        painlessExecuteLocal.setJsonEntity(body);
        return painlessExecuteLocal;
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }
}
