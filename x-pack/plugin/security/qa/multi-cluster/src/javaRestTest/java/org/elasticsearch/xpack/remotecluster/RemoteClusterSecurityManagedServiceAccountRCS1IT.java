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
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

/**
 * Tests managed service account cross-cluster search under the RCS 1.0 (certificate-based) security
 * model. Under RCS 1.0 the querying cluster authenticates the service token locally and forwards the
 * {@code Authentication} object to the fulfilling cluster, which authorizes the request by resolving
 * the subject's role <em>names</em> against its own role store — the same semantics as native users.
 * The fulfilling cluster needs a role definition with matching name but no managed account document
 * or service token.
 */
public class RemoteClusterSecurityManagedServiceAccountRCS1IT extends AbstractRemoteClusterSecurityTestCase {

    private static final String NAMESPACE = "rcs1_poc";
    private static final String SERVICE = "worker";
    private static final String PRINCIPAL = NAMESPACE + "/" + SERVICE;
    private static final String ROLE_NAME = "managed_rcs1_role";

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").apply(commonClusterConfig).build();

        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testManagedServiceAccountCrossClusterSearchWithRcs1() throws Exception {
        // Setup RCS 1.0 (basicSecurity=true); skipUnavailable=false so remote authorization failures
        // propagate as errors instead of marking the cluster skipped
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), false);

        // Query cluster -> define the assigned role with local privileges only
        final Request putRoleRequest = new Request("PUT", "/_security/role/" + ROLE_NAME);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["local-logs"],
                  "privileges": ["read"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        // Query cluster -> create the managed service account and a token for it
        final Request putManagedAccountRequest = new Request("PUT", "/_security/service/" + NAMESPACE + "/" + SERVICE);
        putManagedAccountRequest.setJsonEntity(Strings.format("""
            {
              "roles": ["%s"],
              "enabled": true
            }""", ROLE_NAME));
        assertOK(adminClient().performRequest(putManagedAccountRequest));

        final Request createTokenRequest = new Request("PUT", "/_security/service/" + NAMESPACE + "/" + SERVICE + "/credential/token/t1");
        final String serviceToken = ObjectPath.createFromResponse(adminClient().performRequest(createTokenRequest)).evaluate("token.value");
        final RequestOptions bearerAuth = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + serviceToken).build();

        // Query cluster -> the managed service account authenticates with assigned role names
        final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
        authenticateRequest.setOptions(bearerAuth);
        final Map<String, Object> authenticateResponse = entityAsMap(client().performRequest(authenticateRequest));
        assertThat(authenticateResponse, hasEntry("username", PRINCIPAL));
        assertThat(authenticateResponse.get("roles"), equalTo(List.of(ROLE_NAME)));
        @SuppressWarnings("unchecked")
        final Map<String, Object> metadata = (Map<String, Object>) authenticateResponse.get("metadata");
        assertThat(metadata.get("_managed_service_account"), is(true));

        // Query cluster -> local search authorized by the local role definition
        final Request indexLocalDocRequest = new Request("POST", "/local-logs/_doc?refresh=true");
        indexLocalDocRequest.setJsonEntity("{\"local\": \"doc\"}");
        assertOK(adminClient().performRequest(indexLocalDocRequest));
        final Request localSearchRequest = new Request("GET", "/local-logs/_search");
        localSearchRequest.setOptions(bearerAuth);
        assertOK(client().performRequest(localSearchRequest));

        // Fulfilling cluster -> create test indices
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "shared-logs" } }
            { "shared": "logs" }
            { "index": { "_index": "secretindex" } }
            { "secret": "index" }
            """);
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        {
            // TEST CASE 1: role name not defined on the fulfilling cluster -> remote search is denied there
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithBearerAuth(new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":shared-logs/_search"), bearerAuth)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(exception.getMessage(), containsString("unauthorized for service account [" + PRINCIPAL + "]"));
        }

        // Fulfilling cluster -> define the same role name with local index privileges (RCS 1.0 semantics)
        final Request putRoleOnFulfillingClusterRequest = new Request("PUT", "/_security/role/" + ROLE_NAME);
        putRoleOnFulfillingClusterRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["shared-logs"],
                  "privileges": ["read"]
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(putRoleOnFulfillingClusterRequest));

        {
            // TEST CASE 2: remote search on the granted index succeeds
            final Response response = performRequestWithBearerAuth(
                new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":shared-logs/_search"),
                bearerAuth
            );
            assertOK(response);
            final ObjectPath objectPath = ObjectPath.createFromResponse(response);
            assertThat(objectPath.evaluate("hits.total.value"), is(1));
            assertThat(objectPath.evaluate("hits.hits.0._index"), equalTo(REMOTE_CLUSTER_ALIAS + ":shared-logs"));
        }
        {
            // TEST CASE 3: remote search on an index outside the fulfilling cluster's role definition is denied
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithBearerAuth(new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":secretindex/_search"), bearerAuth)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(exception.getMessage(), containsString("unauthorized for service account [" + PRINCIPAL + "]"));
            assertThat(exception.getMessage(), containsString("on indices [secretindex]"));
        }
        {
            // The fulfilling cluster has no managed account document; authentication happened on the query cluster
            final Request getManagedAccountsRequest = new Request("GET", "/_security/service/" + NAMESPACE);
            final Map<String, Object> accounts = entityAsMap(performRequestAgainstFulfillingCluster(getManagedAccountsRequest));
            assertThat(accounts, anEmptyMap());
        }
    }

    private Response performRequestWithBearerAuth(Request request, RequestOptions options) throws IOException {
        request.setOptions(options);
        return client().performRequest(request);
    }
}
