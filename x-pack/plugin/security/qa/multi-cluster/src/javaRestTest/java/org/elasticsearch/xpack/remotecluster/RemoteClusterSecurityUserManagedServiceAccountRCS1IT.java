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
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
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
 * Under RCS 1.0 the querying cluster authenticates the service token and forwards the {@code Authentication}
 * object. The fulfilling cluster authorizes by resolving the subject's role names against its own role store,
 * the same way it does for a native user, so it needs a role of the matching name but neither an account
 * document nor a service token.
 */
public class RemoteClusterSecurityUserManagedServiceAccountRCS1IT extends AbstractRemoteClusterSecurityTestCase {

    private static final String NAMESPACE = "rcs1";
    private static final String SERVICE = "worker";
    private static final String PRINCIPAL = NAMESPACE + "/" + SERVICE;
    private static final String ROLE_NAME = "user_managed_rcs1_role";

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").apply(commonClusterConfig).build();

        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testUserManagedServiceAccountCrossClusterSearchWithRcs1() throws Exception {
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), false);

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

        final Request putAccountRequest = new Request("PUT", "/_security/service/" + NAMESPACE + "/" + SERVICE);
        putAccountRequest.setJsonEntity(Strings.format("""
            {
              "roles": ["%s"],
              "enabled": true
            }""", ROLE_NAME));
        assertOK(adminClient().performRequest(putAccountRequest));

        final Request createTokenRequest = new Request("PUT", "/_security/service/" + NAMESPACE + "/" + SERVICE + "/credential/token/t1");
        final String serviceToken = ObjectPath.createFromResponse(adminClient().performRequest(createTokenRequest)).evaluate("token.value");
        final RequestOptions bearerAuth = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + serviceToken).build();

        final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
        authenticateRequest.setOptions(bearerAuth);
        final Map<String, Object> authenticateResponse = entityAsMap(client().performRequest(authenticateRequest));
        assertThat(authenticateResponse, hasEntry("username", PRINCIPAL));
        assertThat(authenticateResponse.get("roles"), equalTo(List.of(ROLE_NAME)));
        @SuppressWarnings("unchecked")
        final Map<String, Object> metadata = (Map<String, Object>) authenticateResponse.get("metadata");
        assertThat(metadata.get(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD), is(true));

        final Request indexLocalDocRequest = new Request("POST", "/local-logs/_doc?refresh=true");
        indexLocalDocRequest.setJsonEntity("{\"local\": \"doc\"}");
        assertOK(adminClient().performRequest(indexLocalDocRequest));
        final Request localSearchRequest = new Request("GET", "/local-logs/_search");
        localSearchRequest.setOptions(bearerAuth);
        assertOK(client().performRequest(localSearchRequest));

        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "shared-logs" } }
            { "shared": "logs" }
            { "index": { "_index": "secretindex" } }
            { "secret": "index" }
            """);
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        {
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithBearerAuth(new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":shared-logs/_search"), bearerAuth)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(exception.getMessage(), containsString("unauthorized for service account [" + PRINCIPAL + "]"));
        }

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
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithBearerAuth(new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":secretindex/_search"), bearerAuth)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(403));
            assertThat(exception.getMessage(), containsString("unauthorized for service account [" + PRINCIPAL + "]"));
            assertThat(exception.getMessage(), containsString("on indices [secretindex]"));
        }
        {
            final Request getAccountsRequest = new Request("GET", "/_security/service/" + NAMESPACE);
            final Map<String, Object> accounts = entityAsMap(performRequestAgainstFulfillingCluster(getAccountsRequest));
            assertThat(accounts, anEmptyMap());
        }
    }

    private Response performRequestWithBearerAuth(Request request, RequestOptions options) throws IOException {
        request.setOptions(options);
        return client().performRequest(request);
    }
}
