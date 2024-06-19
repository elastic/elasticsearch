/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.secondary.auth.actions;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SecondaryAuthActionsIT extends ESRestTestCase {

    private static final String ADMIN_TOKEN = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
    private static final String USER_TOKEN = basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()));
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        // ensure secondary auth actions go across the cluster, so we don't attempt to double swap out the user in context
        .node(0, n -> n.setting("node.roles", "[master]"))
        .node(1, n -> n.setting("node.roles", "[data]"))
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", "x-pack-test-password", "superuser", false)
        .user("test_user", "x-pack-test-password", "logsrole", false)
        .plugin("secondary-auth-actions-extension")

        .build();

    @Before
    public void setup() throws IOException {
        final Request roleRequest = new Request("PUT", "/_security/role/logsrole");
        roleRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN));
        roleRequest.setJsonEntity("{\"cluster\":[],\"indices\":[{\"names\":[\"logs*\"],\"privileges\":[\"view_index_metadata\"]}]}");
        client().performRequest(roleRequest);

        final Request logsRequest = new Request("PUT", "/logs/_doc/1");
        logsRequest.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));
        logsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN));
        client().performRequest(logsRequest);

        final Request metricsRequest = new Request("PUT", "/metrics/_doc/1");
        metricsRequest.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));
        metricsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN));
        client().performRequest(metricsRequest);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", ADMIN_TOKEN).build();
    }

    public void testSecondaryAuthUser() throws IOException {
        final Request authenticateRequest = new Request("GET", "_security/_authenticate");
        authenticateRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN));
        // This should fail because the secondary auth header is not set
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(authenticateRequest));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(responseException.getMessage(), containsString("es-secondary-authorization header must be used to call action"));
        // set the secondary auth header
        authenticateRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN).addHeader("es-secondary-authorization", USER_TOKEN)
        );
        final Response authenticateResponse = client().performRequest(authenticateRequest);
        final Map<String, Object> authenticateResponseBody = entityAsMap(authenticateResponse);
        // ensure the result represents the secondary user
        assertEquals("test_user", authenticateResponseBody.get("username"));
        assertEquals(List.of("logsrole"), authenticateResponseBody.get("roles"));

        // check index level permissions
        final Request getIndicesRequest = new Request("GET", "*");
        getIndicesRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN).addHeader("es-secondary-authorization", USER_TOKEN)
        );
        final Response getIndicesResponse = client().performRequest(getIndicesRequest);
        final Map<String, Object> getIndicesResponseBody = entityAsMap(getIndicesResponse);
        assertNotNull(getIndicesResponseBody.get("logs"));
        assertNull(getIndicesResponseBody.get("metrics"));

        // invalid secondary auth header
        getIndicesRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN).addHeader("es-secondary-authorization", "junk")
        );
        responseException = expectThrows(ResponseException.class, () -> client().performRequest(getIndicesRequest));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        assertThat(responseException.getMessage(), containsString("Failed to authenticate secondary user"));
    }
}
