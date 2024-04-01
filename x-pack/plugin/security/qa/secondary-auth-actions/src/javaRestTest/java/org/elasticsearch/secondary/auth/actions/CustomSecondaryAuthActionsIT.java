/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.secondary.auth.actions;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CustomSecondaryAuthActionsIT extends ESRestTestCase {

    private static final String ADMIN_TOKEN = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
    private static final String USER_TOKEN = basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()));
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", "x-pack-test-password", "superuser", false)
        .user("test_user", "x-pack-test-password", "logsrole", false)
        .plugin("secondary-auth-actions-extension")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", ADMIN_TOKEN).build();
    }

    public void testSecondaryAuthUser() throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN));
        // This should fail because the secondary auth header is not set
        ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(responseException.getMessage(), containsString("es-secondary-authorization header must be used to call action"));
        // set the secondary auth header
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", ADMIN_TOKEN).addHeader("es-secondary-authorization", USER_TOKEN)
        );
        final Response response = client().performRequest(request);
        final Map<String, Object> responseBody = entityAsMap(response);
        // ensure the result represents the secondary user
        assertEquals("test_user", responseBody.get("username"));
        assertEquals(List.of("logsrole"), responseBody.get("roles"));
    }
}
