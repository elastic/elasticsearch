/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;

public class TestSecurityClient {

    private final RestClient client;
    private final RequestOptions options;

    public TestSecurityClient(RestClient client) {
        this(client, RequestOptions.DEFAULT);
    }

    public TestSecurityClient(RestClient client, RequestOptions options) {
        this.client = client;
        this.options = options;
    }

    /**
     * Uses the REST API to retrieve the currently authenticated user.
     * @see User.Fields
     * @see org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction
     */
    public Map<String, Object> authenticate() throws IOException {
        final String endpoint = "/_security/_authenticate";
        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        return entityAsMap(execute(request));
    }

    /**
     * Uses the REST API to create a new user in the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction
     */
    public void putUser(User user, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + user.principal();
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        final Map<String, Object> map = XContentTestUtils.convertToMap(user);
        if (password != null) {
            map.put("password", password.toString());
        }
        final String body = toJson(map);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));
        execute(request);
    }

    /**
     * Uses the REST API to delete a user from the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction
     */
    public void deleteUser(String username) throws IOException {
        final String endpoint = "/_security/user/" + username;
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        request.addParameters(Map.of("refresh", "true"));
        execute(request);
    }

    /**
     * Uses the REST API to change the password of a user in the native/reserverd realms.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction
     */
    public void changePassword(String username, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + username + "/_password";
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        final String body = """
            {
                "password": "%s"
            }
            """.formatted(password.toString());
        request.setJsonEntity(body);
        execute(request);
    }

    /**
     * Uses the REST API to enable or disable a user in the native/reserved realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction
     */
    public void setUserEnabled(String username, boolean enabled) throws IOException {
        final String endpoint = "/_security/user/" + username + "/" + (enabled ? "_enable" : "_disable");
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        execute(request);
    }

    private static String toJson(Map<String, Object> map) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().map(map);
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }

    private Response execute(Request request) throws IOException {
        request.setOptions(options);
        return this.client.performRequest(request);
    }

}
