/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Map;

public class SecurityClientTestHelper {

    /**
     * Uses the REST API to create a new user in the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction
     */
    public static void putUser(RestClient client, User user, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + user.principal();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        final Map<String, Object> map = XContentTestUtils.convertToMap(user);
        if (password != null) {
            map.put("password", password.toString());
        }
        final String body = toJson(map);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));
        client.performRequest(request);
    }

    /**
     * Uses the REST API to delete a user from the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction
     */
    public static void deleteUser(RestClient client, String username) throws IOException {
        final String endpoint = "/_security/user/" + username;
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        request.addParameters(Map.of("refresh", "true"));
        client.performRequest(request);
    }

    /**
     * Uses the REST API to enable or disable a user in the native/reserved realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction
     */
    public static void setUserEnabled(RestClient client, String username, boolean enabled) throws IOException {
        final String endpoint = "/_security/user/" + username + "/" + (enabled ? "_enable" : "_disable");
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setOptions(SecuritySettingsSource.SECURITY_REQUEST_OPTIONS);
        client.performRequest(request);
    }

    private static String toJson(Map<String, Object> map) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().map(map);
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }
}
