/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.apikey;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityInBasicRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This IT runs in the "security-basic" QA test because it has a working cluster with
 * API keys enabled, and there's no reason to have a dedicated QA project for API keys.
 */
public class ApiKeyRestIT extends SecurityInBasicRestTestCase {

    private static final String SYSTEM_USER = "system_user";
    private static final SecureString SYSTEM_USER_PASSWORD = new SecureString("sys-pass".toCharArray());
    private static final String END_USER = "end_user";
    private static final SecureString END_USER_PASSWORD = new SecureString("user-pass".toCharArray());

    @Before
    public void createUsers() throws IOException {
        createUser(SYSTEM_USER, SYSTEM_USER_PASSWORD, List.of("system_role"));
        createRole("system_role", Set.of("manage_api_key"));
        createUser(END_USER, END_USER_PASSWORD, List.of("user_role"));
        createRole("user_role", Set.of("monitor"));
    }

    @After
    public void cleanUp() throws IOException {
        deleteUser("system_user");
        deleteUser("end_user");
        deleteRole("system_role");
        deleteRole("user_role");
        invalidateApiKeysForUser(END_USER);
    }

    public void testGrantApiKeyForOtherUser() throws IOException {
        Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SYSTEM_USER, SYSTEM_USER_PASSWORD)));
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", END_USER),
            Map.entry("password", END_USER_PASSWORD.toString()),
            Map.entry("api_key", Map.of("name", "test_api_key"))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final Response response = client().performRequest(request);
        final Map<String, Object> responseBody = entityAsMap(response);

        assertThat(responseBody.get("name"), equalTo("test_api_key"));
        assertThat(responseBody.get("id"), notNullValue());
        assertThat(responseBody.get("id"), instanceOf(String.class));

        ApiKey apiKey = getApiKey((String)responseBody.get("id"));
        assertThat(apiKey.getUsername(), equalTo(END_USER));
    }
}
