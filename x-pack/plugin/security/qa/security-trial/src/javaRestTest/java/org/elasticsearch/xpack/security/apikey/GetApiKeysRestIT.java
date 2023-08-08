/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.apikey;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;

public class GetApiKeysRestIT extends SecurityOnTrialLicenseRestTestCase {
    private static final SecureString END_USER_PASSWORD = new SecureString("end-user-password".toCharArray());
    private static final String MANAGE_OWN_API_KEY_USER = "manage_own_api_key_user";
    private static final String MANAGE_API_KEY_USER = "manage_api_key_user";
    private static final String MANAGE_SECURITY_USER = "manage_security_user";

    @Before
    public void createUsers() throws IOException {
        createUser(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD, List.of("manage_own_api_key_role"));
        createRole("manage_own_api_key_role", Set.of("manage_own_api_key"));
        createUser(MANAGE_API_KEY_USER, END_USER_PASSWORD, List.of("manage_api_key_role"));
        createRole("manage_api_key_role", Set.of("manage_api_key"));
        createUser(MANAGE_SECURITY_USER, END_USER_PASSWORD, List.of("manage_security_role"));
        createRole("manage_security_role", Set.of("manage_security"));
    }

    @After
    public void cleanUp() throws IOException {
        deleteUser(MANAGE_OWN_API_KEY_USER);
        deleteUser(MANAGE_API_KEY_USER);
        deleteUser(MANAGE_SECURITY_USER);
        deleteRole("manage_own_api_key_role");
        deleteRole("manage_api_key_role");
        deleteRole("manage_security_role");
        invalidateApiKeysForUser(MANAGE_OWN_API_KEY_USER);
        invalidateApiKeysForUser(MANAGE_API_KEY_USER);
        invalidateApiKeysForUser(MANAGE_SECURITY_USER);
    }

    public void testGetApiKeysWithActiveOnlyFlag() throws Exception {
        final String apiKeyId0 = createApiKey("key-0", MANAGE_OWN_API_KEY_USER);
        final String apiKeyId1 = createApiKey("key-1", MANAGE_OWN_API_KEY_USER);
        // Set short enough expiration for the API key to be expired by the time we query for it
        final String apiKeyId2 = createApiKey("key-2", MANAGE_OWN_API_KEY_USER, TimeValue.timeValueNanos(1));

        {
            final GetApiKeyResponse response = getApiKeysWithRequestParams(Map.of("active_only", "true"));
            assertResponseContainsApiKeyIds(response, apiKeyId0, apiKeyId1);
        }
        {
            final Map<String, String> parameters = new HashMap<>();
            if (randomBoolean()) {
                parameters.put("active_only", "false");
            }
            final GetApiKeyResponse response = getApiKeysWithRequestParams(parameters);
            assertResponseContainsApiKeyIds(response, apiKeyId0, apiKeyId1, apiKeyId2);
        }

        getSecurityClient().invalidateApiKeys(apiKeyId0);

        {
            final GetApiKeyResponse response = getApiKeysWithRequestParams(Map.of("active_only", "true"));
            assertResponseContainsApiKeyIds(response, apiKeyId1);
        }
        {
            final Map<String, String> parameters = new HashMap<>();
            if (randomBoolean()) {
                parameters.put("active_only", "false");
            }
            final GetApiKeyResponse response = getApiKeysWithRequestParams(parameters);
            assertResponseContainsApiKeyIds(response, apiKeyId0, apiKeyId1, apiKeyId2);
        }

        getSecurityClient().invalidateApiKeys(apiKeyId1);

        {
            final GetApiKeyResponse response = getApiKeysWithRequestParams(Map.of("active_only", "true"));
            assertThat(response.getApiKeyInfos(), emptyArray());
        }
    }

    public void testGetApiKeysWithActiveOnlyFlagAndMultipleUsers() throws Exception {
        final String manageOwnApiKeyUserApiKeyId = createApiKey("key-0", MANAGE_OWN_API_KEY_USER);
        final String manageApiKeyUserApiKeyId = createApiKey("key-1", MANAGE_API_KEY_USER);

        // Two active API keys
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()))),
            manageOwnApiKeyUserApiKeyId,
            manageApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()), "username", MANAGE_API_KEY_USER)),
            manageApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()), "username", MANAGE_OWN_API_KEY_USER)),
            manageOwnApiKeyUserApiKeyId
        );

        // One active API key
        invalidateApiKeysForUser(MANAGE_OWN_API_KEY_USER);

        assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(Map.of("active_only", "true")), manageApiKeyUserApiKeyId);
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "username", MANAGE_API_KEY_USER)),
            manageApiKeyUserApiKeyId
        );
        assertThat(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "username", MANAGE_OWN_API_KEY_USER)).getApiKeyInfos(),
            emptyArray()
        );

        // Test with owner=true flag
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "owner", "true")),
            manageApiKeyUserApiKeyId
        );

        // No more active API keys
        invalidateApiKeysForUser(MANAGE_API_KEY_USER);

        assertThat(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "username", randomFrom(MANAGE_API_KEY_USER, MANAGE_OWN_API_KEY_USER)))
                .getApiKeyInfos(),
            emptyArray()
        );
        assertThat(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "owner", Boolean.toString(randomBoolean()))).getApiKeyInfos(),
            emptyArray()
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(randomBoolean() ? Map.of() : Map.of("active_only", "false")),
            manageOwnApiKeyUserApiKeyId,
            manageApiKeyUserApiKeyId
        );
    }

    private GetApiKeyResponse getApiKeysWithRequestParams(Map<String, String> requestParams) throws IOException {
        return getApiKeysWithRequestParams(MANAGE_API_KEY_USER, requestParams);
    }

    private GetApiKeyResponse getApiKeysWithRequestParams(String userOnRequest, Map<String, String> requestParams) throws IOException {
        final var request = new Request(HttpGet.METHOD_NAME, "/_security/api_key/");
        request.addParameters(requestParams);
        setUserForRequest(request, userOnRequest);
        return GetApiKeyResponse.fromXContent(getParser(client().performRequest(request)));
    }

    private static void assertResponseContainsApiKeyIds(GetApiKeyResponse response, String... ids) {
        assertThat(Arrays.stream(response.getApiKeyInfos()).map(ApiKey::getId).collect(Collectors.toList()), containsInAnyOrder(ids));
    }

    private static XContentParser getParser(Response response) throws IOException {
        final byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, responseBody);
    }

    private String createApiKey(String apiKeyName, String creatorUser) throws IOException {
        return createApiKey(apiKeyName, creatorUser, null);
    }

    private String createApiKey(String apiKeyName, String creatorUser, @Nullable TimeValue expiration) throws IOException {
        // Sanity check to ensure API key name and creator name aren't flipped
        assert creatorUser.equals(MANAGE_OWN_API_KEY_USER)
            || creatorUser.equals(MANAGE_API_KEY_USER)
            || creatorUser.equals(MANAGE_SECURITY_USER);

        final Map<String, Object> createApiKeyRequestBody = expiration == null
            ? Map.of("name", apiKeyName)
            : Map.of("name", apiKeyName, "expiration", expiration);
        final var createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString());
        setUserForRequest(createApiKeyRequest, creatorUser);

        final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);

        assertOK(createApiKeyResponse);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse);
        return (String) createApiKeyResponseMap.get("id");
    }

    private void setUserForRequest(Request request, String username) {
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .removeHeader("Authorization")
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(username, END_USER_PASSWORD))
        );
    }
}
