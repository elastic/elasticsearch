/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.apikey;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetApiKeysRestIT extends SecurityOnTrialLicenseRestTestCase {
    private static final SecureString END_USER_PASSWORD = new SecureString("end-user-password".toCharArray());
    private static final String MANAGE_OWN_API_KEY_USER = "manage_own_api_key_user";
    private static final String MANAGE_SECURITY_USER = "manage_security_user";

    @Before
    public void createUsers() throws IOException {
        createUser(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD, List.of("manage_own_api_key_role"));
        createRole("manage_own_api_key_role", Set.of("manage_own_api_key"));
        createUser(MANAGE_SECURITY_USER, END_USER_PASSWORD, List.of("manage_security_role"));
        createRole("manage_security_role", Set.of("manage_security"));
    }

    public void testGetApiKeysWithActiveOnlyFlag() throws Exception {
        final String apiKeyId0 = createApiKey(MANAGE_SECURITY_USER, "key-0");
        final String apiKeyId1 = createApiKey(MANAGE_SECURITY_USER, "key-1");
        // Set short enough expiration for the API key to be expired by the time we query for it
        final String apiKeyId2 = createApiKey(MANAGE_SECURITY_USER, "key-2", TimeValue.timeValueNanos(1));

        // All API keys returned when flag false (implicitly or explicitly)
        {
            final Map<String, String> parameters = new HashMap<>();
            if (randomBoolean()) {
                parameters.put("active_only", "false");
            }
            assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(parameters), apiKeyId0, apiKeyId1, apiKeyId2);
        }

        // Only active keys returned when flag true
        assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(Map.of("active_only", "true")), apiKeyId0, apiKeyId1);
        // Also works with `name` filter
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "name", randomFrom("*", "key-*"))),
            apiKeyId0,
            apiKeyId1
        );
        // Also works with `realm_name` filter
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "realm_name", "default_native")),
            apiKeyId0,
            apiKeyId1
        );

        // Same applies to invalidated key
        getSecurityClient().invalidateApiKeys(apiKeyId0);
        {
            final Map<String, String> parameters = new HashMap<>();
            if (randomBoolean()) {
                parameters.put("active_only", "false");
            }
            assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(parameters), apiKeyId0, apiKeyId1, apiKeyId2);
        }
        assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(Map.of("active_only", "true")), apiKeyId1);
        // also works with name filter
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "name", randomFrom("*", "key-*", "key-1"))),
            apiKeyId1
        );

        // We get an empty result when no API keys active
        getSecurityClient().invalidateApiKeys(apiKeyId1);
        assertThat(getApiKeysWithRequestParams(Map.of("active_only", "true")).getApiKeyInfos(), emptyArray());

        {
            // Using together with id parameter, returns 404 for inactive key
            var ex = expectThrows(
                ResponseException.class,
                () -> getApiKeysWithRequestParams(Map.of("active_only", "true", "id", randomFrom(apiKeyId0, apiKeyId1, apiKeyId2)))
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }

        {
            // manage_own_api_key prohibits owner=false, even if active_only is set
            var ex = expectThrows(
                ResponseException.class,
                () -> getApiKeysWithRequestParams(MANAGE_OWN_API_KEY_USER, Map.of("active_only", "true", "owner", "false"))
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        }
    }

    public void testGetApiKeysWithActiveOnlyFlagAndMultipleUsers() throws Exception {
        final String manageOwnApiKeyUserApiKeyId = createApiKey(MANAGE_OWN_API_KEY_USER, "key-0");
        final String manageApiKeyUserApiKeyId = createApiKey(MANAGE_SECURITY_USER, "key-1");

        // Both users' API keys are returned
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()))),
            manageOwnApiKeyUserApiKeyId,
            manageApiKeyUserApiKeyId
        );
        // Filtering by username works (also via owner flag)
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()), "username", MANAGE_SECURITY_USER)),
            manageApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", Boolean.toString(randomBoolean()), "username", MANAGE_OWN_API_KEY_USER)),
            manageOwnApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(MANAGE_SECURITY_USER, Map.of("active_only", Boolean.toString(randomBoolean()), "owner", "true")),
            manageApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(MANAGE_OWN_API_KEY_USER, Map.of("active_only", Boolean.toString(randomBoolean()), "owner", "true")),
            manageOwnApiKeyUserApiKeyId
        );

        // One user's API key is active
        invalidateApiKeysForUser(MANAGE_OWN_API_KEY_USER);

        // Filtering by username still works (also via owner flag)
        assertResponseContainsApiKeyIds(getApiKeysWithRequestParams(Map.of("active_only", "true")), manageApiKeyUserApiKeyId);
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "username", MANAGE_SECURITY_USER)),
            manageApiKeyUserApiKeyId
        );
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(MANAGE_SECURITY_USER, Map.of("active_only", "true", "owner", "true")),
            manageApiKeyUserApiKeyId
        );
        assertThat(
            getApiKeysWithRequestParams(Map.of("active_only", "true", "username", MANAGE_OWN_API_KEY_USER)).getApiKeyInfos(),
            emptyArray()
        );
        assertThat(
            getApiKeysWithRequestParams(MANAGE_OWN_API_KEY_USER, Map.of("active_only", "true", "owner", "true")).getApiKeyInfos(),
            emptyArray()
        );

        // No more active API keys
        invalidateApiKeysForUser(MANAGE_SECURITY_USER);

        assertThat(
            getApiKeysWithRequestParams(
                Map.of("active_only", "true", "username", randomFrom(MANAGE_SECURITY_USER, MANAGE_OWN_API_KEY_USER))
            ).getApiKeyInfos(),
            emptyArray()
        );
        assertThat(
            getApiKeysWithRequestParams(
                randomFrom(MANAGE_SECURITY_USER, MANAGE_OWN_API_KEY_USER),
                Map.of("active_only", "true", "owner", "true")
            ).getApiKeyInfos(),
            emptyArray()
        );
        // With flag set to false, we get both inactive keys
        assertResponseContainsApiKeyIds(
            getApiKeysWithRequestParams(randomBoolean() ? Map.of() : Map.of("active_only", "false")),
            manageOwnApiKeyUserApiKeyId,
            manageApiKeyUserApiKeyId
        );
    }

    public void testInvalidateApiKey() throws Exception {
        final String apiKeyId0 = createApiKey(MANAGE_SECURITY_USER, "key-2");

        Request request = new Request(HttpGet.METHOD_NAME, "/_security/api_key/");
        setUserForRequest(request, MANAGE_SECURITY_USER);
        GetApiKeyResponse getApiKeyResponse = GetApiKeyResponse.fromXContent(getParser(client().performRequest(request)));

        assertThat(getApiKeyResponse.getApiKeyInfos().length, equalTo(1));
        ApiKey apiKey = getApiKeyResponse.getApiKeyInfos()[0];
        assertThat(apiKey.isInvalidated(), equalTo(false));
        assertThat(apiKey.getInvalidation(), nullValue());
        assertThat(apiKey.getId(), equalTo(apiKeyId0));

        request = new Request(HttpDelete.METHOD_NAME, "/_security/api_key/");
        setUserForRequest(request, MANAGE_SECURITY_USER);
        request.setJsonEntity(XContentTestUtils.convertToXContent(Map.of("ids", List.of(apiKeyId0)), XContentType.JSON).utf8ToString());

        InvalidateApiKeyResponse invalidateApiKeyResponse = InvalidateApiKeyResponse.fromXContent(
            getParser(client().performRequest(request))
        );

        assertThat(invalidateApiKeyResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateApiKeyResponse.getInvalidatedApiKeys().get(0), equalTo(apiKey.getId()));

        request = new Request(HttpGet.METHOD_NAME, "/_security/api_key/");
        setUserForRequest(request, MANAGE_SECURITY_USER);
        getApiKeyResponse = GetApiKeyResponse.fromXContent(getParser(client().performRequest(request)));

        assertThat(getApiKeyResponse.getApiKeyInfos().length, equalTo(1));
        apiKey = getApiKeyResponse.getApiKeyInfos()[0];
        assertThat(apiKey.isInvalidated(), equalTo(true));
        assertThat(apiKey.getInvalidation(), notNullValue());
        assertThat(apiKey.getId(), equalTo(apiKeyId0));
    }

    private GetApiKeyResponse getApiKeysWithRequestParams(Map<String, String> requestParams) throws IOException {
        return getApiKeysWithRequestParams(MANAGE_SECURITY_USER, requestParams);
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

    private String createApiKey(String creatorUser, String apiKeyName) throws IOException {
        return createApiKey(creatorUser, apiKeyName, null);
    }

    /**
     * Returns id of created API key.
     */
    private String createApiKey(String creatorUser, String apiKeyName, @Nullable TimeValue expiration) throws IOException {
        // Sanity check to ensure API key name and creator name aren't flipped
        assert creatorUser.equals(MANAGE_OWN_API_KEY_USER) || creatorUser.equals(MANAGE_SECURITY_USER);

        // Exercise cross cluster keys, if viable (i.e., creator has enough privileges and feature flag is enabled)
        final boolean createCrossClusterKey = creatorUser.equals(MANAGE_SECURITY_USER) && randomBoolean();
        if (createCrossClusterKey) {
            final Map<String, Object> createApiKeyRequestBody = expiration == null
                ? Map.of("name", apiKeyName, "access", Map.of("search", List.of(Map.of("names", List.of("*")))))
                : Map.of("name", apiKeyName, "expiration", expiration, "access", Map.of("search", List.of(Map.of("names", List.of("*")))));
            final var createApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
            createApiKeyRequest.setJsonEntity(
                XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString()
            );
            setUserForRequest(createApiKeyRequest, creatorUser);

            final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);

            assertOK(createApiKeyResponse);
            final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse);
            return (String) createApiKeyResponseMap.get("id");
        } else {
            final Map<String, Object> createApiKeyRequestBody = expiration == null
                ? Map.of("name", apiKeyName)
                : Map.of("name", apiKeyName, "expiration", expiration);
            final var createApiKeyRequest = new Request("POST", "/_security/api_key");
            createApiKeyRequest.setJsonEntity(
                XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString()
            );
            setUserForRequest(createApiKeyRequest, creatorUser);

            final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);

            assertOK(createApiKeyResponse);
            final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse);
            return (String) createApiKeyResponseMap.get("id");
        }
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
