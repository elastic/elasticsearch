/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.apikey;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSourceField.ES_TEST_ROOT_ROLE;
import static org.elasticsearch.test.SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_DESCRIPTOR;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration Rest Tests relating to API Keys.
 * Tested against a trial license
 */
public class ApiKeyRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String SYSTEM_USER = "system_user";
    private static final SecureString SYSTEM_USER_PASSWORD = new SecureString("system-user-password".toCharArray());
    private static final String END_USER = "end_user";
    private static final SecureString END_USER_PASSWORD = new SecureString("end-user-password".toCharArray());
    private static final String MANAGE_OWN_API_KEY_USER = "manage_own_api_key_user";
    private static final String REMOTE_INDICES_USER = "remote_indices_user";
    private static final String MANAGE_API_KEY_USER = "manage_api_key_user";
    private static final String MANAGE_SECURITY_USER = "manage_security_user";

    @Before
    public void createUsers() throws IOException {
        createUser(SYSTEM_USER, SYSTEM_USER_PASSWORD, List.of("system_role"));
        createRole("system_role", Set.of("grant_api_key"));
        createUser(END_USER, END_USER_PASSWORD, List.of("user_role"));
        createRole("user_role", Set.of("monitor"));
        createUser(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD, List.of("manage_own_api_key_role"));
        createRole("manage_own_api_key_role", Set.of("manage_own_api_key"));
        createUser(MANAGE_API_KEY_USER, END_USER_PASSWORD, List.of("manage_api_key_role"));
        createRole("manage_api_key_role", Set.of("manage_api_key"));
        createUser(MANAGE_SECURITY_USER, END_USER_PASSWORD, List.of("manage_security_role"));
        createRole("manage_security_role", Set.of("manage_security"));
    }

    @After
    public void cleanUp() throws IOException {
        deleteUser(SYSTEM_USER);
        deleteUser(END_USER);
        deleteUser(MANAGE_OWN_API_KEY_USER);
        deleteUser(MANAGE_API_KEY_USER);
        deleteUser(MANAGE_SECURITY_USER);
        deleteRole("system_role");
        deleteRole("user_role");
        deleteRole("manage_own_api_key_role");
        deleteRole("manage_api_key_role");
        deleteRole("manage_security_role");
        invalidateApiKeysForUser(END_USER);
        invalidateApiKeysForUser(MANAGE_OWN_API_KEY_USER);
        invalidateApiKeysForUser(MANAGE_API_KEY_USER);
        invalidateApiKeysForUser(MANAGE_SECURITY_USER);
    }

    @SuppressWarnings("unchecked")
    public void testGetApiKeyRoleDescriptors() throws IOException {
        // First key without assigned role descriptors, i.e. it inherits owner user's permission
        // This can be achieved by either omitting the role_descriptors field in the request or
        // explicitly set it to an empty object
        final Request createApiKeyRequest1 = new Request("POST", "_security/api_key");
        if (randomBoolean()) {
            createApiKeyRequest1.setJsonEntity("""
                {
                  "name": "k1"
                }""");
        } else {
            createApiKeyRequest1.setJsonEntity("""
                {
                  "name": "k1",
                  "role_descriptors": { }
                }""");
        }
        assertOK(adminClient().performRequest(createApiKeyRequest1));

        // Second key with a single assigned role descriptor
        final Request createApiKeyRequest2 = new Request("POST", "_security/api_key");
        createApiKeyRequest2.setJsonEntity("""
            {
              "name": "k2",
                "role_descriptors": {
                  "x": {
                    "cluster": [
                      "monitor"
                    ]
                  }
                }
            }""");
        assertOK(adminClient().performRequest(createApiKeyRequest2));

        // Third key with two assigned role descriptors
        final Request createApiKeyRequest3 = new Request("POST", "_security/api_key");
        createApiKeyRequest3.setJsonEntity("""
            {
              "name": "k3",
                "role_descriptors": {
                  "x": {
                    "cluster": [
                      "monitor"
                    ]
                  },
                  "y": {
                    "indices": [
                      {
                        "names": [
                          "index"
                        ],
                        "privileges": [
                          "read"
                        ]
                      }
                    ]
                  }
                }
            }""");
        assertOK(adminClient().performRequest(createApiKeyRequest3));

        // Role descriptors are returned by both get and query api key calls
        final boolean withLimitedBy = randomBoolean();
        final List<Map<String, Object>> apiKeyMaps;
        if (randomBoolean()) {
            final Request getApiKeyRequest = new Request("GET", "_security/api_key");
            if (withLimitedBy) {
                getApiKeyRequest.addParameter("with_limited_by", "true");
            } else if (randomBoolean()) {
                getApiKeyRequest.addParameter("with_limited_by", "false");
            }
            final Response getApiKeyResponse = adminClient().performRequest(getApiKeyRequest);
            assertOK(getApiKeyResponse);
            apiKeyMaps = (List<Map<String, Object>>) responseAsMap(getApiKeyResponse).get("api_keys");
        } else {
            final Request queryApiKeyRequest = new Request("POST", "_security/_query/api_key");
            if (withLimitedBy) {
                queryApiKeyRequest.addParameter("with_limited_by", "true");
            } else if (randomBoolean()) {
                queryApiKeyRequest.addParameter("with_limited_by", "false");
            }
            final Response queryApiKeyResponse = adminClient().performRequest(queryApiKeyRequest);
            assertOK(queryApiKeyResponse);
            apiKeyMaps = (List<Map<String, Object>>) responseAsMap(queryApiKeyResponse).get("api_keys");
        }
        assertThat(apiKeyMaps.size(), equalTo(3));

        for (Map<String, Object> apiKeyMap : apiKeyMaps) {
            final String name = (String) apiKeyMap.get("name");
            @SuppressWarnings("unchecked")
            final var roleDescriptors = (Map<String, Object>) apiKeyMap.get("role_descriptors");

            if (withLimitedBy) {
                final List<Map<String, Object>> limitedBy = (List<Map<String, Object>>) apiKeyMap.get("limited_by");
                assertThat(limitedBy.size(), equalTo(1));
                assertThat(
                    limitedBy.get(0),
                    equalTo(Map.of(ES_TEST_ROOT_ROLE, XContentTestUtils.convertToMap(ES_TEST_ROOT_ROLE_DESCRIPTOR)))
                );
            } else {
                assertThat(apiKeyMap, not(hasKey("limited_by")));
            }

            switch (name) {
                case "k1" -> {
                    assertThat(roleDescriptors, anEmptyMap());
                }
                case "k2" -> {
                    assertThat(
                        roleDescriptors,
                        equalTo(
                            Map.of("x", XContentTestUtils.convertToMap(new RoleDescriptor("x", new String[] { "monitor" }, null, null)))
                        )
                    );
                }
                case "k3" -> {
                    assertThat(
                        roleDescriptors,
                        equalTo(
                            Map.of(
                                "x",
                                XContentTestUtils.convertToMap(new RoleDescriptor("x", new String[] { "monitor" }, null, null)),
                                "y",
                                XContentTestUtils.convertToMap(
                                    new RoleDescriptor(
                                        "y",
                                        null,
                                        new RoleDescriptor.IndicesPrivileges[] {
                                            RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("read").build() },
                                        null
                                    )
                                )
                            )
                        )
                    );
                }
                default -> throw new IllegalStateException("unknown api key name [" + name + "]");
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    public void testAuthenticateResponseApiKey() throws IOException {
        final String expectedApiKeyName = "my-api-key-name";
        final Map<String, String> expectedApiKeyMetadata = Map.of("not", "returned");
        final Map<String, Object> createApiKeyRequestBody = Map.of("name", expectedApiKeyName, "metadata", expectedApiKeyMetadata);

        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString());

        final Response createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse); // keys: id, name, api_key, encoded
        final String actualApiKeyId = (String) createApiKeyResponseMap.get("id");
        final String actualApiKeyName = (String) createApiKeyResponseMap.get("name");
        final String actualApiKeyEncoded = (String) createApiKeyResponseMap.get("encoded"); // Base64(id:api_key)
        assertThat(actualApiKeyId, not(emptyString()));
        assertThat(actualApiKeyName, equalTo(expectedApiKeyName));
        assertThat(actualApiKeyEncoded, not(emptyString()));

        doTestAuthenticationWithApiKey(expectedApiKeyName, actualApiKeyId, actualApiKeyEncoded);
    }

    public void testGrantApiKeyForOtherUserWithPassword() throws IOException {
        Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SYSTEM_USER, SYSTEM_USER_PASSWORD))
        );
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", END_USER),
            Map.entry("password", END_USER_PASSWORD.toString()),
            Map.entry("api_key", Map.of("name", "test_api_key_password"))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final Response response = client().performRequest(request);
        final Map<String, Object> responseBody = entityAsMap(response);

        assertThat(responseBody.get("name"), equalTo("test_api_key_password"));
        assertThat(responseBody.get("id"), notNullValue());
        assertThat(responseBody.get("id"), instanceOf(String.class));

        ApiKey apiKey = getApiKey((String) responseBody.get("id"));
        assertThat(apiKey.getUsername(), equalTo(END_USER));
    }

    public void testGrantApiKeyForOtherUserWithAccessToken() throws IOException {
        final Tuple<String, String> token = super.createOAuthToken(END_USER, END_USER_PASSWORD);
        final String accessToken = token.v1();

        final Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SYSTEM_USER, SYSTEM_USER_PASSWORD))
        );
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "access_token"),
            Map.entry("access_token", accessToken),
            Map.entry("api_key", Map.of("name", "test_api_key_token", "expiration", "2h"))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final Instant before = Instant.now();
        final Response response = client().performRequest(request);
        final Instant after = Instant.now();
        final Map<String, Object> responseBody = entityAsMap(response);

        assertThat(responseBody.get("name"), equalTo("test_api_key_token"));
        assertThat(responseBody.get("id"), notNullValue());
        assertThat(responseBody.get("id"), instanceOf(String.class));

        ApiKey apiKey = getApiKey((String) responseBody.get("id"));
        assertThat(apiKey.getUsername(), equalTo(END_USER));

        Instant minExpiry = before.plus(2, ChronoUnit.HOURS);
        Instant maxExpiry = after.plus(2, ChronoUnit.HOURS);
        assertThat(apiKey.getExpiration(), notNullValue());
        assertThat(apiKey.getExpiration(), greaterThanOrEqualTo(minExpiry));
        assertThat(apiKey.getExpiration(), lessThanOrEqualTo(maxExpiry));
    }

    public void testGrantApiKeyWithoutApiKeyNameWillFail() throws IOException {
        Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SYSTEM_USER, SYSTEM_USER_PASSWORD))
        );
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", END_USER),
            Map.entry("password", END_USER_PASSWORD.toString())
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("api key name is required"));
    }

    public void testGrantApiKeyWithOnlyManageOwnApiKeyPrivilegeFails() throws IOException {
        final Request request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD))
        );
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", MANAGE_OWN_API_KEY_USER),
            Map.entry("password", END_USER_PASSWORD.toString()),
            Map.entry("api_key", Map.of("name", "test_api_key_password"))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [" + GrantApiKeyAction.NAME + "] is unauthorized for user"));
    }

    public void testUpdateApiKey() throws IOException {
        final var apiKeyName = "my-api-key-name";
        final Map<String, Object> apiKeyMetadata = Map.of("not", "returned");
        final Map<String, Object> createApiKeyRequestBody = Map.of("name", apiKeyName, "metadata", apiKeyMetadata);

        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString());
        createApiKeyRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", headerFromRandomAuthMethod(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD))
        );

        final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse); // keys: id, name, api_key, encoded
        final var apiKeyId = (String) createApiKeyResponseMap.get("id");
        final var apiKeyEncoded = (String) createApiKeyResponseMap.get("encoded"); // Base64(id:api_key)
        assertThat(apiKeyId, not(emptyString()));
        assertThat(apiKeyEncoded, not(emptyString()));

        doTestUpdateApiKey(apiKeyName, apiKeyId, apiKeyEncoded, apiKeyMetadata);
    }

    @SuppressWarnings({ "unchecked" })
    public void testBulkUpdateApiKey() throws IOException {
        final EncodedApiKey apiKeyExpectingUpdate = createApiKey("my-api-key-name-1", Map.of("not", "returned"));
        final EncodedApiKey apiKeyExpectingNoop = createApiKey("my-api-key-name-2", Map.of("not", "returned (changed)", "foo", "bar"));
        final Map<String, Object> metadataForInvalidatedKey = Map.of("will not be updated", true);
        final EncodedApiKey invalidatedApiKey = createApiKey("my-api-key-name-3", metadataForInvalidatedKey);
        getSecurityClient().invalidateApiKeys(invalidatedApiKey.id);
        final var notFoundApiKeyId = "not-found-api-key-id";
        final List<String> idsToUpdate = shuffledList(
            List.of(apiKeyExpectingUpdate.id, apiKeyExpectingNoop.id, notFoundApiKeyId, invalidatedApiKey.id)
        );
        final var bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        final Map<String, Object> expectedApiKeyMetadata = Map.of("not", "returned (changed)", "foo", "bar");
        final Map<String, Object> updateApiKeyRequestBody = Map.of("ids", idsToUpdate, "metadata", expectedApiKeyMetadata);
        bulkUpdateApiKeyRequest.setJsonEntity(
            XContentTestUtils.convertToXContent(updateApiKeyRequestBody, XContentType.JSON).utf8ToString()
        );

        final Response bulkUpdateApiKeyResponse = performRequestUsingRandomAuthMethod(bulkUpdateApiKeyRequest);

        assertOK(bulkUpdateApiKeyResponse);
        final Map<String, Object> response = responseAsMap(bulkUpdateApiKeyResponse);
        assertEquals(List.of(apiKeyExpectingUpdate.id()), response.get("updated"));
        assertEquals(List.of(apiKeyExpectingNoop.id()), response.get("noops"));
        final Map<String, Object> errors = (Map<String, Object>) response.get("errors");
        assertEquals(2, errors.get("count"));
        final Map<String, Map<String, Object>> errorDetails = (Map<String, Map<String, Object>>) errors.get("details");
        assertEquals(2, errorDetails.size());
        expectErrorFields(
            "resource_not_found_exception",
            "no API key owned by requesting user found for ID [" + notFoundApiKeyId + "]",
            errorDetails.get(notFoundApiKeyId)
        );
        expectErrorFields(
            "illegal_argument_exception",
            "cannot update invalidated API key [" + invalidatedApiKey.id + "]",
            errorDetails.get(invalidatedApiKey.id)
        );
        expectMetadata(apiKeyExpectingUpdate.id, expectedApiKeyMetadata);
        expectMetadata(apiKeyExpectingNoop.id, expectedApiKeyMetadata);
        expectMetadata(invalidatedApiKey.id, metadataForInvalidatedKey);
        doTestAuthenticationWithApiKey(apiKeyExpectingUpdate.name, apiKeyExpectingUpdate.id, apiKeyExpectingUpdate.encoded);
        doTestAuthenticationWithApiKey(apiKeyExpectingNoop.name, apiKeyExpectingNoop.id, apiKeyExpectingNoop.encoded);
    }

    public void testGrantTargetCanUpdateApiKey() throws IOException {
        final var request = new Request("POST", "_security/api_key/grant");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SYSTEM_USER, SYSTEM_USER_PASSWORD))
        );
        final var apiKeyName = "test_api_key_password";
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", MANAGE_OWN_API_KEY_USER),
            Map.entry("password", END_USER_PASSWORD.toString()),
            Map.entry("api_key", Map.of("name", apiKeyName))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());

        final Response response = client().performRequest(request);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(response); // keys: id, name, api_key, encoded
        final var apiKeyId = (String) createApiKeyResponseMap.get("id");
        final var apiKeyEncoded = (String) createApiKeyResponseMap.get("encoded"); // Base64(id:api_key)
        assertThat(apiKeyId, not(emptyString()));
        assertThat(apiKeyEncoded, not(emptyString()));

        if (randomBoolean()) {
            doTestUpdateApiKey(apiKeyName, apiKeyId, apiKeyEncoded, null);
        } else {
            doTestUpdateApiKeyUsingBulkAction(apiKeyName, apiKeyId, apiKeyEncoded, null);
        }
    }

    @SuppressWarnings({ "unchecked" })
    public void testGrantorCannotUpdateApiKeyOfGrantTarget() throws IOException {
        final var request = new Request("POST", "_security/api_key/grant");
        final var apiKeyName = "test_api_key_password";
        final Map<String, Object> requestBody = Map.ofEntries(
            Map.entry("grant_type", "password"),
            Map.entry("username", MANAGE_OWN_API_KEY_USER),
            Map.entry("password", END_USER_PASSWORD.toString()),
            Map.entry("api_key", Map.of("name", apiKeyName))
        );
        request.setJsonEntity(XContentTestUtils.convertToXContent(requestBody, XContentType.JSON).utf8ToString());
        final Response response = adminClient().performRequest(request);

        final Map<String, Object> createApiKeyResponseMap = responseAsMap(response); // keys: id, name, api_key, encoded
        final var apiKeyId = (String) createApiKeyResponseMap.get("id");
        final var apiKeyEncoded = (String) createApiKeyResponseMap.get("encoded"); // Base64(id:api_key)
        assertThat(apiKeyId, not(emptyString()));
        assertThat(apiKeyEncoded, not(emptyString()));

        final var updateApiKeyRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        updateApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(Map.of(), XContentType.JSON).utf8ToString());

        final ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(updateApiKeyRequest));

        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("no API key owned by requesting user found for ID [" + apiKeyId + "]"));

        // Bulk update also not allowed
        final var bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        bulkUpdateApiKeyRequest.setJsonEntity(
            XContentTestUtils.convertToXContent(Map.of("ids", List.of(apiKeyId)), XContentType.JSON).utf8ToString()
        );
        final Response bulkUpdateApiKeyResponse = adminClient().performRequest(bulkUpdateApiKeyRequest);

        assertOK(bulkUpdateApiKeyResponse);
        final Map<String, Object> bulkUpdateApiKeyResponseMap = responseAsMap(bulkUpdateApiKeyResponse);
        assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("updated"), empty());
        assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("noops"), empty());
        final Map<String, Object> errors = (Map<String, Object>) bulkUpdateApiKeyResponseMap.get("errors");
        assertEquals(1, errors.get("count"));
        final Map<String, Map<String, Object>> errorDetails = (Map<String, Map<String, Object>>) errors.get("details");
        assertEquals(1, errorDetails.size());
        expectErrorFields(
            "resource_not_found_exception",
            "no API key owned by requesting user found for ID [" + apiKeyId + "]",
            errorDetails.get(apiKeyId)
        );
    }

    public void testGetPrivilegesForApiKeyWorksIfItDoesNotHaveAssignedPrivileges() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        if (randomBoolean()) {
            createApiKeyRequest.setJsonEntity("""
                { "name": "k1" }""");
        } else {
            createApiKeyRequest.setJsonEntity("""
                {
                  "name": "k1",
                  "role_descriptors": { }
                }""");
        }
        final Response createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        assertOK(createApiKeyResponse);

        final Request getPrivilegesRequest = new Request("GET", "_security/user/_privileges");
        getPrivilegesRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + responseAsMap(createApiKeyResponse).get("encoded"))
        );
        final Response getPrivilegesResponse = client().performRequest(getPrivilegesRequest);
        assertOK(getPrivilegesResponse);

        assertThat(responseAsMap(getPrivilegesResponse), equalTo(XContentHelper.convertToMap(JsonXContent.jsonXContent, """
            {
              "cluster": [
                "all"
              ],
              "global": [],
              "indices": [
                {
                  "names": [
                    "*"
                  ],
                  "privileges": [
                    "all"
                  ],
                  "allow_restricted_indices": true
                }
              ],
              "applications": [
                {
                  "application": "*",
                  "privileges": [
                    "*"
                  ],
                  "resources": [
                    "*"
                  ]
                }
              ],
              "run_as": [
                "*"
              ]
            }""", false)));
    }

    public void testGetPrivilegesForApiKeyThrows400IfItHasAssignedPrivileges() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "k1",
              "role_descriptors": { "a": { "cluster": ["monitor"] } }
            }""");
        final Response createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        assertOK(createApiKeyResponse);

        final Request getPrivilegesRequest = new Request("GET", "_security/user/_privileges");
        getPrivilegesRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + responseAsMap(createApiKeyResponse).get("encoded"))
        );
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getPrivilegesRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            e.getMessage(),
            containsString(
                "Cannot retrieve privileges for API keys with assigned role descriptors. "
                    + "Please use the Get API key information API https://ela.st/es-api-get-api-key"
            )
        );
    }

    public void testRemoteIndicesSupportForApiKeys() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        createUser(REMOTE_INDICES_USER, END_USER_PASSWORD, List.of("remote_indices_role"));
        createRole("remote_indices_role", Set.of("grant_api_key", "manage_own_api_key"), "remote");
        final String remoteIndicesSection = """
            "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"]
                }
            ]""";

        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        final boolean includeRemoteIndices = randomBoolean();
        createApiKeyRequest.setJsonEntity(Strings.format("""
            {"name": "k1", "role_descriptors": {"r1": {%s}}}""", includeRemoteIndices ? remoteIndicesSection : ""));
        Response response = sendRequestWithRemoteIndices(createApiKeyRequest, false == includeRemoteIndices);
        String apiKeyId = ObjectPath.createFromResponse(response).evaluate("id");
        assertThat(apiKeyId, notNullValue());
        assertOK(response);

        final Request grantApiKeyRequest = new Request("POST", "_security/api_key/grant");
        grantApiKeyRequest.setJsonEntity(Strings.format("""
            {
               "grant_type":"password",
               "username":"%s",
               "password":"end-user-password",
               "api_key":{
                  "name":"k1",
                  "role_descriptors":{
                     "r1":{
                        %s
                     }
                  }
               }
            }""", includeRemoteIndices ? MANAGE_OWN_API_KEY_USER : REMOTE_INDICES_USER, includeRemoteIndices ? remoteIndicesSection : ""));
        response = sendRequestWithRemoteIndices(grantApiKeyRequest, false == includeRemoteIndices);

        final String updatedRemoteIndicesSection = """
            "remote_indices": [
                {
                  "names": ["index-b", "index-a"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "remote-b"]
                }
            ]""";
        final Request updateApiKeyRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        updateApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "role_descriptors": {
                "r1": {
                  %s
                }
              }
            }""", includeRemoteIndices ? updatedRemoteIndicesSection : ""));
        response = sendRequestWithRemoteIndices(updateApiKeyRequest, false == includeRemoteIndices);
        assertThat(ObjectPath.createFromResponse(response).evaluate("updated"), equalTo(includeRemoteIndices));

        final String bulkUpdatedRemoteIndicesSection = """
            "remote_indices": [
                {
                  "names": ["index-c"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "remote-c"]
                }
            ]""";
        final Request bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        bulkUpdateApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "ids": ["%s"],
              "role_descriptors": {
                "r1": {
                  %s
                }
              }
            }""", apiKeyId, includeRemoteIndices ? bulkUpdatedRemoteIndicesSection : ""));
        response = sendRequestWithRemoteIndices(bulkUpdateApiKeyRequest, false == includeRemoteIndices);
        if (includeRemoteIndices) {
            assertThat(ObjectPath.createFromResponse(response).evaluate("updated"), contains(apiKeyId));
        } else {
            assertThat(ObjectPath.createFromResponse(response).evaluate("noops"), contains(apiKeyId));
        }

        deleteUser(REMOTE_INDICES_USER);
        deleteRole("remote_indices_role");

    }

    public void testCreateCrossClusterApiKey() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final Request createRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createRequest.setJsonEntity("""
            {
              "name": "my-key",
              "access": {
                "search": [
                  {
                    "names": [ "metrics" ],
                    "query": "{\\"term\\":{\\"score\\":42}}"
                  }
                ],
                "replication": [
                  {
                    "names": [ "logs" ],
                    "allow_restricted_indices": true
                  }
                ]
              },
              "expiration": "7d",
              "metadata": { "tag": "shared", "points": 0 }
            }""");
        setUserForRequest(createRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);

        final ObjectPath createResponse = assertOKAndCreateObjectPath(client().performRequest(createRequest));
        final String apiKeyId = createResponse.evaluate("id");

        // Cross cluster API key cannot be used on the REST interface
        final Request authenticateRequest1 = new Request("GET", "/_security/_authenticate");
        authenticateRequest1.setOptions(
            authenticateRequest1.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + createResponse.evaluate("encoded"))
        );
        final ResponseException authenticateError1 = expectThrows(
            ResponseException.class,
            () -> client().performRequest(authenticateRequest1)
        );
        assertThat(authenticateError1.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        assertThat(
            authenticateError1.getMessage(),
            containsString("authentication expected API key type of [rest], but API key [" + apiKeyId + "] has type [cross_cluster]")
        );

        // Not allowed as secondary authentication on the REST interface either
        final Request authenticateRequest2 = new Request("GET", "/_security/_authenticate");
        setUserForRequest(authenticateRequest2, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        authenticateRequest2.setOptions(
            authenticateRequest2.getOptions()
                .toBuilder()
                .addHeader("es-secondary-authorization", "ApiKey " + createResponse.evaluate("encoded"))
        );
        final ResponseException authenticateError2 = expectThrows(
            ResponseException.class,
            () -> client().performRequest(authenticateRequest2)
        );
        assertThat(authenticateError2.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        assertThat(
            authenticateError2.getMessage(),
            containsString("authentication expected API key type of [rest], but API key [" + apiKeyId + "] has type [cross_cluster]")
        );

        final ObjectPath fetchResponse = fetchCrossClusterApiKeyById(apiKeyId);
        assertThat(
            fetchResponse.evaluate("api_keys.0.role_descriptors"),
            equalTo(
                Map.of(
                    "cross_cluster",
                    XContentTestUtils.convertToMap(
                        new RoleDescriptor(
                            "cross_cluster",
                            new String[] { "cross_cluster_search", "cross_cluster_replication" },
                            new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder()
                                    .indices("metrics")
                                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                                    .query("{\"term\":{\"score\":42}}")
                                    .build(),
                                RoleDescriptor.IndicesPrivileges.builder()
                                    .indices("logs")
                                    .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                                    .allowRestrictedIndices(true)
                                    .build() },
                            null
                        )
                    )
                )
            )
        );
        assertThat(fetchResponse.evaluate("api_keys.0.limited_by"), nullValue());

        final Request deleteRequest = new Request("DELETE", "/_security/api_key");
        deleteRequest.setJsonEntity(Strings.format("""
            {"ids": ["%s"]}""", apiKeyId));
        if (randomBoolean()) {
            setUserForRequest(deleteRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        } else {
            setUserForRequest(deleteRequest, MANAGE_API_KEY_USER, END_USER_PASSWORD);
        }
        final ObjectPath deleteResponse = assertOKAndCreateObjectPath(client().performRequest(deleteRequest));
        assertThat(deleteResponse.evaluate("invalidated_api_keys"), equalTo(List.of(apiKeyId)));

        // Cannot create cross-cluster API keys with either manage_api_key or manage_own_api_key privilege
        if (randomBoolean()) {
            setUserForRequest(createRequest, MANAGE_API_KEY_USER, END_USER_PASSWORD);
        } else {
            setUserForRequest(createRequest, MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD);
        }
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(createRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e.getMessage(), containsString("action [cluster:admin/xpack/security/cross_cluster/api_key/create] is unauthorized"));
    }

    public void testCannotCreateDerivedCrossClusterApiKey() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final Request createRestApiKeyRequest = new Request("POST", "_security/api_key");
        setUserForRequest(createRestApiKeyRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        createRestApiKeyRequest.setJsonEntity("{\"name\":\"rest-key\"}");
        final ObjectPath createRestApiKeyResponse = assertOKAndCreateObjectPath(client().performRequest(createRestApiKeyRequest));

        final Request createDerivedRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createDerivedRequest.setJsonEntity("""
            {
              "name": "derived-cross-cluster-key",
              "access": {
                "replication": [
                  {
                    "names": [ "logs" ]
                  }
                ]
              }
            }""");
        createDerivedRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + createRestApiKeyResponse.evaluate("encoded"))
        );
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(createDerivedRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            e.getMessage(),
            containsString("authentication via API key not supported: An API key cannot be used to create a cross-cluster API key")
        );
    }

    public void testCrossClusterApiKeyDoesNotAllowEmptyAccess() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        assertBadCreateCrossClusterApiKeyRequest("""
            {"name": "my-key"}""", "Required [access]");

        assertBadCreateCrossClusterApiKeyRequest("""
            {"name": "my-key", "access": null}""", "access doesn't support values of type: VALUE_NULL");

        assertBadCreateCrossClusterApiKeyRequest("""
            {"name": "my-key", "access": {}}}""", "must specify non-empty access for either [search] or [replication]");

        assertBadCreateCrossClusterApiKeyRequest("""
            {"name": "my-key", "access": {"search":[]}}}""", "must specify non-empty access for either [search] or [replication]");

        assertBadCreateCrossClusterApiKeyRequest("""
            {"name": "my-key", "access": {"replication":[]}}}""", "must specify non-empty access for either [search] or [replication]");

        assertBadCreateCrossClusterApiKeyRequest(
            """
                {"name": "my-key", "access": {"search":[],"replication":[]}}}""",
            "must specify non-empty access for either [search] or [replication]"
        );
    }

    public void testCrossClusterApiKeyDoesNotAllowDlsFlsForReplication() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        assertBadCreateCrossClusterApiKeyRequest("""
            {
              "name": "key",
              "access": {
                "replication": [ {"names": ["logs"], "query":{"term": {"tag": 42}}} ]
              }
            }""", "replication does not support document or field level security");

        assertBadCreateCrossClusterApiKeyRequest("""
            {
              "name": "key",
              "access": {
                "replication": [ {"names": ["logs"], "field_security": {"grant": ["*"], "except": ["private"]}} ]
              }
            }""", "replication does not support document or field level security");

        assertBadCreateCrossClusterApiKeyRequest("""
            {
              "name": "key",
              "access": {
                "replication": [ {
                  "names": ["logs"],
                  "query": {"term": {"tag": 42}},
                  "field_security": {"grant": ["*"], "except": ["private"]}
                 } ]
              }
            }""", "replication does not support document or field level security");
    }

    public void testCrossClusterApiKeyRequiresName() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        assertBadCreateCrossClusterApiKeyRequest("""
            {
              "access": {
                "search": [ {"names": ["logs"]} ]
              }
            }""", "Required [name]");
    }

    public void testUpdateCrossClusterApiKey() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
        final Request createRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createRequest.setJsonEntity("""
            {
              "name": "cross-cluster-key",
              "access": {
                "search": [
                  {
                    "names": [ "metrics" ]
                  }
                ]
              }
            }""");
        setUserForRequest(createRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        final ObjectPath createResponse = assertOKAndCreateObjectPath(client().performRequest(createRequest));
        final String apiKeyId = createResponse.evaluate("id");

        // Update both access and metadata
        final Request updateRequest1 = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
        updateRequest1.setJsonEntity("""
            {
              "access": {
                "search": [
                  {
                    "names": [ "data" ],
                    "query": "{\\"term\\":{\\"score\\":42}}"
                  }
                ],
                "replication": [
                  {
                    "names": [ "logs" ]
                  }
                ]
              },
              "metadata": { "tag": "shared", "points": 0 }
            }""");
        setUserForRequest(updateRequest1, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        final ObjectPath updateResponse1 = assertOKAndCreateObjectPath(client().performRequest(updateRequest1));
        assertThat(updateResponse1.evaluate("updated"), is(true));
        final RoleDescriptor updatedRoleDescriptor1 = new RoleDescriptor(
            "cross_cluster",
            new String[] { "cross_cluster_search", "cross_cluster_replication" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("data")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .query("{\"term\":{\"score\":42}}")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs")
                    .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                    .build() },
            null
        );

        final ObjectPath fetchResponse1 = fetchCrossClusterApiKeyById(apiKeyId);
        assertThat(
            fetchResponse1.evaluate("api_keys.0.role_descriptors"),
            equalTo(Map.of("cross_cluster", XContentTestUtils.convertToMap(updatedRoleDescriptor1)))
        );
        assertThat(fetchResponse1.evaluate("api_keys.0.metadata"), equalTo(Map.of("tag", "shared", "points", 0)));

        // Update metadata only
        final Request updateRequest2 = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
        setUserForRequest(updateRequest2, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateRequest2.setJsonEntity("""
            {
              "metadata": { "env": "prod", "magic": 42 }
            }""");
        final ObjectPath updateResponse2 = assertOKAndCreateObjectPath(client().performRequest(updateRequest2));
        assertThat(updateResponse2.evaluate("updated"), is(true));
        final ObjectPath fetchResponse2 = fetchCrossClusterApiKeyById(apiKeyId);
        assertThat(
            fetchResponse2.evaluate("api_keys.0.role_descriptors"),
            equalTo(Map.of("cross_cluster", XContentTestUtils.convertToMap(updatedRoleDescriptor1)))
        );
        assertThat(fetchResponse2.evaluate("api_keys.0.metadata"), equalTo(Map.of("env", "prod", "magic", 42)));

        // Update access only
        final Request updateRequest3 = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
        setUserForRequest(updateRequest3, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateRequest3.setJsonEntity("""
            {
              "access": {
                "search": [
                  {
                    "names": [ "blogs" ]
                  }
                ]
              }
            }""");
        final ObjectPath updateResponse3 = assertOKAndCreateObjectPath(client().performRequest(updateRequest3));
        assertThat(updateResponse3.evaluate("updated"), is(true));
        final ObjectPath fetchResponse3 = fetchCrossClusterApiKeyById(apiKeyId);
        final RoleDescriptor updatedRoleDescriptors2 = new RoleDescriptor(
            "cross_cluster",
            new String[] { "cross_cluster_search" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("blogs")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build() },
            null
        );
        assertThat(
            fetchResponse3.evaluate("api_keys.0.role_descriptors"),
            equalTo(Map.of("cross_cluster", XContentTestUtils.convertToMap(updatedRoleDescriptors2)))
        );
        assertThat(fetchResponse3.evaluate("api_keys.0.metadata"), equalTo(Map.of("env", "prod", "magic", 42)));

        // Noop update
        final Request updateRequest4 = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
        setUserForRequest(updateRequest4, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateRequest4.setJsonEntity(randomFrom("""
            {
              "access": {
                "search": [
                  {
                    "names": [ "blogs" ]
                  }
                ]
              }
            }""", """
            {
              "metadata": { "env": "prod", "magic": 42 }
            }""", """
            {
              "access": {
                "search": [
                  {
                    "names": [ "blogs" ]
                  }
                ]
              },
              "metadata": { "env": "prod", "magic": 42 }
            }"""));
        final ObjectPath updateResponse4 = assertOKAndCreateObjectPath(client().performRequest(updateRequest4));
        assertThat(updateResponse4.evaluate("updated"), is(false));
        final ObjectPath fetchResponse4 = fetchCrossClusterApiKeyById(apiKeyId);
        assertThat(
            fetchResponse4.evaluate("api_keys.0.role_descriptors"),
            equalTo(Map.of("cross_cluster", XContentTestUtils.convertToMap(updatedRoleDescriptors2)))
        );
        assertThat(fetchResponse4.evaluate("api_keys.0.metadata"), equalTo(Map.of("env", "prod", "magic", 42)));
    }

    public void testUpdateFailureCases() throws IOException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
        final Request createRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createRequest.setJsonEntity("""
            {
              "name": "cross-cluster-key",
              "access": {
                "search": [
                  {
                    "names": [ "metrics" ]
                  }
                ]
              }
            }""");
        setUserForRequest(createRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        final ObjectPath createResponse = assertOKAndCreateObjectPath(client().performRequest(createRequest));
        final String apiKeyId = createResponse.evaluate("id");

        final Request updateRequest = new Request("PUT", "/_security/cross_cluster/api_key/" + apiKeyId);
        setUserForRequest(updateRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);

        // Request body is required
        final ResponseException e1 = expectThrows(ResponseException.class, () -> client().performRequest(updateRequest));
        assertThat(e1.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e1.getMessage(), containsString("request body is required"));

        // Must update either access or metadata
        updateRequest.setJsonEntity("{}");
        final ResponseException e2 = expectThrows(ResponseException.class, () -> client().performRequest(updateRequest));
        assertThat(e2.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e2.getMessage(), containsString("must update either [access] or [metadata] for cross-cluster API keys"));

        // Access cannot be empty
        updateRequest.setJsonEntity("{\"access\":{}}");
        final ResponseException e3 = expectThrows(ResponseException.class, () -> client().performRequest(updateRequest));
        assertThat(e3.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e3.getMessage(), containsString("must specify non-empty access for either [search] or [replication]"));

        // Cannot update with API for REST API keys
        final Request updateWithRestApi = new Request("PUT", "/_security/api_key/" + apiKeyId);
        setUserForRequest(updateWithRestApi, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateWithRestApi.setJsonEntity("{\"metadata\":{}}");
        final ResponseException e4 = expectThrows(ResponseException.class, () -> client().performRequest(updateWithRestApi));
        assertThat(e4.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e4.getMessage(), containsString("cannot update API key of type [cross_cluster] while expected type is [rest]"));

        final Request updateWithBulkRestApi = new Request("POST", "/_security/api_key/_bulk_update");
        setUserForRequest(updateWithBulkRestApi, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateWithBulkRestApi.setJsonEntity("{\"ids\": [\"" + apiKeyId + "\"]}");
        final ObjectPath bulkUpdateResponse = assertOKAndCreateObjectPath(client().performRequest(updateWithBulkRestApi));
        assertThat(bulkUpdateResponse.evaluate("errors.count"), equalTo(1));
        assertThat(
            bulkUpdateResponse.evaluate("errors.details." + apiKeyId + ".reason"),
            containsString("cannot update API key of type [cross_cluster] while expected type is [rest]")
        );

        // Cannot update REST API key with cross-cluster API
        final Request createRestApiKeyRequest = new Request("POST", "_security/api_key");
        setUserForRequest(createRestApiKeyRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        createRestApiKeyRequest.setJsonEntity("{\"name\":\"rest-key\"}");
        final ObjectPath createRestApiKeyResponse = assertOKAndCreateObjectPath(client().performRequest(createRestApiKeyRequest));
        final Request updateRestWithCrossClusterApi = new Request(
            "PUT",
            "/_security/cross_cluster/api_key/" + createRestApiKeyResponse.evaluate("id")
        );
        setUserForRequest(updateRestWithCrossClusterApi, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        updateRestWithCrossClusterApi.setJsonEntity("{\"metadata\":{}}");
        final ResponseException e6 = expectThrows(ResponseException.class, () -> client().performRequest(updateRestWithCrossClusterApi));
        assertThat(e6.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e6.getMessage(), containsString("cannot update API key of type [rest] while expected type is [cross_cluster]"));

        // Cannot update other's API keys
        final String anotherPowerUser = "another_power_user";
        createUser(anotherPowerUser, END_USER_PASSWORD, List.of("manage_security_role"));
        setUserForRequest(createRequest, anotherPowerUser, END_USER_PASSWORD);
        final ObjectPath anotherCrossClusterApiKey = assertOKAndCreateObjectPath(client().performRequest(createRequest));
        final Request anotherUpdateRequest = new Request(
            "PUT",
            "/_security/cross_cluster/api_key/" + anotherCrossClusterApiKey.evaluate("id")
        );
        setUserForRequest(anotherUpdateRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        anotherUpdateRequest.setJsonEntity("{\"metadata\":{}}");
        final ResponseException e7 = expectThrows(ResponseException.class, () -> client().performRequest(anotherUpdateRequest));
        assertThat(e7.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e7.getMessage(), containsString("no API key owned by requesting user found"));

        // Cannot update cross-cluster API key with manage_api_key or manage_own_api_keys
        createUser(anotherPowerUser, END_USER_PASSWORD, List.of(randomFrom("manage_api_key_role", "manage_own_api_key_role")));
        setUserForRequest(anotherUpdateRequest, anotherPowerUser, END_USER_PASSWORD);
        anotherUpdateRequest.setJsonEntity("{\"metadata\":{}}");
        final ResponseException e8 = expectThrows(ResponseException.class, () -> client().performRequest(anotherUpdateRequest));
        assertThat(e8.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e8.getMessage(), containsString("action [cluster:admin/xpack/security/cross_cluster/api_key/update] is unauthorized"));
    }

    public void testWorkflowsRestrictionSupportForApiKeys() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
                "name": "key1",
                "role_descriptors":{
                    "r1": {
                        "restriction": {
                            "workflows": ["search_application"]
                        }
                    }
                }
            }""");
        Response response = performRequestWithManageOwnApiKeyUser(createApiKeyRequest);
        String apiKeyId = assertOKAndCreateObjectPath(response).evaluate("id");
        assertThat(apiKeyId, notNullValue());
        fetchAndAssertApiKeyContainsWorkflows(apiKeyId, "r1", "search_application");

        final Request grantApiKeyRequest = new Request("POST", "_security/api_key/grant");
        grantApiKeyRequest.setJsonEntity(Strings.format("""
            {
               "grant_type":"password",
               "username":"%s",
               "password":"end-user-password",
               "api_key":{
                  "name":"key2",
                  "role_descriptors":{
                     "r1":{
                        "restriction": {
                            "workflows": ["search_application"]
                        }
                     }
                  }
               }
            }""", MANAGE_OWN_API_KEY_USER));
        response = adminClient().performRequest(grantApiKeyRequest);
        String grantedApiKeyId = assertOKAndCreateObjectPath(response).evaluate("id");
        fetchAndAssertApiKeyContainsWorkflows(grantedApiKeyId, "r1", "search_application");

        final Request updateApiKeyRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        updateApiKeyRequest.setJsonEntity("""
            {
              "role_descriptors": {
                "r1": {
                  "restriction": {
                   "workflows": ["search_application", "search_analytics"]
                  }
                }
              }
            }""");
        response = performRequestWithManageOwnApiKeyUser(updateApiKeyRequest);
        assertThat(assertOKAndCreateObjectPath(response).evaluate("updated"), equalTo(true));
        fetchAndAssertApiKeyContainsWorkflows(apiKeyId, "r1", "search_application", "search_analytics");

        final Request bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        bulkUpdateApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "ids": ["%s"],
              "role_descriptors": {
                "r1": {
                  "restriction": {
                     "workflows": ["search_application"]
                  }
                }
              }
            }""", apiKeyId));
        response = performRequestWithManageOwnApiKeyUser(bulkUpdateApiKeyRequest);
        assertThat(assertOKAndCreateObjectPath(response).evaluate("updated"), contains(apiKeyId));
        fetchAndAssertApiKeyContainsWorkflows(apiKeyId, "r1", "search_application");

        final Request removeRestrictionRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        removeRestrictionRequest.setJsonEntity("""
            {
              "role_descriptors": {
                "r1": {
                }
              }
            }""");
        response = performRequestWithManageOwnApiKeyUser(removeRestrictionRequest);
        assertThat(assertOKAndCreateObjectPath(response).evaluate("updated"), equalTo(true));
        fetchAndAssertApiKeyDoesNotContainWorkflows(apiKeyId, "r1");
    }

    public void testWorkflowsRestrictionValidation() throws IOException {
        final Request createInvalidApiKeyRequest = new Request("POST", "_security/api_key");
        final boolean secondRoleWithWorkflowsRestriction = randomBoolean();
        final String r1 = """
                "r1": {
                    "restriction": {
                        "workflows": ["search_application"]
                    }
                }
            """;
        final String r2 = secondRoleWithWorkflowsRestriction ? """
            "r2": {
                "restriction": {
                    "workflows": ["search_analytics"]
                }
            }
            """ : """
            "r2": {}
            """;
        createInvalidApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "name": "key1",
                "role_descriptors":{
                    %s,
                    %s
                }
            }""", r1, r2));
        var e = expectThrows(ResponseException.class, () -> performRequestWithManageOwnApiKeyUser(createInvalidApiKeyRequest));
        if (secondRoleWithWorkflowsRestriction) {
            assertThat(e.getMessage(), containsString("more than one role descriptor with restriction is not supported"));
        } else {
            assertThat(e.getMessage(), containsString("combining role descriptors with and without restriction is not supported"));
        }

        final Request grantApiKeyRequest = new Request("POST", "_security/api_key/grant");
        grantApiKeyRequest.setJsonEntity(Strings.format("""
            {
               "grant_type":"password",
               "username":"%s",
               "password":"end-user-password",
               "api_key":{
                  "name":"key2",
                  "role_descriptors":{
                     %s,
                     %s
                  }
               }
            }""", MANAGE_OWN_API_KEY_USER, r1, r2));
        e = expectThrows(ResponseException.class, () -> adminClient().performRequest(grantApiKeyRequest));
        if (secondRoleWithWorkflowsRestriction) {
            assertThat(e.getMessage(), containsString("more than one role descriptor with restriction is not supported"));
        } else {
            assertThat(e.getMessage(), containsString("combining role descriptors with and without restriction is not supported"));
        }

        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
                "name": "key1",
                "role_descriptors":{
                    "r1": {
                        "restriction": {
                            "workflows": ["search_application"]
                        }
                    }
                }
            }""");
        Response response = performRequestWithManageOwnApiKeyUser(createApiKeyRequest);
        assertOK(response);
        String apiKeyId = ObjectPath.createFromResponse(response).evaluate("id");
        assertThat(apiKeyId, notNullValue());

        final Request updateApiKeyRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        updateApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "role_descriptors": {
                    %s,
                    %s
              }
            }""", r1, r2));
        e = expectThrows(ResponseException.class, () -> performRequestWithManageOwnApiKeyUser(updateApiKeyRequest));
        if (secondRoleWithWorkflowsRestriction) {
            assertThat(e.getMessage(), containsString("more than one role descriptor with restriction is not supported"));
        } else {
            assertThat(e.getMessage(), containsString("combining role descriptors with and without restriction is not supported"));
        }

        final Request bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        bulkUpdateApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "ids": ["%s"],
                "role_descriptors": {
                    %s,
                    %s
                }
            }""", apiKeyId, r1, r2));
        e = expectThrows(ResponseException.class, () -> performRequestWithManageOwnApiKeyUser(bulkUpdateApiKeyRequest));
        if (secondRoleWithWorkflowsRestriction) {
            assertThat(e.getMessage(), containsString("more than one role descriptor with restriction is not supported"));
        } else {
            assertThat(e.getMessage(), containsString("combining role descriptors with and without restriction is not supported"));
        }
    }

    private Response performRequestWithManageOwnApiKeyUser(Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", headerFromRandomAuthMethod(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD))
        );
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private void fetchAndAssertApiKeyContainsWorkflows(String apiKeyId, String roleName, String... expectedWorkflows) throws IOException {
        Response getApiKeyResponse = fetchApiKey(apiKeyId);
        List<String> actualWorkflows = assertOKAndCreateObjectPath(getApiKeyResponse).evaluate(
            "api_keys.0.role_descriptors." + roleName + ".restriction.workflows"
        );
        assertThat(actualWorkflows, containsInAnyOrder(expectedWorkflows));
    }

    @SuppressWarnings("unchecked")
    private void fetchAndAssertApiKeyDoesNotContainWorkflows(String apiKeyId, String roleName) throws IOException {
        Response getApiKeyResponse = fetchApiKey(apiKeyId);
        Map<String, ?> restriction = assertOKAndCreateObjectPath(getApiKeyResponse).evaluate(
            "api_keys.0.role_descriptors." + roleName + ".restriction"
        );
        assertThat(restriction, nullValue());
    }

    private Response fetchApiKey(String apiKeyId) throws IOException {
        Request getApiKeyRequest = new Request(HttpGet.METHOD_NAME, "_security/api_key?id=" + apiKeyId);
        Response getApiKeyResponse = adminClient().performRequest(getApiKeyRequest);
        assertOK(getApiKeyResponse);
        return getApiKeyResponse;
    }

    private void assertBadCreateCrossClusterApiKeyRequest(String body, String expectedErrorMessage) throws IOException {
        final Request createRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createRequest.setJsonEntity(body);
        setUserForRequest(createRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(createRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    private Response sendRequestWithRemoteIndices(final Request request, final boolean executeAsRemoteIndicesUser) throws IOException {
        if (executeAsRemoteIndicesUser) {
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_INDICES_USER, END_USER_PASSWORD))
            );
            return client().performRequest(request);
        } else {
            return adminClient().performRequest(request);
        }
    }

    private void doTestAuthenticationWithApiKey(final String apiKeyName, final String apiKeyId, final String apiKeyEncoded)
        throws IOException {
        final var authenticateRequest = new Request("GET", "_security/_authenticate");
        authenticateRequest.setOptions(authenticateRequest.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + apiKeyEncoded));

        final Response authenticateResponse = client().performRequest(authenticateRequest);
        assertOK(authenticateResponse);
        final Map<String, Object> authenticate = responseAsMap(authenticateResponse); // keys: username, roles, full_name, etc

        // If authentication type is API_KEY, authentication.api_key={"id":"abc123","name":"my-api-key"}. No encoded, api_key, or metadata.
        // If authentication type is other, authentication.api_key not present.
        assertThat(authenticate, hasEntry("api_key", Map.of("id", apiKeyId, "name", apiKeyName)));
    }

    private void doTestUpdateApiKey(
        final String apiKeyName,
        final String apiKeyId,
        final String apiKeyEncoded,
        final Map<String, Object> oldMetadata
    ) throws IOException {
        final var updateApiKeyRequest = new Request("PUT", "_security/api_key/" + apiKeyId);
        final boolean updated = randomBoolean();
        final Map<String, Object> expectedApiKeyMetadata = updated ? Map.of("not", "returned (changed)", "foo", "bar") : oldMetadata;
        final Map<String, Object> updateApiKeyRequestBody = expectedApiKeyMetadata == null
            ? Map.of()
            : Map.of("metadata", expectedApiKeyMetadata);
        updateApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(updateApiKeyRequestBody, XContentType.JSON).utf8ToString());

        final Response updateApiKeyResponse = performRequestUsingRandomAuthMethod(updateApiKeyRequest);

        assertOK(updateApiKeyResponse);
        final Map<String, Object> updateApiKeyResponseMap = responseAsMap(updateApiKeyResponse);
        assertEquals(updated, updateApiKeyResponseMap.get("updated"));
        expectMetadata(apiKeyId, expectedApiKeyMetadata == null ? Map.of() : expectedApiKeyMetadata);
        // validate authentication still works after update
        doTestAuthenticationWithApiKey(apiKeyName, apiKeyId, apiKeyEncoded);
    }

    @SuppressWarnings({ "unchecked" })
    private void doTestUpdateApiKeyUsingBulkAction(
        final String apiKeyName,
        final String apiKeyId,
        final String apiKeyEncoded,
        final Map<String, Object> oldMetadata
    ) throws IOException {
        final var bulkUpdateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
        final boolean updated = randomBoolean();
        final Map<String, Object> expectedApiKeyMetadata = updated ? Map.of("not", "returned (changed)", "foo", "bar") : oldMetadata;
        final Map<String, Object> bulkUpdateApiKeyRequestBody = expectedApiKeyMetadata == null
            ? Map.of("ids", List.of(apiKeyId))
            : Map.of("ids", List.of(apiKeyId), "metadata", expectedApiKeyMetadata);
        bulkUpdateApiKeyRequest.setJsonEntity(
            XContentTestUtils.convertToXContent(bulkUpdateApiKeyRequestBody, XContentType.JSON).utf8ToString()
        );

        final Response bulkUpdateApiKeyResponse = performRequestUsingRandomAuthMethod(bulkUpdateApiKeyRequest);

        assertOK(bulkUpdateApiKeyResponse);
        final Map<String, Object> bulkUpdateApiKeyResponseMap = responseAsMap(bulkUpdateApiKeyResponse);
        assertThat(bulkUpdateApiKeyResponseMap, not(hasKey("errors")));
        if (updated) {
            assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("noops"), empty());
            assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("updated"), contains(apiKeyId));
        } else {
            assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("updated"), empty());
            assertThat((List<String>) bulkUpdateApiKeyResponseMap.get("noops"), contains(apiKeyId));
        }
        expectMetadata(apiKeyId, expectedApiKeyMetadata == null ? Map.of() : expectedApiKeyMetadata);
        // validate authentication still works after update
        doTestAuthenticationWithApiKey(apiKeyName, apiKeyId, apiKeyEncoded);
    }

    private Response performRequestUsingRandomAuthMethod(final Request request) throws IOException {
        final boolean useRunAs = randomBoolean();
        if (useRunAs) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(RUN_AS_USER_HEADER, MANAGE_OWN_API_KEY_USER));
            return adminClient().performRequest(request);
        } else {
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", headerFromRandomAuthMethod(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD))
            );
            return client().performRequest(request);
        }
    }

    private EncodedApiKey createApiKey(final String apiKeyName, final Map<String, Object> metadata) throws IOException {
        final Map<String, Object> createApiKeyRequestBody = Map.of("name", apiKeyName, "metadata", metadata);

        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity(XContentTestUtils.convertToXContent(createApiKeyRequestBody, XContentType.JSON).utf8ToString());
        createApiKeyRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", headerFromRandomAuthMethod(MANAGE_OWN_API_KEY_USER, END_USER_PASSWORD))
        );

        final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);
        final Map<String, Object> createApiKeyResponseMap = responseAsMap(createApiKeyResponse);
        final var apiKeyId = (String) createApiKeyResponseMap.get("id");
        final var apiKeyEncoded = (String) createApiKeyResponseMap.get("encoded");
        final var actualApiKeyName = (String) createApiKeyResponseMap.get("name");
        assertThat(apiKeyId, not(emptyString()));
        assertThat(apiKeyEncoded, not(emptyString()));
        assertThat(apiKeyName, equalTo(actualApiKeyName));

        return new EncodedApiKey(apiKeyId, apiKeyEncoded, actualApiKeyName);
    }

    private ObjectPath fetchCrossClusterApiKeyById(String apiKeyId) throws IOException {
        final Request fetchRequest;
        if (randomBoolean()) {
            fetchRequest = new Request("GET", "/_security/api_key");
            fetchRequest.addParameter("id", apiKeyId);
            fetchRequest.addParameter("with_limited_by", String.valueOf(randomBoolean()));
        } else {
            fetchRequest = new Request("GET", "/_security/_query/api_key");
            fetchRequest.addParameter("with_limited_by", String.valueOf(randomBoolean()));
            fetchRequest.setJsonEntity(Strings.format("""
                { "query": { "ids": { "values": ["%s"] } } }""", apiKeyId));
        }

        if (randomBoolean()) {
            setUserForRequest(fetchRequest, MANAGE_SECURITY_USER, END_USER_PASSWORD);
        } else {
            setUserForRequest(fetchRequest, MANAGE_API_KEY_USER, END_USER_PASSWORD);
        }
        final ObjectPath fetchResponse = assertOKAndCreateObjectPath(client().performRequest(fetchRequest));

        assertThat(fetchResponse.evaluate("api_keys.0.id"), equalTo(apiKeyId));
        assertThat(fetchResponse.evaluate("api_keys.0.type"), equalTo("cross_cluster"));
        return fetchResponse;
    }

    private void setUserForRequest(Request request, String username, SecureString password) throws IOException {
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .removeHeader("Authorization")
                .addHeader("Authorization", headerFromRandomAuthMethod(username, password))
        );
    }

    private String headerFromRandomAuthMethod(final String username, final SecureString password) throws IOException {
        final boolean useBearerTokenAuth = randomBoolean();
        if (useBearerTokenAuth) {
            final Tuple<String, String> token = super.createOAuthToken(username, password);
            return "Bearer " + token.v1();
        } else {
            return UsernamePasswordToken.basicAuthHeaderValue(username, password);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private void expectMetadata(final String apiKeyId, final Map<String, Object> expectedMetadata) throws IOException {
        final var request = new Request("GET", "_security/api_key/");
        request.addParameter("id", apiKeyId);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        try (XContentParser parser = responseAsParser(response)) {
            final var apiKeyResponse = GetApiKeyResponse.fromXContent(parser);
            assertThat(apiKeyResponse.getApiKeyInfos().length, equalTo(1));
            assertThat(apiKeyResponse.getApiKeyInfos()[0].getMetadata(), equalTo(expectedMetadata));
        }
    }

    private void expectErrorFields(final String type, final String reason, final Map<String, Object> rawError) {
        assertNotNull(rawError);
        assertEquals(type, rawError.get("type"));
        assertEquals(reason, rawError.get("reason"));
    }

    private record EncodedApiKey(String id, String encoded, String name) {}

    private void createRole(String name, Collection<String> clusterPrivileges, String... remoteIndicesClusterAliases) throws IOException {
        final RoleDescriptor role = new RoleDescriptor(
            name,
            clusterPrivileges.toArray(String[]::new),
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[0],
            null,
            null,
            null,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                RoleDescriptor.RemoteIndicesPrivileges.builder(remoteIndicesClusterAliases).indices("*").privileges("read").build() },
            null
        );
        getSecurityClient().putRole(role);
    }
}
