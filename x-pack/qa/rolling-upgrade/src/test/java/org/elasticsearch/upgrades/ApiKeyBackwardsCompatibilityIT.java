/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    public static final Version API_KEY_SUPPORT_REMOTE_INDICES_VERSION = Version.V_8_8_0;

    private RestClient oldVersionClient = null;
    private RestClient newVersionClient = null;

    public void testCreatingAndUpdatingApiKeys() throws Exception {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
        assumeTrue(
            "The remote_indices for API Keys are not supported before version " + API_KEY_SUPPORT_REMOTE_INDICES_VERSION,
            UPGRADE_FROM_VERSION.before(API_KEY_SUPPORT_REMOTE_INDICES_VERSION)
        );
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // succeed when remote_indices are not provided
                final boolean includeRoles = randomBoolean();
                final String initialApiKeyRole = includeRoles ? randomRoleDescriptors(false) : "{}";
                final Tuple<String, String> apiKey = createOrGrantApiKey(initialApiKeyRole);
                updateOrBulkUpdateApiKey(apiKey.v1(), randomValueOtherThan(initialApiKeyRole, () -> randomRoleDescriptors(false)));
                authenticateWithApiKey(apiKey.v1(), apiKey.v2());

                // fail if we include remote_indices
                var createException = expectThrows(Exception.class, () -> createOrGrantApiKey(randomRoleDescriptors(true)));
                assertThat(
                    createException.getMessage(),
                    anyOf(
                        containsString("failed to parse role [my_role]. unexpected field [remote_indices]"),
                        containsString("remote indices not supported for API keys")
                    )
                );

                RestClient client = client();
                if (isUpdateApiSupported(client)) {
                    var updateException = expectThrows(
                        Exception.class,
                        () -> updateOrBulkUpdateApiKey(client, apiKey.v1(), randomRoleDescriptors(true))
                    );

                    assertThat(
                        updateException.getMessage(),
                        anyOf(
                            containsString("failed to parse role [my_role]. unexpected field [remote_indices]"),
                            containsString("remote indices not supported for API keys")
                        )
                    );
                }
            }
            case MIXED -> {
                try {
                    this.createClientsByVersion();
                    // succeed when remote_indices are not provided
                    final boolean includeRoles = randomBoolean();
                    final String initialApiKeyRole = includeRoles ? randomRoleDescriptors(false) : "{}";
                    final Tuple<String, String> apiKey = createOrGrantApiKey(initialApiKeyRole);
                    updateOrBulkUpdateApiKey(apiKey.v1(), randomValueOtherThan(initialApiKeyRole, () -> randomRoleDescriptors(false)));
                    authenticateWithApiKey(apiKey.v1(), apiKey.v2());

                    // fail when remote_indices are provided:
                    // against old node
                    if (isUpdateApiSupported(oldVersionClient)) {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateOrBulkUpdateApiKey(oldVersionClient, apiKey.v1(), randomRoleDescriptors(true))
                        );
                        assertThat(
                            e.getMessage(),
                            anyOf(
                                containsString("failed to parse role [my_role]. unexpected field [remote_indices]"),
                                containsString("remote indices not supported for API keys")
                            )
                        );
                    }
                    Exception e = expectThrows(Exception.class, () -> createOrGrantApiKey(oldVersionClient, randomRoleDescriptors(true)));
                    assertThat(
                        e.getMessage(),
                        anyOf(
                            containsString("failed to parse role [my_role]. unexpected field [remote_indices]"),
                            containsString("remote indices not supported for API keys")
                        )
                    );

                    // and against new node
                    e = expectThrows(Exception.class, () -> createOrGrantApiKey(newVersionClient, randomRoleDescriptors(true)));
                    assertThat(
                        e.getMessage(),
                        containsString(
                            "all nodes must have transport version [8080099] or higher to support remote indices privileges for API keys"
                        )
                    );
                    e = expectThrows(
                        Exception.class,
                        () -> updateOrBulkUpdateApiKey(newVersionClient, apiKey.v1(), randomRoleDescriptors(true))
                    );
                    assertThat(
                        e.getMessage(),
                        containsString(
                            "all nodes must have transport version [8080099] or higher to support remote indices privileges for API keys"
                        )
                    );
                } finally {
                    this.closeClientsByVersion();
                }
            }
            case UPGRADED -> {
                // succeed either way
                final boolean includeRoles = randomBoolean();
                final String initialApiKeyRole = includeRoles ? randomRoleDescriptors(false) : "{}";
                final Tuple<String, String> apiKey = createOrGrantApiKey(initialApiKeyRole);
                updateOrBulkUpdateApiKey(apiKey.v1(), randomValueOtherThan(initialApiKeyRole, () -> randomRoleDescriptors(false)));
                authenticateWithApiKey(apiKey.v1(), apiKey.v2());

                final String initialApiKeyRoleWithRemoteIndices = randomRoleDescriptors(true);
                final Tuple<String, String> apiKeyWithRemoteIndices = createOrGrantApiKey(initialApiKeyRoleWithRemoteIndices);
                updateOrBulkUpdateApiKey(
                    apiKeyWithRemoteIndices.v1(),
                    randomValueOtherThan(initialApiKeyRoleWithRemoteIndices, () -> randomRoleDescriptors(true))
                );
                authenticateWithApiKey(apiKeyWithRemoteIndices.v1(), apiKeyWithRemoteIndices.v2());
            }
        }
    }

    private Tuple<String, String> createOrGrantApiKey(String roles) throws IOException {
        return createOrGrantApiKey(client(), roles);
    }

    private Tuple<String, String> createOrGrantApiKey(RestClient client, String roles) throws IOException {
        final String name = "test-api-key-" + randomAlphaOfLengthBetween(3, 5);
        final Request createApiKeyRequest;
        String body = Strings.format("""
            {
                "name": "%s",
                "role_descriptors": %s
            }""", name, roles);
        // Grant API did not exist before 7.7.0
        final boolean grantApiKey = randomBoolean() && UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_7_0);
        if (grantApiKey) {
            createApiKeyRequest = new Request("POST", "/_security/api_key/grant");
            createApiKeyRequest.setJsonEntity(org.elasticsearch.common.Strings.format("""
                    {
                        "grant_type" : "password",
                        "username"   : "%s",
                        "password"   : "%s",
                        "api_key"    :  %s
                    }
                """, "test_user", SecuritySettingsSourceField.TEST_PASSWORD, body));
        } else {
            createApiKeyRequest = new Request("POST", "_security/api_key");
            createApiKeyRequest.setJsonEntity(body);
        }

        final Response createApiKeyResponse;
        if (grantApiKey) {
            createApiKeyResponse = client.performRequest(createApiKeyRequest);
        } else {
            createApiKeyResponse = client.performRequest(createApiKeyRequest);
        }
        assertOK(createApiKeyResponse);
        final ObjectPath path = ObjectPath.createFromResponse(createApiKeyResponse);
        final String id = path.evaluate("id");
        final String key = path.evaluate("api_key");
        assertThat(id, notNullValue());
        assertThat(key, notNullValue());
        return Tuple.tuple(id, key);
    }

    private void updateOrBulkUpdateApiKey(String id, String roles) throws IOException {
        updateOrBulkUpdateApiKey(client(), id, roles);
    }

    private boolean isUpdateApiSupported(RestClient client) {
        return switch (CLUSTER_TYPE) {
            case OLD -> UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_4_0); // Update API was introduced in 8.4.0.
            case MIXED -> UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_4_0) || client == newVersionClient;
            case UPGRADED -> true;
        };
    }

    private boolean isBulkUpdateApiSupported(RestClient client) {
        return switch (CLUSTER_TYPE) {
            case OLD -> UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_5_0); // Bulk update API was introduced in 8.5.0.
            case MIXED -> UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_5_0) || client == newVersionClient;
            case UPGRADED -> true;
        };
    }

    private void updateOrBulkUpdateApiKey(RestClient client, String id, String roles) throws IOException {
        if (false == isUpdateApiSupported(client)) {
            return; // Update API is not supported.
        }
        final Request updateApiKeyRequest;
        final boolean bulkUpdate = randomBoolean() && isBulkUpdateApiSupported(client);
        if (bulkUpdate) {
            updateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
            updateApiKeyRequest.setJsonEntity(org.elasticsearch.common.Strings.format("""
                {
                    "ids": [ "%s" ],
                    "role_descriptors": %s
                }
                """, id, roles));
        } else {
            updateApiKeyRequest = new Request("PUT", "_security/api_key/" + id);
            updateApiKeyRequest.setJsonEntity(org.elasticsearch.common.Strings.format("""
                {
                    "role_descriptors": %s
                }
                """, roles));
        }

        final Response updateApiKeyResponse = client.performRequest(updateApiKeyRequest);
        assertOK(updateApiKeyResponse);

        if (bulkUpdate) {
            List<String> updated = ObjectPath.createFromResponse(updateApiKeyResponse).evaluate("updated");
            assertThat(updated.size(), equalTo(1));
            assertThat(updated.get(0), equalTo(id));
        } else {
            boolean updated = ObjectPath.createFromResponse(updateApiKeyResponse).evaluate("updated");
            assertThat(updated, equalTo(true));
        }
    }

    private Map<String, Object> authenticateWithApiKey(String id, String key) throws IOException {
        return authenticateWithApiKey(client(), id, key);
    }

    private Map<String, Object> authenticateWithApiKey(RestClient client, String id, String key) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_security/_authenticate");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", getApiKeyAuthorizationHeaderValue(id, key)).build()
        );
        final Response response = client.performRequest(request);
        assertOK(response);
        ObjectPath path = ObjectPath.createFromResponse(response);
        final String authenticationTypeString = path.evaluate(User.Fields.AUTHENTICATION_TYPE.getPreferredName());
        final Authentication.AuthenticationType authenticationType = Authentication.AuthenticationType.valueOf(
            authenticationTypeString.toUpperCase(Locale.ROOT)
        );
        assertThat(authenticationType, is(Authentication.AuthenticationType.API_KEY));
        return responseAsMap(response);
    }

    private String getApiKeyAuthorizationHeaderValue(String id, String key) {
        return "ApiKey " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
    }

    private static String randomRoleDescriptors(boolean includeRemoteIndices) {
        try {
            return XContentTestUtils.convertToXContent(Map.of("my_role", randomRoleDescriptor(includeRemoteIndices)), XContentType.JSON)
                .utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createClientsByVersion() throws IOException {
        Map<Version, RestClient> clientsByVersion = getRestClientByVersion();
        if (clientsByVersion.size() == 2) {
            for (Map.Entry<Version, RestClient> client : clientsByVersion.entrySet()) {
                if (client.getKey().before(API_KEY_SUPPORT_REMOTE_INDICES_VERSION)) {
                    oldVersionClient = client.getValue();
                } else {
                    newVersionClient = client.getValue();
                }
            }
            assertThat(oldVersionClient, notNullValue());
            assertThat(newVersionClient, notNullValue());
        } else {
            fail("expected 2 versions during rolling upgrade but got: " + clientsByVersion.size());
        }
    }

    private void closeClientsByVersion() throws IOException {
        if (oldVersionClient != null) {
            oldVersionClient.close();
            oldVersionClient = null;
        }
        if (newVersionClient != null) {
            newVersionClient.close();
            newVersionClient = null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Version, RestClient> getRestClientByVersion() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Map<Version, List<HttpHost>> hostsByVersion = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            Version version = Version.fromString((String) nodeDetails.get("version"));
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            hostsByVersion.computeIfAbsent(version, k -> new ArrayList<>()).add(HttpHost.create((String) httpInfo.get("publish_address")));
        }
        Map<Version, RestClient> clientsByVersion = new HashMap<>();
        for (Map.Entry<Version, List<HttpHost>> entry : hostsByVersion.entrySet()) {
            clientsByVersion.put(entry.getKey(), buildClient(restClientSettings(), entry.getValue().toArray(new HttpHost[0])));
        }
        return clientsByVersion;
    }

    private static RoleDescriptor randomRoleDescriptor(boolean includeRemoteIndices) {
        final Set<String> excludedPrivileges = Set.of("cross_cluster_replication", "cross_cluster_replication_internal");
        return new RoleDescriptor(
            randomAlphaOfLengthBetween(3, 90),
            randomSubsetOf(Set.of("all", "monitor", "none")).toArray(String[]::new),
            RoleDescriptorTests.randomIndicesPrivileges(0, 3, excludedPrivileges),
            RoleDescriptorTests.randomApplicationPrivileges(),
            null,
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(false),
            Map.of(),
            includeRemoteIndices ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 3, excludedPrivileges) : null
        );
    }
}
