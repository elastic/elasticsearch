/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
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
import java.util.function.Consumer;

import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomApplicationPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRemoteClusterPermissions;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRemoteIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRoleDescriptorMetadata;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private RestClient oldVersionClient = null;
    private RestClient newVersionClient = null;

    public void testQueryRestTypeKeys() throws IOException {
        assumeTrue(
            "only API keys created pre-8.9 are relevant for the rest-type query bwc case",
            UPGRADE_FROM_VERSION.before(Version.V_8_9_0)
        );
        switch (CLUSTER_TYPE) {
            case OLD -> createOrGrantApiKey(client(), "query-test-rest-key-from-old-cluster", "{}");
            case MIXED -> createOrGrantApiKey(client(), "query-test-rest-key-from-mixed-cluster", "{}");
            case UPGRADED -> {
                createOrGrantApiKey(client(), "query-test-rest-key-from-upgraded-cluster", "{}");
                for (String query : List.of("""
                    {"query": {"term": {"type": "rest" }}}""", """
                    {"query": {"prefix": {"type": "re" }}}""", """
                    {"query": {"wildcard": {"type": "r*t" }}}""", """
                    {"query": {"range": {"type": {"gte": "raaa", "lte": "rzzz"}}}}""")) {
                    assertQuery(client(), query, apiKeys -> {
                        assertThat(
                            apiKeys.stream().map(k -> (String) k.get("name")).toList(),
                            hasItems(
                                "query-test-rest-key-from-old-cluster",
                                "query-test-rest-key-from-mixed-cluster",
                                "query-test-rest-key-from-upgraded-cluster"
                            )
                        );
                    });
                }
            }
        }
    }

    public void testCreatingAndUpdatingApiKeys() throws Exception {
        assumeTrue(
            "The remote_indices for API Keys are not supported before transport version "
                + RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY,
            minimumTransportVersion().before(RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY)
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
                    e = expectThrows(Exception.class, () -> createOrGrantApiKey(oldVersionClient, randomRoleDescriptors(true)));
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
                            "all nodes must have version ["
                                + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                                + "] or higher to support remote indices privileges for API keys"
                        )
                    );
                    e = expectThrows(
                        Exception.class,
                        () -> updateOrBulkUpdateApiKey(newVersionClient, apiKey.v1(), randomRoleDescriptors(true))
                    );
                    assertThat(
                        e.getMessage(),
                        containsString(
                            "all nodes must have version ["
                                + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion()
                                + "] or higher to support remote indices privileges for API keys"
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
        return createOrGrantApiKey(client, "test-api-key-" + randomAlphaOfLengthBetween(3, 5), roles);
    }

    private Tuple<String, String> createOrGrantApiKey(RestClient client, String name, String roles) throws IOException {
        final Request createApiKeyRequest;
        String body = Strings.format("""
            {
                "name": "%s",
                "role_descriptors": %s
            }""", name, roles);

        final boolean grantApiKey = randomBoolean();
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

    private void updateOrBulkUpdateApiKey(RestClient client, String id, String roles) throws IOException {
        final Request updateApiKeyRequest;
        final boolean bulkUpdate = randomBoolean();
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

    private static String randomRoleDescriptors(boolean includeRemoteDescriptors) {
        try {
            return XContentTestUtils.convertToXContent(Map.of("my_role", randomRoleDescriptor(includeRemoteDescriptors)), XContentType.JSON)
                .utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    boolean nodeSupportApiKeyRemoteIndices(Map<String, Object> nodeDetails) {
        String nodeVersionString = (String) nodeDetails.get("version");
        TransportVersion transportVersion = getTransportVersionWithFallback(
            nodeVersionString,
            nodeDetails.get("transport_version"),
            () -> TransportVersions.ZERO
        );

        if (transportVersion.equals(TransportVersions.ZERO)) {
            // In cases where we were not able to find a TransportVersion, a pre-8.8.0 node answered about a newer (upgraded) node.
            // In that case, the node will be current (upgraded), and remote indices are supported for sure.
            var nodeIsCurrent = nodeVersionString.equals(Build.current().version());
            assertTrue(nodeIsCurrent);
            return true;
        }
        return transportVersion.onOrAfter(RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY);
    }

    private void createClientsByVersion() throws IOException {
        var clientsByCapability = getRestClientByCapability();
        if (clientsByCapability.size() == 2) {
            for (Map.Entry<Boolean, RestClient> client : clientsByCapability.entrySet()) {
                if (client.getKey() == false) {
                    oldVersionClient = client.getValue();
                } else {
                    newVersionClient = client.getValue();
                }
            }
            assertThat(oldVersionClient, notNullValue());
            assertThat(newVersionClient, notNullValue());
        } else {
            fail("expected 2 versions during rolling upgrade but got: " + clientsByCapability.size());
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
    private Map<Boolean, RestClient> getRestClientByCapability() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Map<Boolean, List<HttpHost>> hostsByCapability = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            var capabilitySupported = nodeSupportApiKeyRemoteIndices(nodeDetails);
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            hostsByCapability.computeIfAbsent(capabilitySupported, k -> new ArrayList<>())
                .add(HttpHost.create((String) httpInfo.get("publish_address")));
        }
        Map<Boolean, RestClient> clientsByCapability = new HashMap<>();
        for (var entry : hostsByCapability.entrySet()) {
            clientsByCapability.put(entry.getKey(), buildClient(restClientSettings(), entry.getValue().toArray(new HttpHost[0])));
        }
        return clientsByCapability;
    }

    private static RoleDescriptor randomRoleDescriptor(boolean includeRemoteDescriptors) {
        final Set<String> excludedPrivileges = Set.of(
            "cross_cluster_replication",
            "cross_cluster_replication_internal",
            "manage_data_stream_lifecycle"
        );
        return new RoleDescriptor(
            randomAlphaOfLengthBetween(3, 90),
            randomSubsetOf(Set.of("all", "monitor", "none")).toArray(String[]::new),
            randomIndicesPrivileges(0, 3, excludedPrivileges),
            randomApplicationPrivileges(),
            null,
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            randomRoleDescriptorMetadata(false),
            Map.of(),
            includeRemoteDescriptors ? randomRemoteIndicesPrivileges(1, 3, excludedPrivileges) : null,
            includeRemoteDescriptors ? randomRemoteClusterPermissions(randomIntBetween(1, 3)) : RemoteClusterPermissions.NONE,
            null,
            null
        );
    }

    private void assertQuery(RestClient restClient, String body, Consumer<List<Map<String, Object>>> apiKeysVerifier) throws IOException {
        final Request request = new Request("GET", "/_security/_query/api_key");
        request.setJsonEntity(body);
        final Response response = restClient.performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> apiKeys = (List<Map<String, Object>>) responseMap.get("api_keys");
        apiKeysVerifier.accept(apiKeys);
    }
}
