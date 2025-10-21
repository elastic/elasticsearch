/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
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
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;
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
    private static final String CERTIFICATE_IDENTITY_FIELD_FEATURE = "certificate_identity_field";

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
                    this.createClientsByCapability(this::nodeSupportApiKeyRemoteIndices);
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

    public void testCertificateIdentityBackwardsCompatibility() throws Exception {
        final Set<TestNodeInfo> nodes = collectNodeInfos(adminClient());

        final Set<TestNodeInfo> newVersionNodes = nodes.stream().filter(TestNodeInfo::isUpgradedVersionCluster).collect(toSet());
        final Set<TestNodeInfo> oldVersionNodes = nodes.stream().filter(TestNodeInfo::isOriginalVersionCluster).collect(toSet());

        assumeTrue(
            "Old version nodes must not support certificate identity feature",
            oldVersionNodes.stream().noneMatch(info -> info.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE))
        );
        assumeTrue(
            "New version nodes must support certificate identity feature",
            newVersionNodes.stream().allMatch(info -> info.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE))
        );

        switch (CLUSTER_TYPE) {
            case OLD -> {
                var exception = expectThrows(Exception.class, () -> createCrossClusterApiKeyWithCertIdentity("CN=test-.*"));
                assertThat(
                    exception.getMessage(),
                    anyOf(containsString("unknown field [certificate_identity]"), containsString("certificate_identity not supported"))
                );
            }
            case MIXED -> {
                try {
                    this.createClientsByCapability(this::nodeSupportsCertificateIdentity);

                    // Test against old node - should get parsing error
                    Exception oldNodeException = expectThrows(
                        Exception.class,
                        () -> createCrossClusterApiKeyWithCertIdentity(oldVersionClient, "CN=test-.*")
                    );
                    assertThat(
                        oldNodeException.getMessage(),
                        anyOf(containsString("unknown field [certificate_identity]"), containsString("certificate_identity not supported"))
                    );

                    // Test against new node - should get mixed-version error
                    Exception newNodeException = expectThrows(
                        Exception.class,
                        () -> createCrossClusterApiKeyWithCertIdentity(newVersionClient, "CN=test-.*")
                    );
                    assertThat(
                        newNodeException.getMessage(),
                        containsString("cluster is in a mixed-version state and does not yet support the [certificate_identity] field")
                    );
                } finally {
                    this.closeClientsByVersion();
                }
            }
            case UPGRADED -> {
                // Fully upgraded cluster should support certificate identity
                final Tuple<String, String> apiKey = createCrossClusterApiKeyWithCertIdentity("CN=test-.*");

                // Verify the API key was created with certificate identity
                final Request getApiKeyRequest = new Request("GET", "/_security/api_key");
                getApiKeyRequest.addParameter("id", apiKey.v1());
                final Response getResponse = client().performRequest(getApiKeyRequest);
                assertOK(getResponse);

                final ObjectPath getPath = ObjectPath.createFromResponse(getResponse);
                assertThat(getPath.evaluate("api_keys.0.certificate_identity"), equalTo("CN=test-.*"));
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

    boolean nodeSupportApiKeyRemoteIndices(TestNodeInfo testNodeInfo) {
        if (testNodeInfo.transportVersion().equals(TransportVersion.zero())) {
            // In cases where we were not able to find a TransportVersion, a pre-8.8.0 node answered about a newer (upgraded) node.
            // In that case, the node will be current (upgraded), and remote indices are supported for sure.
            var nodeIsCurrent = testNodeInfo.version().equals(Build.current().version());
            assertTrue(nodeIsCurrent);
            return true;
        }
        return testNodeInfo.transportVersion().onOrAfter(RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY);
    }

    private static RoleDescriptor randomRoleDescriptor(boolean includeRemoteDescriptors) {
        final Set<String> excludedPrivileges = Set.of(
            "read_failure_store",
            "manage_failure_store",
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

    private boolean nodeSupportsCertificateIdentity(TestNodeInfo nodeDetails) {
        return nodeDetails.supportsFeature(CERTIFICATE_IDENTITY_FIELD_FEATURE);
    }

    private Tuple<String, String> createCrossClusterApiKeyWithCertIdentity(String certificateIdentity) throws IOException {
        return createCrossClusterApiKeyWithCertIdentity(client(), certificateIdentity);
    }

    private Tuple<String, String> createCrossClusterApiKeyWithCertIdentity(RestClient client, String certificateIdentity)
        throws IOException {
        final String name = "test-cc-api-key-" + randomAlphaOfLengthBetween(3, 5);
        final Request createApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createApiKeyRequest.setJsonEntity(Strings.format("""
            {
                "name": "%s",
                "certificate_identity": "%s",
                "access": {
                    "search": [
                        {
                            "names": ["test-*"]
                        }
                    ]
                }
            }""", name, certificateIdentity));

        final Response createResponse = client.performRequest(createApiKeyRequest);
        assertOK(createResponse);
        final ObjectPath path = ObjectPath.createFromResponse(createResponse);
        final String id = path.evaluate("id");
        final String key = path.evaluate("api_key");
        assertThat(id, notNullValue());
        assertThat(key, notNullValue());
        return Tuple.tuple(id, key);
    }

}
