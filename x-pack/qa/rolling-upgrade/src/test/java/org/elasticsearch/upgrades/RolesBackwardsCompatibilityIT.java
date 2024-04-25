/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomApplicationPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRoleDescriptorMetadata;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RolesBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    private RestClient oldVersionClient = null;
    private RestClient newVersionClient = null;

    public void testCreatingAndUpdatingRoles() throws Exception {
        assumeTrue(
            "The role description is supported after transport version: " + TransportVersions.SECURITY_ROLE_DESCRIPTION,
            minimumTransportVersion().before(TransportVersions.SECURITY_ROLE_DESCRIPTION)
        );
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // Creating role in "old" cluster should succeed when description is not provided
                final String initialRole = randomRoleDescriptorSerialized(false);
                final String roleName = createRole(initialRole);
                updateRole(roleName, randomValueOtherThan(initialRole, () -> randomRoleDescriptorSerialized(false)));

                // and fail if we include description
                var createException = expectThrows(Exception.class, () -> createRole(randomRoleDescriptorSerialized(true)));
                assertThat(
                    createException.getMessage(),
                    allOf(containsString("failed to parse role"), containsString("unexpected field [description]"))
                );

                RestClient client = client();
                var updateException = expectThrows(
                    Exception.class,
                    () -> updateRole(client, roleName, randomRoleDescriptorSerialized(true))
                );
                assertThat(
                    updateException.getMessage(),
                    containsString("failed to parse role [" + roleName + "]. unexpected field [description]")
                );
            }
            case MIXED -> {
                try {
                    this.createClientsByVersion();
                    // succeed when role description is not provided
                    final String initialRole = randomRoleDescriptorSerialized(false);
                    final String roleName = createRole(initialRole);
                    updateRole(roleName, randomValueOtherThan(initialRole, () -> randomRoleDescriptorSerialized(false)));

                    // against old node, fail when description is provided either in update or create request
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(oldVersionClient, roleName, randomRoleDescriptorSerialized(true))
                        );
                        assertThat(
                            e.getMessage(),
                            allOf(containsString("failed to parse role"), containsString("unexpected field [description]"))
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(oldVersionClient, randomRoleDescriptorSerialized(true))
                        );
                        assertThat(
                            e.getMessage(),
                            allOf(containsString("failed to parse role"), containsString("unexpected field [description]"))
                        );
                    }

                    // and against new node in a mixed cluster we should fail
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(newVersionClient, randomRoleDescriptorSerialized(true))
                        );
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have transport version ["
                                    + TransportVersions.SECURITY_ROLE_DESCRIPTION
                                    + "] or higher to support specifying role description"
                            )
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(newVersionClient, roleName, randomRoleDescriptorSerialized(true))
                        );
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have transport version ["
                                    + TransportVersions.SECURITY_ROLE_DESCRIPTION
                                    + "] or higher to support specifying role description"
                            )
                        );
                    }
                } finally {
                    this.closeClientsByVersion();
                }
            }
            case UPGRADED -> {
                // on upgraded cluster which supports new description field
                // create/update requests should succeed either way (with or without description)
                final String initialRole = randomRoleDescriptorSerialized(randomBoolean());
                final String roleName = createRole(initialRole);
                updateRole(roleName, randomValueOtherThan(initialRole, () -> randomRoleDescriptorSerialized(randomBoolean())));
            }
        }
    }

    private String createRole(String role) throws IOException {
        return createRole(client(), role);
    }

    private String createRole(RestClient client, String role) throws IOException {
        return createRole(client, "test-role-" + randomAlphaOfLengthBetween(5, 10), role);
    }

    private String createRole(RestClient client, String roleName, String role) throws IOException {
        final Request createRoleRequest = new Request("POST", "_security/role/" + roleName);
        createRoleRequest.setJsonEntity(role);
        var createRoleResponse = client.performRequest(createRoleRequest);
        final ObjectPath path = assertOKAndCreateObjectPath(createRoleResponse);
        boolean created = path.evaluate("role.created");
        assertThat(created, equalTo(true));
        return roleName;
    }

    private void updateRole(String roleName, String payload) throws IOException {
        updateRole(client(), roleName, payload);
    }

    private void updateRole(RestClient client, String roleName, String payload) throws IOException {
        final Request updateRequest = new Request("PUT", "_security/role/" + roleName);
        updateRequest.setJsonEntity(payload);
        boolean created = assertOKAndCreateObjectPath(client.performRequest(updateRequest)).evaluate("role.created");
        assertThat(created, equalTo(false));
    }

    private static String randomRoleDescriptorSerialized(boolean includeDescription) {
        try {
            return XContentTestUtils.convertToXContent(
                XContentTestUtils.convertToMap(randomRoleDescriptor(includeDescription)),
                XContentType.JSON
            ).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean nodeSupportRoleDescription(Map<String, Object> nodeDetails) {
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
        return transportVersion.onOrAfter(TransportVersions.SECURITY_ROLE_DESCRIPTION);
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
            var capabilitySupported = nodeSupportRoleDescription(nodeDetails);
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

    private static RoleDescriptor randomRoleDescriptor(boolean includeDescription) {
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
            null,
            null,
            includeDescription ? randomAlphaOfLength(20) : null
        );
    }
}
