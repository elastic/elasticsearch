/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptor.SECURITY_ROLE_DESCRIPTION;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomApplicationPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomManageRolesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRoleDescriptorMetadata;
import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.MANAGE_ROLES_PRIVILEGE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RolesBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    public void testRolesWithDescription() throws Exception {
        assumeTrue(
            "The role description is supported after transport version: " + SECURITY_ROLE_DESCRIPTION,
            minimumTransportVersion().supports(SECURITY_ROLE_DESCRIPTION) == false
        );
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // Creating role in "old" cluster should succeed when description is not provided
                final String initialRole = randomRoleDescriptorSerialized();
                createRole(client(), "my-old-role", initialRole);
                updateRole("my-old-role", randomValueOtherThan(initialRole, RolesBackwardsCompatibilityIT::randomRoleDescriptorSerialized));

                // and fail if we include description
                var createException = expectThrows(
                    Exception.class,
                    () -> createRole(client(), "my-invalid-old-role", randomRoleDescriptorWithDescriptionSerialized())
                );
                assertThat(
                    createException.getMessage(),
                    containsString("failed to parse role [my-invalid-old-role]. unexpected field [description]")
                );

                RestClient client = client();
                var updateException = expectThrows(
                    Exception.class,
                    () -> updateRole(client, "my-old-role", randomRoleDescriptorWithDescriptionSerialized())
                );
                assertThat(
                    updateException.getMessage(),
                    containsString("failed to parse role [my-old-role]. unexpected field [description]")
                );
            }
            case MIXED -> {
                try {
                    this.createClientsByCapability(node -> nodeSupportTransportVersion(node, RoleDescriptor.SECURITY_ROLE_DESCRIPTION));

                    // succeed when role description is not provided
                    final String initialRole = randomRoleDescriptorSerialized();
                    createRole(client(), "my-valid-mixed-role", initialRole);
                    updateRole(
                        "my-valid-mixed-role",
                        randomValueOtherThan(initialRole, RolesBackwardsCompatibilityIT::randomRoleDescriptorSerialized)
                    );

                    // against old node, fail when description is provided either in update or create request
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(oldVersionClient, "my-valid-mixed-role", randomRoleDescriptorWithDescriptionSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            allOf(containsString("failed to parse role"), containsString("unexpected field [description]"))
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(oldVersionClient, "my-invalid-mixed-role", randomRoleDescriptorWithDescriptionSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            containsString("failed to parse role [my-invalid-mixed-role]. unexpected field [description]")
                        );
                    }

                    // and against new node in a mixed cluster we should fail
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(newVersionClient, "my-invalid-mixed-role", randomRoleDescriptorWithDescriptionSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have version ["
                                    + SECURITY_ROLE_DESCRIPTION.toReleaseVersion()
                                    + "] or higher to support specifying role description"
                            )
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(newVersionClient, "my-valid-mixed-role", randomRoleDescriptorWithDescriptionSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have version ["
                                    + SECURITY_ROLE_DESCRIPTION.toReleaseVersion()
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
                final String initialRole = randomFrom(randomRoleDescriptorSerialized(), randomRoleDescriptorWithDescriptionSerialized());
                createRole(client(), "my-valid-upgraded-role", initialRole);
                updateRole(
                    "my-valid-upgraded-role",
                    randomValueOtherThan(
                        initialRole,
                        () -> randomFrom(randomRoleDescriptorSerialized(), randomRoleDescriptorWithDescriptionSerialized())
                    )
                );
            }
        }
    }

    public void testRolesWithManageRoles() throws Exception {
        assumeTrue(
            "The manage roles privilege is supported after transport version: " + MANAGE_ROLES_PRIVILEGE,
            minimumTransportVersion().supports(MANAGE_ROLES_PRIVILEGE) == false
        );
        switch (CLUSTER_TYPE) {
            case OLD -> {
                // Creating role in "old" cluster should succeed when manage roles is not provided
                final String initialRole = randomRoleDescriptorSerialized();
                createRole(client(), "my-old-role", initialRole);
                updateRole("my-old-role", randomValueOtherThan(initialRole, RolesBackwardsCompatibilityIT::randomRoleDescriptorSerialized));

                // and fail if we include manage roles
                var createException = expectThrows(
                    Exception.class,
                    () -> createRole(client(), "my-invalid-old-role", randomRoleDescriptorWithManageRolesSerialized())
                );
                assertThat(
                    createException.getMessage(),
                    allOf(containsString("failed to parse privilege"), containsString("but found [role] instead"))
                );

                RestClient client = client();
                var updateException = expectThrows(
                    Exception.class,
                    () -> updateRole(client, "my-old-role", randomRoleDescriptorWithManageRolesSerialized())
                );
                assertThat(
                    updateException.getMessage(),
                    allOf(containsString("failed to parse privilege"), containsString("but found [role] instead"))
                );
            }
            case MIXED -> {
                try {
                    this.createClientsByCapability(node -> nodeSupportTransportVersion(node, MANAGE_ROLES_PRIVILEGE));
                    // succeed when role manage roles is not provided
                    final String initialRole = randomRoleDescriptorSerialized();
                    createRole(client(), "my-valid-mixed-role", initialRole);
                    updateRole(
                        "my-valid-mixed-role",
                        randomValueOtherThan(initialRole, RolesBackwardsCompatibilityIT::randomRoleDescriptorSerialized)
                    );

                    // against old node, fail when manage roles is provided either in update or create request
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(oldVersionClient, "my-valid-mixed-role", randomRoleDescriptorWithManageRolesSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            allOf(containsString("failed to parse privilege"), containsString("but found [role] instead"))
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(oldVersionClient, "my-invalid-mixed-role", randomRoleDescriptorWithManageRolesSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            allOf(containsString("failed to parse privilege"), containsString("but found [role] instead"))
                        );
                    }

                    // and against new node in a mixed cluster we should fail
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> createRole(newVersionClient, "my-invalid-mixed-role", randomRoleDescriptorWithManageRolesSerialized())
                        );

                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have version ["
                                    + MANAGE_ROLES_PRIVILEGE.toReleaseVersion()
                                    + "] or higher to support the manage roles privilege"
                            )
                        );
                    }
                    {
                        Exception e = expectThrows(
                            Exception.class,
                            () -> updateRole(newVersionClient, "my-valid-mixed-role", randomRoleDescriptorWithManageRolesSerialized())
                        );
                        assertThat(
                            e.getMessage(),
                            containsString(
                                "all nodes must have version ["
                                    + MANAGE_ROLES_PRIVILEGE.toReleaseVersion()
                                    + "] or higher to support the manage roles privilege"
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
                final String initialRole = randomFrom(randomRoleDescriptorSerialized(), randomRoleDescriptorWithManageRolesSerialized());
                createRole(client(), "my-valid-upgraded-role", initialRole);
                updateRole(
                    "my-valid-upgraded-role",
                    randomValueOtherThan(
                        initialRole,
                        () -> randomFrom(randomRoleDescriptorSerialized(), randomRoleDescriptorWithManageRolesSerialized())
                    )
                );
            }
        }
    }

    private void createRole(RestClient client, String roleName, String role) throws IOException {
        final Request createRoleRequest = new Request("POST", "_security/role/" + roleName);
        createRoleRequest.setJsonEntity(role);
        var createRoleResponse = client.performRequest(createRoleRequest);
        assertOK(createRoleResponse);
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

    private static String randomRoleDescriptorSerialized() {
        return randomRoleDescriptorSerialized(false, false);
    }

    private static String randomRoleDescriptorWithDescriptionSerialized() {
        return randomRoleDescriptorSerialized(true, false);
    }

    private static String randomRoleDescriptorWithManageRolesSerialized() {
        return randomRoleDescriptorSerialized(false, true);
    }

    private static String randomRoleDescriptorSerialized(boolean includeDescription, boolean includeManageRoles) {
        try {
            return XContentTestUtils.convertToXContent(
                XContentTestUtils.convertToMap(randomRoleDescriptor(includeDescription, includeManageRoles)),
                XContentType.JSON
            ).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean nodeSupportTransportVersion(TestNodeInfo testNodeInfo, TransportVersion transportVersion) {
        if (testNodeInfo.transportVersion().equals(TransportVersion.zero())) {
            // In cases where we were not able to find a TransportVersion, a pre-8.8.0 node answered about a newer (upgraded) node.
            // In that case, the node will be current (upgraded), and remote indices are supported for sure.
            var nodeIsCurrent = testNodeInfo.version().equals(Build.current().version());
            assertTrue(nodeIsCurrent);
            return true;
        }
        return testNodeInfo.transportVersion().supports(transportVersion);
    }

    private static RoleDescriptor randomRoleDescriptor(boolean includeDescription, boolean includeManageRoles) {
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
            includeManageRoles ? randomManageRolesPrivileges() : null,
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            randomRoleDescriptorMetadata(false),
            Map.of(),
            null,
            null,
            null,
            includeDescription ? randomAlphaOfLength(20) : null
        );
    }
}
