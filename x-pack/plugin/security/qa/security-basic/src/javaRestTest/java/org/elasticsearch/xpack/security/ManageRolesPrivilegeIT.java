/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.StringContains.containsString;

public class ManageRolesPrivilegeIT extends SecurityInBasicRestTestCase {

    private TestSecurityClient adminSecurityClient;
    private static final SecureString TEST_PASSWORD = new SecureString("100%-secure-password".toCharArray());

    @Before
    public void setupClient() {
        adminSecurityClient = new TestSecurityClient(adminClient());
    }

    public void testManageRoles() throws Exception {
        createManageRolesRole("manage-roles-role", new String[0], Set.of("*-allowed-suffix"), Set.of("read", "write"));
        createUser("test-user", Set.of("manage-roles-role"));

        String authHeader = basicAuthHeaderValue("test-user", TEST_PASSWORD);

        createRole(
            authHeader,
            new RoleDescriptor(
                "manage-roles-role",
                new String[0],
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("test-allowed-suffix").privileges(Set.of("read", "write")).build() },
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                new ConfigurableClusterPrivilege[0],
                new String[0],
                Map.of(),
                Map.of()
            )
        );

        {
            ResponseException responseException = assertThrows(
                ResponseException.class,
                () -> createRole(
                    authHeader,
                    new RoleDescriptor(
                        "manage-roles-role",
                        new String[0],
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices("test-suffix-not-allowed").privileges("write").build() },
                        new RoleDescriptor.ApplicationResourcePrivileges[0],
                        new ConfigurableClusterPrivilege[0],
                        new String[0],
                        Map.of(),
                        Map.of()
                    )
                )
            );

            assertThat(
                responseException.getMessage(),
                containsString("this action is granted by the cluster privileges [manage_security,all]")
            );
        }

        {
            ResponseException responseException = assertThrows(
                ResponseException.class,
                () -> createRole(
                    authHeader,
                    new RoleDescriptor(
                        "manage-roles-role",
                        new String[0],
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices("test-allowed-suffix").privileges("manage").build() },
                        new RoleDescriptor.ApplicationResourcePrivileges[0],
                        new ConfigurableClusterPrivilege[0],
                        new String[0],
                        Map.of(),
                        Map.of()
                    )
                )
            );
            assertThat(
                responseException.getMessage(),
                containsString("this action is granted by the cluster privileges [manage_security,all]")
            );
        }
    }

    public void testManageSecurityNullifiesManageRoles() throws Exception {
        createManageRolesRole("manage-roles-no-manage-security", new String[0], Set.of("allowed"));
        createManageRolesRole("manage-roles-manage-security", new String[] { "manage_security" }, Set.of("allowed"));

        createUser("test-user-no-manage-security", Set.of("manage-roles-no-manage-security"));
        createUser("test-user-manage-security", Set.of("manage-roles-manage-security"));

        String authHeaderNoManageSecurity = basicAuthHeaderValue("test-user-no-manage-security", TEST_PASSWORD);
        String authHeaderManageSecurity = basicAuthHeaderValue("test-user-manage-security", TEST_PASSWORD);

        createRole(
            authHeaderNoManageSecurity,
            new RoleDescriptor(
                "test-role-allowed-by-manage-roles",
                new String[0],
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("allowed").privileges("read").build() },
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                new ConfigurableClusterPrivilege[0],
                new String[0],
                Map.of(),
                Map.of()
            )
        );

        ResponseException responseException = assertThrows(
            ResponseException.class,
            () -> createRole(
                authHeaderNoManageSecurity,
                new RoleDescriptor(
                    "test-role-not-allowed-by-manage-roles",
                    new String[0],
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("not-allowed").privileges("read").build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[0],
                    new ConfigurableClusterPrivilege[0],
                    new String[0],
                    Map.of(),
                    Map.of()
                )
            )
        );

        assertThat(
            responseException.getMessage(),
            // TODO Should the new global role/manage privilege be listed here? Probably not because it's not documented
            containsString("this action is granted by the cluster privileges [manage_security,all]")
        );

        createRole(
            authHeaderManageSecurity,
            new RoleDescriptor(
                "test-role-not-allowed-by-manage-roles",
                new String[0],
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("not-allowed").privileges("read").build() },
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                new ConfigurableClusterPrivilege[0],
                new String[0],
                Map.of(),
                Map.of()
            )
        );
    }

    private void createRole(String authHeader, RoleDescriptor descriptor) throws IOException {
        TestSecurityClient userAuthSecurityClient = new TestSecurityClient(
            adminClient(),
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader).build()
        );
        userAuthSecurityClient.putRole(descriptor);
    }

    private void createUser(String username, Set<String> roles) throws IOException {
        adminSecurityClient.putUser(new User(username, roles.toArray(String[]::new)), TEST_PASSWORD);
    }

    private void createManageRolesRole(String roleName, String[] clusterPrivileges, Set<String> indexPatterns) throws IOException {
        createManageRolesRole(roleName, clusterPrivileges, indexPatterns, Set.of("read"));
    }

    private void createManageRolesRole(String roleName, String[] clusterPrivileges, Set<String> indexPatterns, Set<String> privileges)
        throws IOException {
        adminSecurityClient.putRole(
            new RoleDescriptor(
                roleName,
                clusterPrivileges,
                new RoleDescriptor.IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                new ConfigurableClusterPrivilege[] {
                    new ConfigurableClusterPrivileges.ManageRolesPrivilege(
                        List.of(
                            new ConfigurableClusterPrivileges.ManageRolesPrivilege.ManageRolesIndexPermissionGroup(
                                indexPatterns.toArray(String[]::new),
                                privileges.toArray(String[]::new)
                            )
                        )
                    ) },
                new String[0],
                Map.of(),
                Map.of()
            )
        );
    }
}
