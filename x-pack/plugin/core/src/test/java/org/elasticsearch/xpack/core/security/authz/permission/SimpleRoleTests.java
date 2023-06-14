/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SimpleRoleTests extends ESTestCase {

    public void testEmptyRoleHasNoEmptyListOfNames() {
        assertThat(Role.EMPTY.names(), emptyArray());
    }

    public void testHasPrivilegesCache() throws ExecutionException {
        final SimpleRole role = Role.buildFromRoleDescriptor(
            new RoleDescriptor(randomAlphaOfLengthBetween(3, 8), new String[] { "monitor" }, null, null),
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );

        // cache is null to begin with
        assertThat(role.getHasPrivilegesCache(), nullValue());

        final AuthorizationEngine.PrivilegesToCheck privilegesToCheck = new AuthorizationEngine.PrivilegesToCheck(
            new String[] { "monitor" },
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[0],
            true
        );
        final AuthorizationEngine.PrivilegesCheckResult privilegesCheckResult = new AuthorizationEngine.PrivilegesCheckResult(
            true,
            new AuthorizationEngine.PrivilegesCheckResult.Details(Map.of("monitor", true), Map.of(), Map.of())
        );
        role.cacheHasPrivileges(Settings.EMPTY, privilegesToCheck, privilegesCheckResult);

        assertThat(role.getHasPrivilegesCache(), notNullValue());
        assertThat(role.getHasPrivilegesCache().count(), equalTo(1));
        assertThat(role.getHasPrivilegesCache().get(privilegesToCheck), equalTo(privilegesCheckResult));

        assertThat(role.checkPrivilegesWithCache(privilegesToCheck), equalTo(privilegesCheckResult));
    }

    public void testBuildFromRoleDescriptorWithApplicationPrivileges() {
        final boolean wildcardApplication = randomBoolean();
        final boolean wildcardPrivileges = randomBoolean();
        final boolean wildcardResources = randomBoolean();
        final RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege = RoleDescriptor.ApplicationResourcePrivileges.builder()
            .application(wildcardApplication ? "*" : randomAlphaOfLengthBetween(5, 12))
            // concrete privileges need to be prefixed with lower case letters to be considered valid, so use "app"
            .privileges(wildcardPrivileges ? "*" : "app" + randomAlphaOfLengthBetween(5, 12))
            .resources(wildcardResources ? new String[] { "*" } : generateRandomStringArray(6, randomIntBetween(4, 8), false, false))
            .build();

        final String allowedApplicationActionPattern = randomAlphaOfLengthBetween(5, 12);
        final SimpleRole role = Role.buildFromRoleDescriptor(
            new RoleDescriptor(
                "r1",
                null,
                null,
                new RoleDescriptor.ApplicationResourcePrivileges[] { applicationPrivilege },
                null,
                null,
                null,
                null
            ),
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            wildcardPrivileges
                ? List.of()
                : List.of(
                    new ApplicationPrivilegeDescriptor(
                        applicationPrivilege.getApplication(),
                        Arrays.stream(applicationPrivilege.getPrivileges()).iterator().next(),
                        Set.of(allowedApplicationActionPattern),
                        Map.of()
                    )
                )
        );
        assertThat(
            "expected grant for role with application privilege to be: " + applicationPrivilege,
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        wildcardApplication ? randomAlphaOfLengthBetween(1, 10) : applicationPrivilege.getApplication(),
                        wildcardPrivileges ? Set.of(randomAlphaOfLengthBetween(1, 10)) : Set.of(applicationPrivilege.getPrivileges()),
                        wildcardPrivileges ? randomAlphaOfLengthBetween(1, 10) : allowedApplicationActionPattern
                    ),
                    wildcardResources ? randomAlphaOfLengthBetween(1, 10) : randomFrom(applicationPrivilege.getResources())
                ),
            is(true)
        );
        // This gives decent but not complete coverage of denial cases; for any non-wildcard field we pick a mismatched value to force a
        // denial
        assertThat(
            "expected grant for role with application privilege to be: " + applicationPrivilege,
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        false == wildcardApplication
                            ? randomValueOtherThan(applicationPrivilege.getApplication(), () -> randomAlphaOfLengthBetween(1, 10))
                            : randomAlphaOfLengthBetween(1, 10),
                        false == wildcardPrivileges
                            ? randomValueOtherThan(
                                Set.of(applicationPrivilege.getPrivileges()),
                                () -> Set.of(randomAlphaOfLengthBetween(1, 10))
                            )
                            : Set.of(randomAlphaOfLengthBetween(1, 10)),
                        false == wildcardPrivileges
                            ? randomValueOtherThan(allowedApplicationActionPattern, () -> randomAlphaOfLengthBetween(1, 10))
                            : randomAlphaOfLengthBetween(1, 10)
                    ),
                    false == wildcardResources
                        ? randomValueOtherThanMany(
                            it -> List.of(applicationPrivilege.getResources()).contains(it),
                            () -> randomAlphaOfLengthBetween(1, 10)
                        )
                        : randomAlphaOfLengthBetween(1, 10)
                ),
            // If all are wildcards, then we necessarily get a grant, otherwise expect a denial
            is(wildcardApplication && wildcardPrivileges && wildcardResources)
        );
    }

    public void testGetRoleDescriptorsIntersectionForRemoteCluster() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        SimpleRole role = Role.builder(RESTRICTED_INDICES, randomAlphaOfLength(6))
            .addRemoteGroup(
                Set.of("remote-cluster-a"),
                FieldPermissions.DEFAULT,
                null,
                IndexPrivilege.READ,
                true,
                "remote-index-a-1",
                "remote-index-a-2"
            )
            .addRemoteGroup(Set.of("remote-*-a"), FieldPermissions.DEFAULT, null, IndexPrivilege.READ, false, "remote-index-a-3")
            // This privilege should be ignored
            .addRemoteGroup(
                Set.of("remote-cluster-b"),
                FieldPermissions.DEFAULT,
                null,
                IndexPrivilege.READ,
                false,
                "remote-index-b-1",
                "remote-index-b-2"
            )
            // This privilege should be ignored
            .addRemoteGroup(
                Set.of(randomAlphaOfLength(8)),
                new FieldPermissions(new FieldPermissionsDefinition(new String[] { randomAlphaOfLength(5) }, null)),
                null,
                IndexPrivilege.get(Set.of(randomFrom(IndexPrivilege.names()))),
                randomBoolean(),
                randomAlphaOfLength(9)
            )
            .build();

        RoleDescriptorsIntersection intersection = role.getRoleDescriptorsIntersectionForRemoteCluster("remote-cluster-a");

        assertThat(intersection.roleDescriptorsList().isEmpty(), equalTo(false));
        assertThat(
            intersection,
            equalTo(
                new RoleDescriptorsIntersection(
                    new RoleDescriptor(
                        Role.REMOTE_USER_ROLE_NAME,
                        null,
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .privileges(IndexPrivilege.READ.name())
                                .indices("remote-index-a-3")
                                .allowRestrictedIndices(false)
                                .build(),
                            RoleDescriptor.IndicesPrivileges.builder()
                                .privileges(IndexPrivilege.READ.name())
                                .indices("remote-index-a-1", "remote-index-a-2")
                                .allowRestrictedIndices(true)
                                .build() },
                        null,
                        null,
                        null,
                        null,
                        null
                    )
                )
            )
        );

        // Requesting role descriptors intersection for a cluster alias
        // that has no cross cluster access defined should result in an empty intersection.
        assertThat(
            role.getRoleDescriptorsIntersectionForRemoteCluster("non-existing-cluster-alias"),
            equalTo(RoleDescriptorsIntersection.EMPTY)
        );
    }

    public void testGetRoleDescriptorsIntersectionForRemoteClusterWithoutRemoteIndicesPermissions() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final SimpleRole role = Role.buildFromRoleDescriptor(
            new RoleDescriptor(
                randomAlphaOfLengthBetween(3, 8),
                new String[] { randomFrom(ClusterPrivilegeResolver.names()) },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder()
                        .privileges(randomFrom(IndexPrivilege.names()))
                        .indices(randomAlphaOfLengthBetween(4, 6), randomAlphaOfLengthBetween(4, 6))
                        .allowRestrictedIndices(randomBoolean())
                        .grantedFields(randomAlphaOfLength(4))
                        .build() },
                new String[] { randomAlphaOfLength(7) }
            ),
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );

        assertThat(role.getRoleDescriptorsIntersectionForRemoteCluster(randomAlphaOfLength(8)), equalTo(RoleDescriptorsIntersection.EMPTY));
    }

    public void testBuildFromRoleDescriptorWithWorkflowsRestriction() {
        final SimpleRole role = Role.buildFromRoleDescriptor(
            new RoleDescriptor(
                "r1",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new RoleDescriptor.Restriction(new String[] { WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW.name() })
            ),
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of()
        );

        assertThat(role.hasWorkflowsRestriction(), equalTo(true));
    }
}
