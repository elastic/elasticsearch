/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

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
                    new ApplicationPrivilege(
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
                    new ApplicationPrivilege(
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
}
