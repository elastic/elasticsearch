/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.service.AuthorizedServiceAccounts;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.LimitedRole;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageServiceAccountsPrivilege;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AuthorizedServiceAccountsResolverTests extends ESTestCase {

    public void testGrantsOfOneRoleFormOneGroup() {
        final Role role = role(
            new String[0],
            new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1")),
            new ManageServiceAccountsPrivilege(Set.of("batch/nightly"), Set.of("r2"))
        );
        final AuthorizedServiceAccounts accounts = resolveGet(role);
        assertThat(accounts.patternGroups(), equalTo(List.of(Set.of("apps/*", "batch/nightly"))));
    }

    public void testRoleWithoutGrantsResolvesNothing() {
        assertThat(resolveGet(role(new String[] { "monitor" })), nullValue());
    }

    /**
     * An API key grants only what both its own roles and its creator's grant. When both hold grants, each is a group,
     * so only accounts both cover are readable.
     */
    public void testLimitedRoleIntersectsTheGrantsOfBothRoles() {
        final Role role = new LimitedRole(
            role(new String[0], new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1"))),
            role(new String[0], new ManageServiceAccountsPrivilege(Set.of("*/payroll"), Set.of("r1")))
        );
        final AuthorizedServiceAccounts accounts = resolveQuery(role);
        assertThat(accounts.patternGroups(), containsInAnyOrder(Set.of("apps/*"), Set.of("*/payroll")));
        assertThat(accounts.test("apps/payroll"), equalTo(true));
        assertThat(accounts.test("apps/billing"), equalTo(false));
    }

    /**
     * A creator who may read every account does not restrict their API key, so only the key's own grants count.
     */
    public void testLimitedRoleIgnoresARoleThatReadsEveryAccount() {
        final Role role = new LimitedRole(
            role(new String[0], new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1"))),
            role(new String[] { "manage_security" })
        );
        assertThat(resolveGet(role).patternGroups(), equalTo(List.of(Set.of("apps/*"))));
    }

    /**
     * An API key cannot read more than its creator, so a key holding a grant whose creator holds none reads nothing.
     */
    public void testLimitedRoleResolvesNothingWhenEitherRoleHasNoGrant() {
        final Role keyWithGrant = new LimitedRole(
            role(new String[0], new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1"))),
            role(new String[] { "monitor" })
        );
        assertThat(resolveGet(keyWithGrant), nullValue());

        final Role creatorWithGrant = new LimitedRole(
            role(new String[] { "monitor" }),
            role(new String[0], new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1")))
        );
        assertThat(resolveGet(creatorWithGrant), nullValue());
    }

    private static AuthorizedServiceAccounts resolveGet(Role role) {
        return AuthorizedServiceAccountsResolver.resolve(
            role,
            GetServiceAccountAction.NAME,
            new GetServiceAccountRequest(null, null, EnumSet.allOf(ServiceAccountType.class)),
            authentication()
        );
    }

    private static AuthorizedServiceAccounts resolveQuery(Role role) {
        return AuthorizedServiceAccountsResolver.resolve(
            role,
            QueryServiceAccountAction.NAME,
            new QueryServiceAccountRequest(null, null, null, null, null),
            authentication()
        );
    }

    private static Authentication authentication() {
        return AuthenticationTestHelper.builder().build();
    }

    private static Role role(String[] clusterPrivileges, ConfigurableClusterPrivilege... configurableClusterPrivileges) {
        final RoleDescriptor descriptor = new RoleDescriptor(
            randomAlphaOfLength(8),
            clusterPrivileges,
            null,
            null,
            configurableClusterPrivileges,
            null,
            null,
            null
        );
        return Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
    }
}
