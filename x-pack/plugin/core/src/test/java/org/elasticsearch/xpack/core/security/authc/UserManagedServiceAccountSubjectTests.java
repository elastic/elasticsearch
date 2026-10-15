/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Covers how a service account {@link Subject} decides where its privileges come from. Built-in accounts resolve a role
 * descriptor fixed by the account definition, whereas user-managed accounts resolve the named roles assigned to them.
 * The two are told apart solely by a marker on the authenticated user, so these tests pin the routing for a marked
 * account, an unmarked one, and a subject that carries the marker without being a service account at all.
 */
public class UserManagedServiceAccountSubjectTests extends ESTestCase {

    public void testBuiltInServiceAccountResolvesPrivilegesFromItsPrincipal() {
        final Subject subject = serviceAccountSubject(
            "elastic/kibana",
            Strings.EMPTY_ARRAY,
            Map.of(ServiceAccountSettings.BUILTIN_SERVICE_ACCOUNT_FIELD, true)
        );

        assertThat(subject.isUserManagedServiceAccount(), is(false));
        final RoleReference roleReference = singleRoleReference(subject);
        assertThat(roleReference, instanceOf(RoleReference.ServiceAccountRoleReference.class));
        assertThat(((RoleReference.ServiceAccountRoleReference) roleReference).getPrincipal(), equalTo("elastic/kibana"));
    }

    /**
     * The shape of an authentication serialized by a node that predates user-managed service accounts: no marker at all.
     * It must keep resolving privileges from the principal rather than falling through to the user-managed branch.
     */
    public void testServiceAccountWithoutAnyMarkerResolvesPrivilegesFromItsPrincipal() {
        final Subject subject = serviceAccountSubject("elastic/fleet-server", Strings.EMPTY_ARRAY, Map.of());

        assertThat(subject.isUserManagedServiceAccount(), is(false));
        final RoleReference roleReference = singleRoleReference(subject);
        assertThat(roleReference, instanceOf(RoleReference.ServiceAccountRoleReference.class));
        assertThat(((RoleReference.ServiceAccountRoleReference) roleReference).getPrincipal(), equalTo("elastic/fleet-server"));
    }

    public void testUserManagedServiceAccountResolvesItsAssignedRoles() {
        final Subject subject = serviceAccountSubject(
            "my-namespace/my-service",
            new String[] { "role-a", "role-b" },
            Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true)
        );

        assertThat(subject.isUserManagedServiceAccount(), is(true));
        final RoleReference roleReference = singleRoleReference(subject);
        assertThat(roleReference, instanceOf(RoleReference.NamedRoleReference.class));
        assertThat(((RoleReference.NamedRoleReference) roleReference).getRoleNames(), arrayContaining("role-a", "role-b"));
    }

    /**
     * An account with no roles must end up with no privileges. Asserting the role key rather than only the reference type
     * is what pins that: {@link RoleKey#ROLE_KEY_EMPTY} is the key the role stores resolve to the empty role.
     */
    public void testUserManagedServiceAccountWithoutRolesResolvesToNoPrivileges() {
        final Subject subject = serviceAccountSubject(
            "my-namespace/my-service",
            Strings.EMPTY_ARRAY,
            Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true)
        );

        final RoleReference roleReference = singleRoleReference(subject);
        assertThat(roleReference, instanceOf(RoleReference.NamedRoleReference.class));
        assertThat(((RoleReference.NamedRoleReference) roleReference).getRoleNames(), emptyArray());
        assertThat(roleReference.id(), equalTo(RoleKey.ROLE_KEY_EMPTY));
    }

    /**
     * The marker only means anything on a subject that authenticated through the service account realm. A realm user
     * whose metadata happens to carry the key must not be mistaken for a service account by callers that branch on it.
     */
    public void testMarkerIsIgnoredOnSubjectsThatAreNotServiceAccounts() {
        final User user = new User(
            "not-a-service-account",
            new String[] { "role-a" },
            null,
            null,
            Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true),
            true
        );
        final Subject subject = new Subject(user, new Authentication.RealmRef("native", "native", "node"));

        assertThat(subject.getType(), equalTo(Subject.Type.USER));
        assertThat(subject.isUserManagedServiceAccount(), is(false));
    }

    private static Subject serviceAccountSubject(String principal, String[] roles, Map<String, Object> metadata) {
        final User user = new User(principal, roles, "Service account - " + principal, null, metadata, true);
        return new Subject(user, Authentication.RealmRef.newServiceAccountRealmRef("node"));
    }

    private static RoleReference singleRoleReference(Subject subject) {
        final List<RoleReference> roleReferences = subject.getRoleReferenceIntersection(null).getRoleReferences();
        assertThat(roleReferences, hasSize(1));
        return roleReferences.get(0);
    }
}
