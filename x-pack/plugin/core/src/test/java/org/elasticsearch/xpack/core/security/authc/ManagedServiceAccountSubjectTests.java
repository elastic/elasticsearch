/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ManagedServiceAccountSubjectTests extends ESTestCase {

    public void testBuiltInServiceAccountUsesFixedRoleReference() {
        final User user = new User(
            "elastic/kibana",
            new String[0],
            "Service account - elastic/kibana",
            null,
            Map.of(ServiceAccountSettings.BUILTIN_SERVICE_ACCOUNT_FIELD, true),
            true
        );
        final Subject subject = new Subject(user, Authentication.RealmRef.newServiceAccountRealmRef("node"));
        final RoleReference roleReference = subject.getRoleReferenceIntersection(null).getRoleReferences().iterator().next();
        assertThat(roleReference, instanceOf(RoleReference.ServiceAccountRoleReference.class));
    }

    public void testManagedServiceAccountUsesNamedRoleReference() {
        final User user = new User(
            "custom/my-service",
            new String[] { "role-a", "role-b" },
            "Managed service account - custom/my-service",
            null,
            Map.of(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD, true),
            true
        );
        final Subject subject = new Subject(user, Authentication.RealmRef.newServiceAccountRealmRef("node"));
        final RoleReference roleReference = subject.getRoleReferenceIntersection(null).getRoleReferences().iterator().next();
        assertThat(roleReference, instanceOf(RoleReference.NamedRoleReference.class));
        assertThat(((RoleReference.NamedRoleReference) roleReference).getRoleNames(), equalTo(new String[] { "role-a", "role-b" }));
    }

    public void testManagedServiceAccountWithNoRolesUsesEmptyNamedRoleReference() {
        final User user = new User(
            "custom/empty",
            new String[0],
            "Managed service account - custom/empty",
            null,
            Map.of(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD, true),
            true
        );
        final Subject subject = new Subject(user, Authentication.RealmRef.newServiceAccountRealmRef("node"));
        final RoleReference roleReference = subject.getRoleReferenceIntersection(null).getRoleReferences().iterator().next();
        assertThat(roleReference, instanceOf(RoleReference.NamedRoleReference.class));
        assertThat(((RoleReference.NamedRoleReference) roleReference).getRoleNames(), equalTo(new String[0]));
    }
}
