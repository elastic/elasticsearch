/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class RoleReferenceTests extends ESTestCase {

    public void testNamedRoleReference() {
        final String[] roleNames = randomArray(0, 2, String[]::new, () -> randomAlphaOfLength(8));

        final boolean hasSuperUserRole = roleNames.length > 0 && randomBoolean();
        if (hasSuperUserRole) {
            roleNames[randomIntBetween(0, roleNames.length - 1)] = "superuser";
        }
        final RoleReference.NamedRoleReference namedRoleReference = new RoleReference.NamedRoleReference(roleNames);

        if (hasSuperUserRole) {
            assertThat(namedRoleReference.id(), is(RoleKey.ROLE_KEY_SUPERUSER));
        } else if (roleNames.length == 0) {
            assertThat(namedRoleReference.id(), is(RoleKey.ROLE_KEY_EMPTY));
        } else {
            final RoleKey roleKey = namedRoleReference.id();
            assertThat(roleKey.getNames(), equalTo(Set.of(roleNames)));
            assertThat(roleKey.getSource(), equalTo(RoleKey.ROLES_STORE_SOURCE));
        }
    }

    public void testApiKeyRoleReference() {
        final String apiKeyId = randomAlphaOfLength(20);
        final BytesArray roleDescriptorsBytes = new BytesArray(randomAlphaOfLength(50));
        final String roleKeySource = randomAlphaOfLength(8);
        final RoleReference.ApiKeyRoleReference apiKeyRoleReference = new RoleReference.ApiKeyRoleReference(
            apiKeyId,
            roleDescriptorsBytes,
            roleKeySource
        );

        final RoleKey roleKey = apiKeyRoleReference.id();
        assertThat(
            roleKey.getNames(),
            hasItem("apikey:" + MessageDigests.toHexString(MessageDigests.digest(roleDescriptorsBytes, MessageDigests.sha256())))
        );
        assertThat(roleKey.getSource(), equalTo(roleKeySource));
    }

    public void testServiceAccountRoleReference() {
        final String principal = randomAlphaOfLength(8) + "/" + randomAlphaOfLength(8);
        final RoleReference.ServiceAccountRoleReference serviceAccountRoleReference = new RoleReference.ServiceAccountRoleReference(
            principal
        );
        final RoleKey roleKey = serviceAccountRoleReference.id();
        assertThat(roleKey.getNames(), hasItem(principal));
        assertThat(roleKey.getSource(), equalTo("service_account"));
    }
}
