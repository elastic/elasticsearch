/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class QueryableReservedRolesProviderTests extends ESTestCase {

    public void testReservedRoleProvider() {
        QueryableReservedRolesProvider provider = new QueryableReservedRolesProvider(new ReservedRolesStore());
        assertNotNull(provider.getRoles());
        assertThat(provider.getRoles(), equalTo(provider.getRoles()));
        assertThat(provider.getRoles().rolesDigest().size(), equalTo(ReservedRolesStore.roleDescriptors().size()));
        assertThat(
            provider.getRoles().rolesDigest().keySet(),
            equalTo(ReservedRolesStore.roleDescriptors().stream().map(RoleDescriptor::getName).collect(Collectors.toSet()))
        );
    }

}
