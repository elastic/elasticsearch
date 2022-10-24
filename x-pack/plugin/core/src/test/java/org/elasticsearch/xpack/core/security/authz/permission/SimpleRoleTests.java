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

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
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

}
