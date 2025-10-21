/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.client.Request;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;

public class RoleWithDescriptionRestIT extends SecurityOnTrialLicenseRestTestCase {

    public void testCreateOrUpdateRoleWithDescription() throws Exception {
        final String roleName = "role_with_description";
        final String initialRoleDescription = randomAlphaOfLengthBetween(0, 10);
        {
            upsertRole(Strings.format("""
                {
                  "description": "%s",
                  "cluster": ["all"],
                  "indices": [{"names": ["*"], "privileges": ["all"]}]
                }""", initialRoleDescription), roleName);

            fetchRoleAndAssertEqualsExpected(
                roleName,
                new RoleDescriptor(
                    roleName,
                    new String[] { "all" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build() },
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    initialRoleDescription
                )
            );
        }
        {
            final String newRoleDescription = randomValueOtherThan(initialRoleDescription, () -> randomAlphaOfLengthBetween(0, 10));
            upsertRole(Strings.format("""
                {
                  "description": "%s",
                  "cluster": ["all"],
                  "indices": [{"names": ["index-*"], "privileges": ["all"]}]
                }""", newRoleDescription), roleName);

            fetchRoleAndAssertEqualsExpected(
                roleName,
                new RoleDescriptor(
                    roleName,
                    new String[] { "all" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("index-*").privileges("all").build() },
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    newRoleDescription
                )
            );
        }
    }

    public void testCreateRoleWithInvalidDescriptionFails() throws IOException {
        Request request = roleRequest(Strings.format("""
            {
              "description": "%s",
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["all"]}]
            }""", randomAlphaOfLength(Validation.Roles.MAX_DESCRIPTION_LENGTH + randomIntBetween(1, 5))), "role_with_large_description");

        assertSendRequestThrowsError(
            request,
            "Role description must be less than " + Validation.Roles.MAX_DESCRIPTION_LENGTH + " characters."
        );
    }

    public void testUpdateRoleWithInvalidDescriptionFails() throws IOException {
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["all"]}]
            }""", "my_role");

        Request updateRoleRequest = roleRequest(Strings.format("""
            {
              "description": "%s",
              "cluster": ["all"],
              "indices": [{"names": ["index-*"], "privileges": ["all"]}]
            }""", randomAlphaOfLength(Validation.Roles.MAX_DESCRIPTION_LENGTH + randomIntBetween(1, 5))), "my_role");

        assertSendRequestThrowsError(
            updateRoleRequest,
            "Role description must be less than " + Validation.Roles.MAX_DESCRIPTION_LENGTH + " characters."
        );
    }
}
