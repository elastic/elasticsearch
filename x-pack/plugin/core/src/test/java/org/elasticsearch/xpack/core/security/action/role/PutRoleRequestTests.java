/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.junit.BeforeClass;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutRoleRequestTests extends ESTestCase {

    @BeforeClass
    public static void setUpClass() {
        // Initialize the reserved roles store so that static fields are populated.
        // In production code, this is guaranteed by how components are initialized by the Security plugin
        new ReservedRolesStore();
    }

    public void testValidationErrorWithUnknownClusterPrivilegeName() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        String unknownClusterPrivilegeName = "unknown_" + randomAlphaOfLengthBetween(3, 9);
        request.cluster("manage_security", unknownClusterPrivilegeName);

        // Fail
        assertValidationError("unknown cluster privilege [" + unknownClusterPrivilegeName.toLowerCase(Locale.ROOT) + "]", request);
    }

    public void testValidationErrorWithTooLongRoleName() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(
            randomAlphaOfLengthBetween(NativeRealmValidationUtil.MAX_NAME_LENGTH + 1, NativeRealmValidationUtil.MAX_NAME_LENGTH * 2)
        );
        request.cluster("manage_security");

        // Fail
        assertValidationError("Role names must be at least 1 and no more than " + NativeRealmValidationUtil.MAX_NAME_LENGTH, request);
    }

    public void testValidationSuccessWithCorrectClusterPrivilegeName() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        request.cluster("manage_security", "manage", "cluster:admin/xpack/security/*");
        assertSuccessfulValidation(request);
    }

    public void testValidationErrorWithUnknownIndexPrivilegeName() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        String unknownIndexPrivilegeName = "unknown_" + randomAlphaOfLengthBetween(3, 9);
        request.addIndex(
            new String[] { randomAlphaOfLength(5) },
            new String[] { "index", unknownIndexPrivilegeName },
            null,
            null,
            null,
            randomBoolean()
        );

        // Fail
        assertValidationError("unknown index privilege [" + unknownIndexPrivilegeName.toLowerCase(Locale.ROOT) + "]", request);
    }

    public void testValidationErrorWithEmptyClustersInRemoteIndices() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        request.addRemoteIndex(
            new String[] { randomAlphaOfLength(5), "" },
            new String[] { randomAlphaOfLength(5) },
            new String[] { "index", "write", "indices:data/read" },
            null,
            null,
            null,
            randomBoolean()
        );
        assertValidationError("remote index cluster alias cannot be an empty string", request);
    }

    public void testValidationSuccessWithCorrectRemoteIndexPrivilegeClusters() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        if (randomBoolean()) {
            request.addRemoteIndex(
                new String[] { randomAlphaOfLength(5), "*", "* " },
                new String[] { randomAlphaOfLength(5) },
                new String[] { "index", "write", "indices:data/read" },
                null,
                null,
                null,
                randomBoolean()
            );
        } else {
            // Empty remote index section is valid
            request.addRemoteIndex();
        }
        assertSuccessfulValidation(request);
    }

    public void testValidationSuccessWithCorrectIndexPrivilegeName() {
        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));
        request.addIndex(
            new String[] { randomAlphaOfLength(5) },
            new String[] { "index", "write", "indices:data/read" },
            null,
            null,
            null,
            randomBoolean()
        );
        assertSuccessfulValidation(request);
    }

    public void testValidationOfApplicationPrivileges() {
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[] { "read" }, new String[] { "*" }));
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[] { "action:login" }, new String[] { "/" }));
        assertSuccessfulValidation(
            buildRequestWithApplicationPrivilege("*", new String[] { "data/read:user" }, new String[] { "user/123" })
        );

        // Fail
        assertValidationError(
            "privilege names and actions must match the pattern",
            buildRequestWithApplicationPrivilege("app", new String[] { "in valid" }, new String[] { "*" })
        );
        assertValidationError(
            "An application name prefix must match the pattern",
            buildRequestWithApplicationPrivilege("000", new String[] { "all" }, new String[] { "*" })
        );
        assertValidationError(
            "An application name prefix must match the pattern",
            buildRequestWithApplicationPrivilege("%*", new String[] { "all" }, new String[] { "*" })
        );
    }

    public void testSetRefreshPolicy() {
        final PutRoleRequest request = new PutRoleRequest();
        final String refreshPolicy = randomFrom(
            WriteRequest.RefreshPolicy.IMMEDIATE.getValue(),
            WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue()
        );
        request.setRefreshPolicy(refreshPolicy);
        assertThat(request.getRefreshPolicy().getValue(), equalTo(refreshPolicy));

        request.setRefreshPolicy((String) null);
        assertThat(request.getRefreshPolicy().getValue(), equalTo(refreshPolicy));
    }

    private void assertSuccessfulValidation(PutRoleRequest request) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, nullValue());
    }

    private void assertValidationError(String message, PutRoleRequest request) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem(containsString(message)));
    }

    private PutRoleRequest buildRequestWithApplicationPrivilege(String appName, String[] privileges, String[] resources) {
        final PutRoleRequest request = new PutRoleRequest();
        request.name("test");
        final ApplicationResourcePrivileges privilege = ApplicationResourcePrivileges.builder()
            .application(appName)
            .privileges(privileges)
            .resources(resources)
            .build();
        request.addApplicationPrivileges(privilege);
        return request;
    }
}
