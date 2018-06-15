/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutRoleRequestTests extends ESTestCase {

    public void testValidationOfApplicationPrivileges() {
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[]{"read"}, new String[]{"*"}));
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[]{"action:login"}, new String[]{"/"}));
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("*", new String[]{"data/read:user"}, new String[]{"user/123"}));

        // Fail
        assertValidationError("privilege names and actions must match the pattern",
            buildRequestWithApplicationPrivilege("app", new String[]{"in valid"}, new String[]{"*"}));
        assertValidationError("Application names must match the pattern",
            buildRequestWithApplicationPrivilege("000", new String[]{"all"}, new String[]{"*"}));
        assertValidationError("Application names must match the pattern",
            buildRequestWithApplicationPrivilege("%*", new String[]{"all"}, new String[]{"*"}));
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
        request.addApplicationPrivileges(new ApplicationResourcePrivileges[]{privilege});
        return request;
    }
}
