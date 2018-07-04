/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    public void testSerialization() throws IOException {
        final PutRoleRequest original = buildRandomRequest();

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final PutRoleRequest copy = new PutRoleRequest();
        copy.readFrom(out.bytes().streamInput());

        assertThat(copy.roleDescriptor(), equalTo(original.roleDescriptor()));
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

    private PutRoleRequest buildRandomRequest() {

        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));

        request.cluster(randomSubsetOf(Arrays.asList("monitor", "manage", "all", "manage_security", "manage_ml", "monitor_watcher"))
            .toArray(Strings.EMPTY_ARRAY));

        for (int i = randomIntBetween(0, 4); i > 0; i--) {
            request.addIndex(
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), false, false),
                randomSubsetOf(randomIntBetween(1, 2), "read", "write", "index", "all").toArray(Strings.EMPTY_ARRAY),
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), true),
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), true),
                null
            );
        }

        final Supplier<String> stringWithInitialLowercase = ()
            -> randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 12);
        final ApplicationResourcePrivileges[] applicationPrivileges = new ApplicationResourcePrivileges[randomIntBetween(0, 5)];
        for (int i = 0; i < applicationPrivileges.length; i++) {
            applicationPrivileges[i] = ApplicationResourcePrivileges.builder()
                .application(stringWithInitialLowercase.get())
                .privileges(randomArray(1, 3, String[]::new, stringWithInitialLowercase))
                .resources(generateRandomStringArray(5, randomIntBetween(3, 8), false, false))
                .build();
        }
        request.addApplicationPrivileges(applicationPrivileges);

        request.runAs(generateRandomStringArray(4, 3, false, true));

        final Map<String, Object> metadata = new HashMap<>();
        for (String key : generateRandomStringArray(3, 5, false, true)) {
            metadata.put(key, randomFrom(Boolean.TRUE, Boolean.FALSE, 1, 2, randomAlphaOfLengthBetween(2, 9)));
        }
        request.metadata(metadata);

        request.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        return request;
    }
}
