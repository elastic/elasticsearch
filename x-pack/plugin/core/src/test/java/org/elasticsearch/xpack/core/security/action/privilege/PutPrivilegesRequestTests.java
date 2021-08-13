/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class PutPrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final PutPrivilegesRequest original = request(randomArray(8, ApplicationPrivilegeDescriptor[]::new,
            () -> new ApplicationPrivilegeDescriptor(
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                Sets.newHashSet(randomArray(3, String[]::new, () -> randomAlphaOfLength(3).toLowerCase(Locale.ROOT) + "/*")),
                Collections.emptyMap()
            )
        ));
        original.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final PutPrivilegesRequest copy = new PutPrivilegesRequest(out.bytes().streamInput());

        assertThat(original.getPrivileges(), Matchers.equalTo(copy.getPrivileges()));
        assertThat(original.getRefreshPolicy(), Matchers.equalTo(copy.getRefreshPolicy()));
    }

    public void testValidation() {
        // wildcard app name
        final ApplicationPrivilegeDescriptor wildcardApp = descriptor("*", "all", "*");
        assertValidationFailure(request(wildcardApp), "Application names may not contain");

        // invalid priv names
        final ApplicationPrivilegeDescriptor spaceName = descriptor("app", "r e a d", "read/*");
        final ApplicationPrivilegeDescriptor numericName = descriptor("app", "7346", "read/*");
        assertValidationFailure(request(spaceName), "Application privilege names must match");
        assertValidationFailure(request(numericName), "Application privilege names must match");

        // no actions
        final ApplicationPrivilegeDescriptor nothing = descriptor("*", "nothing");
        assertValidationFailure(request(nothing), "Application privileges must have at least one action");

        // reserved metadata
        final ApplicationPrivilegeDescriptor reservedMetadata = new ApplicationPrivilegeDescriptor("app", "all",
            Collections.emptySet(), Collections.singletonMap("_notAllowed", true)
        );
        assertValidationFailure(request(reservedMetadata), "metadata keys may not start");

        ApplicationPrivilegeDescriptor badAction = descriptor("app", "foo", randomFrom("data.read", "data_read", "data+read", "read"));
        assertValidationFailure(request(badAction), "must contain one of");

        // mixed
        assertValidationFailure(request(wildcardApp, numericName, reservedMetadata, badAction),
            "Application names may not contain", "Application privilege names must match", "metadata keys may not start",
            "must contain one of");

        // Empty request
        assertValidationFailure(new PutPrivilegesRequest(), "At least one application privilege must be provided");
    }

    private ApplicationPrivilegeDescriptor descriptor(String application, String name, String... actions) {
        return new ApplicationPrivilegeDescriptor(application, name, Sets.newHashSet(actions), Collections.emptyMap());
    }

    private void assertValidationFailure(PutPrivilegesRequest request, String... messages) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        for (String message : messages) {
            assertThat(exception.validationErrors(), hasItem(containsString(message)));
        }
    }

    private PutPrivilegesRequest request(ApplicationPrivilegeDescriptor... privileges) {
        final PutPrivilegesRequest original = new PutPrivilegesRequest();

        original.setPrivileges(Arrays.asList(privileges));
        return original;
    }
}
