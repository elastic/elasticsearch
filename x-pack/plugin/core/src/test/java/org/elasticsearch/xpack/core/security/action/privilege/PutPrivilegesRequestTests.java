/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class PutPrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final PutPrivilegesRequest original = request(randomArray(8, ApplicationPrivilege[]::new, () -> new ApplicationPrivilege(
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT),
                randomArray(3, String[]::new, () -> randomAlphaOfLength(3).toLowerCase(Locale.ROOT) + "/*")
            )
        ));
        original.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final PutPrivilegesRequest copy = new PutPrivilegesRequest();
        copy.readFrom(out.bytes().streamInput());

        assertThat(original.getPrivileges(), Matchers.equalTo(copy.getPrivileges()));
        assertThat(original.getRefreshPolicy(), Matchers.equalTo(copy.getRefreshPolicy()));
    }

    public void testValidation() {
        // wildcard app name
        final ApplicationPrivilege wildcardApp = new ApplicationPrivilege("*", "all", "*");
        assertValidationFailure(request(wildcardApp), "Application names must match");

        // multiple names
        final ApplicationPrivilege read = new ApplicationPrivilege("app", "read", "read/*");
        final ApplicationPrivilege write = new ApplicationPrivilege("app", "write", "write/*");
        final ApplicationPrivilege multiName = ApplicationPrivilege.get("app",
            Sets.newHashSet("read", "write"), Sets.newHashSet(read, write));
        assertThat(multiName.name(), iterableWithSize(2));
        assertValidationFailure(request(multiName), "must have a single name");

        // reserved metadata
        final ApplicationPrivilege reservedMetadata = new ApplicationPrivilege("app", "all", Collections.emptyList(),
            Collections.singletonMap("_foo", "var"));
        assertValidationFailure(request(reservedMetadata), "metadata keys may not start");

        // mixed
        assertValidationFailure(request(wildcardApp, multiName, reservedMetadata),
            "Application names must match", "must have a single name", "metadata keys may not start");
    }

    private ApplicationPrivilege reservedMetadata() {
        return new ApplicationPrivilege("app", "all", Collections.emptyList(),
            Collections.singletonMap("_foo", "var"));
    }

    private void assertValidationFailure(PutPrivilegesRequest request, String... messages) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        for (String message : messages) {
            assertThat(exception.validationErrors(), hasItem(containsString(message)));
        }
    }

    private PutPrivilegesRequest request(ApplicationPrivilege... privileges) {
        final PutPrivilegesRequest original = new PutPrivilegesRequest();

        original.setPrivileges(Arrays.asList(privileges));
        return original;
    }
}
