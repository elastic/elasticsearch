/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeletePrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final DeletePrivilegesRequest original = new DeletePrivilegesRequest(
            randomAlphaOfLengthBetween(3, 8), generateRandomStringArray(5, randomIntBetween(3, 8), false, false));
        original.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));

        final BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);
        output.flush();
        final DeletePrivilegesRequest copy = new DeletePrivilegesRequest(output.bytes().streamInput());
        assertThat(copy.application(), equalTo(original.application()));
        assertThat(copy.privileges(), equalTo(original.privileges()));
        assertThat(copy.getRefreshPolicy(), equalTo(original.getRefreshPolicy()));
    }

    public void testValidation() {
        assertValidationFailure(new DeletePrivilegesRequest(null, null), "application name", "privileges");
        assertValidationFailure(new DeletePrivilegesRequest("", null), "application name", "privileges");
        assertValidationFailure(new DeletePrivilegesRequest(null, new String[0]), "application name", "privileges");
        assertValidationFailure(new DeletePrivilegesRequest("", new String[0]), "application name", "privileges");
        assertValidationFailure(new DeletePrivilegesRequest(null, new String[]{"all"}), "application name");
        assertValidationFailure(new DeletePrivilegesRequest("", new String[]{"all"}), "application name");
        assertValidationFailure(new DeletePrivilegesRequest("app", null), "privileges");
        assertValidationFailure(new DeletePrivilegesRequest("app", new String[0]), "privileges");
        assertValidationFailure(new DeletePrivilegesRequest("app", new String[]{""}), "privileges");

        assertThat(new DeletePrivilegesRequest("app", new String[]{"all"}).validate(), nullValue());
        assertThat(new DeletePrivilegesRequest("app", new String[]{"all", "some"}).validate(), nullValue());
    }

    private void assertValidationFailure(DeletePrivilegesRequest request, String... messages) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        for (String message : messages) {
            assertThat(exception.validationErrors(), Matchers.hasItem(containsString(message)));
        }
    }

}
