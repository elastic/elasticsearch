/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetPrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final GetPrivilegesRequest original = new GetPrivilegesRequest();
        if (randomBoolean()) {
            original.application(randomAlphaOfLengthBetween(3, 8));
        }
        original.privileges(generateRandomStringArray(3, 5, false, true));

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final GetPrivilegesRequest copy = new GetPrivilegesRequest(out.bytes().streamInput());

        assertThat(original.application(), Matchers.equalTo(copy.application()));
        assertThat(original.privileges(), Matchers.equalTo(copy.privileges()));
    }

    public void testValidation() {
        assertThat(request(null).validate(), nullValue());
        assertThat(request(null, "all").validate(), nullValue());
        assertThat(request(null, "read", "write").validate(), nullValue());
        assertThat(request("my_app").validate(), nullValue());
        assertThat(request("my_app", "all").validate(), nullValue());
        assertThat(request("my_app", "read", "write").validate(), nullValue());
        final ActionRequestValidationException exception = request("my_app", ((String[]) null)).validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(),
            containsInAnyOrder("privileges cannot be null"));
    }

    private GetPrivilegesRequest request(String application, String... privileges) {
        final GetPrivilegesRequest request = new GetPrivilegesRequest();
        request.application(application);
        request.privileges(privileges);
        return request;
    }

}
