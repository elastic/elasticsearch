/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetPrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final GetPrivilegesRequest original = new GetPrivilegesRequest();
        if (randomBoolean()) {
            GetPrivilegesRequest.PrivilegeType type = randomFrom(GetPrivilegesRequest.PrivilegeType.values());
            original.privilegeTypes(type);
        }
        if (randomBoolean()) {
            original.application(randomAlphaOfLengthBetween(3, 8));
        }
        original.privileges(generateRandomStringArray(3, 5, false, true));

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final GetPrivilegesRequest copy = new GetPrivilegesRequest();
        copy.readFrom(out.bytes().streamInput());

        assertThat(original.privilegeTypes(), Matchers.equalTo(copy.privilegeTypes()));
        assertThat(original.application(), Matchers.equalTo(copy.application()));
        assertThat(original.privileges(), Matchers.equalTo(copy.privileges()));
    }

    public void testSerializationBeforeV72() throws IOException {
        final GetPrivilegesRequest original = new GetPrivilegesRequest();
        original.privilegeTypes(GetPrivilegesRequest.PrivilegeType.APPLICATION);
        if (randomBoolean()) {
            original.application(randomAlphaOfLengthBetween(3, 8));
        }
        original.privileges(generateRandomStringArray(3, 5, false, true));

        final BytesStreamOutput out = new BytesStreamOutput();
        final Version version = VersionUtils.randomVersionBetween(random(), null, Version.V_7_1_0);
        out.setVersion(version);
        original.writeTo(out);


        final GetPrivilegesRequest copy = new GetPrivilegesRequest();
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        copy.readFrom(in);

        assertThat(original.privilegeTypes(), Matchers.equalTo(copy.privilegeTypes()));
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
            containsInAnyOrder("if application privileges are requested, privileges array cannot be null (but may be empty)"));
    }

    private GetPrivilegesRequest request(String application, String... privileges) {
        final GetPrivilegesRequest request = new GetPrivilegesRequest();
        request.application(application);
        request.privileges(privileges);
        return request;
    }

}
