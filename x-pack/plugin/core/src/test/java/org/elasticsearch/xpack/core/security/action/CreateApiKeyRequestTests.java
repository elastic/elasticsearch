/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CreateApiKeyRequestTests extends ESTestCase {

    public void testNameValidation() {
        final String name = randomAlphaOfLengthBetween(1, 256);
        CreateApiKeyRequest request = new CreateApiKeyRequest();
        request.setName(name);

        ActionRequestValidationException ve = request.validate();
        assertNull(ve);

        request.setName("");
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("name is required"));

        request.setName(null);
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("name is required"));

        request.setName(randomAlphaOfLength(257));
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("name may not be more than 256 characters long"));
    }

    public void testSerialization() throws IOException {
        final String name = randomAlphaOfLengthBetween(1, 256);
        final TimeValue expiration = randomBoolean() ? null :
            TimeValue.parseTimeValue(randomTimeValue(), "test serialization of create api key");
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
        final int numDescriptors = randomIntBetween(0, 4);
        final List<RoleDescriptor> descriptorList = new ArrayList<>();
        for (int i = 0; i < numDescriptors; i++) {
            descriptorList.add(new RoleDescriptor("role_" + i, new String[] { "all" }, null, null));
        }

        final CreateApiKeyRequest request = new CreateApiKeyRequest();
        request.setName(name);
        request.setExpiration(expiration);

        if (refreshPolicy != request.getRefreshPolicy() || randomBoolean()) {
            request.setRefreshPolicy(refreshPolicy);
        }
        if (descriptorList.isEmpty() == false || randomBoolean()) {
            request.setRoleDescriptors(descriptorList);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final CreateApiKeyRequest serialized = new CreateApiKeyRequest(in);
                assertEquals(name, serialized.getName());
                assertEquals(expiration, serialized.getExpiration());
                assertEquals(refreshPolicy, serialized.getRefreshPolicy());
                assertEquals(descriptorList, serialized.getRoleDescriptors());
            }
        }
    }
}
