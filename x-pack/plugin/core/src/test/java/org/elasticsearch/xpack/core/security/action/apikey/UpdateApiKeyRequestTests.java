/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UpdateApiKeyRequestTests extends ESTestCase {

    public void testNullValuesValid() {
        final var request = new UpdateApiKeyRequest("id", null, null);
        assertNull(request.validate());
    }

    public void testSerialization() throws IOException {
        final boolean roleDescriptorsPresent = randomBoolean();
        final List<RoleDescriptor> descriptorList;
        if (roleDescriptorsPresent == false) {
            descriptorList = null;
        } else {
            final int numDescriptors = randomIntBetween(0, 4);
            descriptorList = new ArrayList<>();
            for (int i = 0; i < numDescriptors; i++) {
                descriptorList.add(new RoleDescriptor("role_" + i, new String[] { "all" }, null, null));
            }
        }

        final var id = randomAlphaOfLength(10);
        final var metadata = ApiKeyTests.randomMetadata();
        final var request = new UpdateApiKeyRequest(id, descriptorList, metadata);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final var serialized = new UpdateApiKeyRequest(in);
                assertEquals(id, serialized.getId());
                assertEquals(descriptorList, serialized.getRoleDescriptors());
                assertEquals(metadata, request.getMetadata());
            }
        }
    }
}
