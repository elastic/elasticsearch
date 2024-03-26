/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.nullValue;

public class UpdateApiKeyRequestSerializationTests extends AbstractWireSerializingTestCase<UpdateApiKeyRequest> {
    public void testSerializationBackwardsCompatibility() throws IOException {
        UpdateApiKeyRequest testInstance = createTestInstance();
        UpdateApiKeyRequest deserializedInstance = copyInstance(testInstance, TransportVersions.V_8_11_X);
        try {
            // Transport is on a version before expiration was introduced, so should always be null
            assertThat(deserializedInstance.getExpiration(), nullValue());
        } finally {
            dispose(deserializedInstance);
        }
    }

    @Override
    protected UpdateApiKeyRequest createTestInstance() {
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
        final TimeValue expiration = ApiKeyTests.randomFutureExpirationTime();
        return new UpdateApiKeyRequest(id, descriptorList, metadata, expiration);
    }

    @Override
    protected Writeable.Reader<UpdateApiKeyRequest> instanceReader() {
        return UpdateApiKeyRequest::new;
    }

    @Override
    protected UpdateApiKeyRequest mutateInstance(UpdateApiKeyRequest instance) throws IOException {
        Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        long days = randomValueOtherThan(instance.getExpiration().days(), () -> ApiKeyTests.randomFutureExpirationTime().getDays());
        return new UpdateApiKeyRequest(
            instance.getId(),
            instance.getRoleDescriptors(),
            metadata,
            TimeValue.parseTimeValue(days + "d", null, "expiration")
        );
    }

}
