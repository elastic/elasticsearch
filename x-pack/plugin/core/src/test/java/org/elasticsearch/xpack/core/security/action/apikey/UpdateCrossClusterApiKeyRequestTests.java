/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequestTests.randomCrossClusterApiKeyAccessField;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateCrossClusterApiKeyRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final var metadata = ApiKeyTests.randomMetadata();

        final CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder;
        if (metadata == null || randomBoolean()) {
            roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(randomCrossClusterApiKeyAccessField());
        } else {
            roleDescriptorBuilder = null;
        }

        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), roleDescriptorBuilder, metadata);
        assertThat(request.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(request.validate(), nullValue());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final var serialized = new UpdateCrossClusterApiKeyRequest(in);
                assertEquals(request.getId(), serialized.getId());
                assertEquals(request.getRoleDescriptors(), serialized.getRoleDescriptors());
                assertEquals(metadata, serialized.getMetadata());
                assertEquals(request.getType(), serialized.getType());
            }
        }
    }

    public void testNotEmptyUpdateValidation() {
        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), null, null);
        final ActionRequestValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.validationErrors(), contains("must update either [access] or [metadata] for cross-cluster API keys"));
    }

    public void testMetadataKeyValidation() {
        final var reservedKey = "_" + randomAlphaOfLengthBetween(0, 10);
        final var metadataValue = randomAlphaOfLengthBetween(1, 10);
        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), null, Map.of(reservedKey, metadataValue));
        final ActionRequestValidationException ve = request.validate();
        assertThat(ve, notNullValue());
        assertThat(ve.validationErrors(), contains("API key metadata keys may not start with [_]"));
    }
}
