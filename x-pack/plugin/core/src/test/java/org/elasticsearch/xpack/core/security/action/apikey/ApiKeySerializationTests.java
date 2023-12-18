/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests.randomApiKeyInstance;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeySerializationTests extends AbstractWireSerializingTestCase<ApiKey> {

    public void testSerializationBackwardsCompatibility() throws IOException {
        ApiKey testInstance = createTestInstance();
        ApiKey deserializedInstance = copyInstance(testInstance, TransportVersions.V_8_500_064);
        try {
            // Transport is on a version before invalidation was introduced, so should always be null
            assertThat(deserializedInstance.getInvalidation(), nullValue());
        } finally {
            dispose(deserializedInstance);
        }
    }

    @Override
    protected ApiKey createTestInstance() {
        return randomApiKeyInstance();
    }

    @Override
    protected ApiKey mutateInstance(ApiKey instance) throws IOException {
        ApiKey copyOfInstance = copyInstance(instance);
        // Metadata in test instance is mutable, so mutate it instead of the copy (immutable metadata) to make sure they differ
        Object metadataNumberValue = instance.getMetadata().getOrDefault("number", Integer.toString(randomInt()));
        instance.getMetadata().put("number", Integer.parseInt(metadataNumberValue.toString()) + randomInt());
        return copyOfInstance;
    }

    @Override
    protected Writeable.Reader<ApiKey> instanceReader() {
        return ApiKey::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
    }

    public static Map<String, Object> randomMetadata() {
        Map<String, Object> randomMetadata = randomFrom(
            Map.of(
                "application",
                randomAlphaOfLength(5),
                "number",
                1,
                "numbers",
                List.of(1, 3, 5),
                "environment",
                Map.of("os", "linux", "level", 42, "category", "trusted")
            ),
            Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
            Map.of(),
            null
        );

        // Make metadata mutable for testing purposes
        return randomMetadata == null ? new HashMap<>() : new HashMap<>(randomMetadata);
    }
}
