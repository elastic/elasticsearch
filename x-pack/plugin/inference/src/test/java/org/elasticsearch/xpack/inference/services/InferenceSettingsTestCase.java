/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * All Inference Setting classes derived from {@link org.elasticsearch.inference.ServiceSettings},
 * {@link org.elasticsearch.inference.TaskSettings}, and {@link org.elasticsearch.inference.SecretSettings}
 * must be able to read/write to an index via ToXContent as well as read/write between nodes via Writeable.
 */
public abstract class InferenceSettingsTestCase<T extends Writeable & ToXContent> extends AbstractBWCWireSerializationTestCase<T> {

    /**
     *  This method is final because {@link org.elasticsearch.inference.ModelConfigurations} settings must be registered in
     * {@link InferenceNamedWriteablesProvider}.
     */
    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    /**
     * Helper implementation since most mutates can be handled by continuously randomizing until we have another test instance.
     */
    @Override
    protected T mutateInstance(T instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    /**
     * Change this for BWC once the implementation requires difference objects depending on the transport version.
     */
    @Override
    protected T mutateInstanceForVersion(T instance, TransportVersion version) {
        return instance;
    }

    /**
     * Verify that we can write to XContent and then read from XContent.
     * This simulates saving the model to the index and then reading the model from the index.
     */
    public final void testXContentRoundTrip() throws IOException {
        var instance = createTestInstance();
        var instanceAsMap = toMap(instance);
        var roundTripInstance = fromMutableMap(new HashMap<>(instanceAsMap));
        assertThat(roundTripInstance, equalTo(instance));
    }

    protected abstract T fromMutableMap(Map<String, Object> mutableMap);

    public static Map<String, Object> toMap(ToXContent instance) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            if (instance.isFragment()) {
                builder.startObject();
            }
            instance.toXContent(builder, ToXContent.EMPTY_PARAMS);
            if (instance.isFragment()) {
                builder.endObject();
            }
            var taskSettingsBytes = Strings.toString(builder).getBytes(StandardCharsets.UTF_8);
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, taskSettingsBytes)) {
                return parser.map();
            }
        }
    }

    /**
     * Helper, since most settings contain optional strings.
     */
    protected static String randomOptionalString() {
        return randomBoolean() ? randomString() : null;
    }

    /**
     * Helper, since most settings contain strings.
     */
    protected static String randomString() {
        return randomAlphaOfLength(randomIntBetween(4, 8));
    }
}
