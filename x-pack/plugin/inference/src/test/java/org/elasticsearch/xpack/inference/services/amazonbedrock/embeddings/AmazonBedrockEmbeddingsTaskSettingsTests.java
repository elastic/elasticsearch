/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithoutUnspecified;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TRUNCATE_FIELD;
import static org.hamcrest.Matchers.equalTo;

public class AmazonBedrockEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AmazonBedrockEmbeddingsTaskSettings> {

    public static AmazonBedrockEmbeddingsTaskSettings emptyTaskSettings() {
        return AmazonBedrockEmbeddingsTaskSettings.EMPTY;
    }

    public static AmazonBedrockEmbeddingsTaskSettings randomTaskSettings() {
        var inputType = randomBoolean() ? randomWithoutUnspecified() : null;
        var truncation = randomBoolean() ? randomFrom(CohereTruncation.values()) : null;
        return new AmazonBedrockEmbeddingsTaskSettings(truncation);
    }

    public static AmazonBedrockEmbeddingsTaskSettings mutateTaskSettings(AmazonBedrockEmbeddingsTaskSettings instance) {
        return randomValueOtherThanMany(
            v -> Objects.equals(instance, v) || (instance.cohereTruncation() != null && v.cohereTruncation() == null),
            AmazonBedrockEmbeddingsTaskSettingsTests::randomTaskSettings
        );
    }

    @Override
    protected AmazonBedrockEmbeddingsTaskSettings mutateInstanceForVersion(
        AmazonBedrockEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockEmbeddingsTaskSettings> instanceReader() {
        return AmazonBedrockEmbeddingsTaskSettings::new;
    }

    @Override
    protected AmazonBedrockEmbeddingsTaskSettings createTestInstance() {
        return randomTaskSettings();
    }

    @Override
    protected AmazonBedrockEmbeddingsTaskSettings mutateInstance(AmazonBedrockEmbeddingsTaskSettings instance) throws IOException {
        return mutateTaskSettings(instance);
    }

    public void testEmpty() {
        assertTrue(emptyTaskSettings().isEmpty());
        assertTrue(AmazonBedrockEmbeddingsTaskSettings.fromMap(null).isEmpty());
        assertTrue(AmazonBedrockEmbeddingsTaskSettings.fromMap(Map.of()).isEmpty());
    }

    public static Map<String, Object> mutableMap(String key, Enum<?> value) {
        return new HashMap<>(Map.of(key, value.toString()));
    }

    public void testValidCohereTruncations() {
        for (var expectedCohereTruncation : CohereTruncation.ALL) {
            var map = mutableMap(TRUNCATE_FIELD, expectedCohereTruncation);
            var taskSettings = AmazonBedrockEmbeddingsTaskSettings.fromMap(map);
            assertFalse(taskSettings.isEmpty());
            assertThat(taskSettings.cohereTruncation(), equalTo(expectedCohereTruncation));
        }
    }

    public void testGarbageCohereTruncations() {
        var map = new HashMap<String, Object>(Map.of(TRUNCATE_FIELD, "oiuesoirtuoawoeirha"));
        assertThrows(ValidationException.class, () -> AmazonBedrockEmbeddingsTaskSettings.fromMap(map));
    }

    public void testXContent() throws IOException {
        var taskSettings = randomTaskSettings();
        var taskSettingsAsMap = toMap(taskSettings);
        var roundTripTaskSettings = AmazonBedrockEmbeddingsTaskSettings.fromMap(new HashMap<>(taskSettingsAsMap));
        assertThat(roundTripTaskSettings, equalTo(taskSettings));
    }

    public static Map<String, Object> toMap(AmazonBedrockEmbeddingsTaskSettings taskSettings) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            taskSettings.toXContent(builder, ToXContent.EMPTY_PARAMS);
            var taskSettingsBytes = Strings.toString(builder).getBytes(StandardCharsets.UTF_8);
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, taskSettingsBytes)) {
                return parser.map();
            }
        }
    }
}
