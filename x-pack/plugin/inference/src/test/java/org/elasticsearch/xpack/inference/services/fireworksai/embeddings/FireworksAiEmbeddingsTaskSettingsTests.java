/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class FireworksAiEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<FireworksAiEmbeddingsTaskSettings> {

    public void testIsEmpty() {
        var settings = FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS;
        assertTrue(settings.isEmpty());
    }

    public void testFromMap_EmptyMap_ReturnsEmptySettings() {
        assertEquals(
            FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS,
            FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>())
        );
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertEquals(
            FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS,
            FireworksAiEmbeddingsTaskSettings.fromMap(null)
        );
    }

    public void testFromMap_WithAnyValues_ReturnsEmptySettings() {
        // Task settings don't have any fields, so any input returns empty settings
        assertEquals(
            FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS,
            FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of("someKey", "someValue")))
        );
    }

    public void testOf_AlwaysReturnsEmptySettings() {
        var taskSettings = new FireworksAiEmbeddingsTaskSettings();
        var overriddenTaskSettings = FireworksAiEmbeddingsTaskSettings.of(taskSettings, FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS);
        assertThat(overriddenTaskSettings, is(FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testToXContent_ReturnsEmptyObject() throws IOException {
        var settings = FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS;

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testUpdatedTaskSettings_ReturnsEmptySettings() {
        var initialSettings = new FireworksAiEmbeddingsTaskSettings();
        Map<String, Object> newSettingsMap = new HashMap<>();
        FireworksAiEmbeddingsTaskSettings updatedSettings = (FireworksAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            newSettingsMap
        );
        assertEquals(FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS, updatedSettings);
    }

    @Override
    protected Writeable.Reader<FireworksAiEmbeddingsTaskSettings> instanceReader() {
        return FireworksAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings createTestInstance() {
        return new FireworksAiEmbeddingsTaskSettings();
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings mutateInstance(FireworksAiEmbeddingsTaskSettings instance) throws IOException {
        // No mutation possible for empty settings - return same instance
        return instance;
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings mutateInstanceForVersion(
        FireworksAiEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
