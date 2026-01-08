/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FireworksAiEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<FireworksAiEmbeddingsTaskSettings> {

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testFromMap_WithDimensions() {
        assertEquals(
            new FireworksAiEmbeddingsTaskSettings(512),
            FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, 512)))
        );
    }

    public void testFromMap_MissingDimensions_DoesNotThrowException() {
        var taskSettings = FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.dimensions());
    }

    public void testFromMap_DimensionsIsZero_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, 0)))
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [task_settings] Invalid value [0]. [dimensions] must be a positive integer")
        );
    }

    public void testFromMap_DimensionsIsNegative_ThrowsException() {
        var dimensions = randomNegativeInt();
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, dimensions)))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [%d]. [dimensions] must be a positive integer",
                    dimensions
                )
            )
        );
    }

    public void testOf_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = FireworksAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, 1024))
        );

        var overriddenTaskSettings = FireworksAiEmbeddingsTaskSettings.of(taskSettings, FireworksAiEmbeddingsTaskSettings.EMPTY_SETTINGS);
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesOverriddenSettings() {
        var taskSettings = FireworksAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, 1024))
        );

        var requestTaskSettings = FireworksAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, 512))
        );

        var overriddenTaskSettings = FireworksAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new FireworksAiEmbeddingsTaskSettings(512)));
    }

    public void testToXContent_WithoutDimensions() throws IOException {
        var settings = FireworksAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithDimensions() throws IOException {
        var settings = FireworksAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(768));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"dimensions":768}"""));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.dimensions() != null) {
            newSettingsMap.put(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, newSettings.dimensions());
        }
        FireworksAiEmbeddingsTaskSettings updatedSettings = (FireworksAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            newSettingsMap
        );
        // updatedTaskSettings returns a new instance from the map, not merged
        // So it will use values from the map or null
        if (newSettings.dimensions() == null) {
            assertNull(updatedSettings.dimensions());
        } else {
            assertEquals(newSettings.dimensions(), updatedSettings.dimensions());
        }
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable Integer dimensions) {
        Map<String, Object> map = new HashMap<>();
        if (dimensions != null) {
            map.put(FireworksAiEmbeddingsTaskSettings.DIMENSIONS, dimensions);
        }
        return map;
    }

    @Override
    protected Writeable.Reader<FireworksAiEmbeddingsTaskSettings> instanceReader() {
        return FireworksAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings mutateInstance(FireworksAiEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, FireworksAiEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected FireworksAiEmbeddingsTaskSettings mutateInstanceForVersion(
        FireworksAiEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static FireworksAiEmbeddingsTaskSettings createRandom() {
        return new FireworksAiEmbeddingsTaskSettings(randomFrom(new Integer[] { null, randomNonNegativeInt() }));
    }
}
