/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiEmbeddingsTaskSettings> {
    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.autoTruncate() != null) {
            newSettingsMap.put(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, newSettings.autoTruncate());
        }
        GoogleVertexAiEmbeddingsTaskSettings updatedSettings = (GoogleVertexAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.autoTruncate() == null) {
            assertEquals(initialSettings.autoTruncate(), updatedSettings.autoTruncate());
        } else {
            assertEquals(newSettings.autoTruncate(), updatedSettings.autoTruncate());
        }
    }

    public void testFromMap_AutoTruncateIsSet() {
        var autoTruncate = true;
        var taskSettingsMap = getTaskSettingsMap(autoTruncate);
        var taskSettings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettingsMap);

        assertThat(taskSettings, is(new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate)));
    }

    public void testFromMap_ThrowsValidationException_IfAutoTruncateIsInvalidValue() {
        var taskSettings = getTaskSettingsMap("invalid");

        expectThrows(ValidationException.class, () -> GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettings));
    }

    public void testFromMap_AutoTruncateIsNull() {
        var taskSettingsMap = getTaskSettingsMap(null);
        var taskSettings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(taskSettingsMap);
        // needed, because of constructors being ambiguous otherwise
        Boolean nullBoolean = null;

        assertThat(taskSettings, is(new GoogleVertexAiEmbeddingsTaskSettings(nullBoolean)));
    }

    public void testFromMap_DoesNotThrow_WithEmptyMap() {
        assertNull(GoogleVertexAiEmbeddingsTaskSettings.fromMap(new HashMap<>()).autoTruncate());
    }

    public void testOf_UseRequestSettings() {
        var originalAutoTruncate = true;
        var originalSettings = new GoogleVertexAiEmbeddingsTaskSettings(originalAutoTruncate);

        var requestAutoTruncate = originalAutoTruncate == false;
        var requestTaskSettings = new GoogleVertexAiEmbeddingsRequestTaskSettings(requestAutoTruncate);

        assertThat(GoogleVertexAiEmbeddingsTaskSettings.of(originalSettings, requestTaskSettings).autoTruncate(), is(requestAutoTruncate));
    }

    public void testOf_UseOriginalSettings() {
        var originalAutoTruncate = true;
        var originalSettings = new GoogleVertexAiEmbeddingsTaskSettings(originalAutoTruncate);

        var requestTaskSettings = new GoogleVertexAiEmbeddingsRequestTaskSettings(null);

        assertThat(GoogleVertexAiEmbeddingsTaskSettings.of(originalSettings, requestTaskSettings).autoTruncate(), is(originalAutoTruncate));
    }

    public void testToXContent_WritesAutoTruncateIfNotNull() throws IOException {
        var settings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(true));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"auto_truncate":true}"""));
    }

    public void testToXContent_DoesNotWriteAutoTruncateIfNull() throws IOException {
        var settings = GoogleVertexAiEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {}"""));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiEmbeddingsTaskSettings> instanceReader() {
        return GoogleVertexAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings mutateInstance(GoogleVertexAiEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleVertexAiEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected GoogleVertexAiEmbeddingsTaskSettings mutateInstanceForVersion(
        GoogleVertexAiEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static GoogleVertexAiEmbeddingsTaskSettings createRandom() {
        return new GoogleVertexAiEmbeddingsTaskSettings(randomFrom(new Boolean[] { null, randomBoolean() }));
    }

    private static Map<String, Object> getTaskSettingsMap(@Nullable Object autoTruncate) {
        var map = new HashMap<String, Object>();

        if (autoTruncate != null) {
            map.put(AUTO_TRUNCATE, autoTruncate);
        }

        return map;
    }
}
