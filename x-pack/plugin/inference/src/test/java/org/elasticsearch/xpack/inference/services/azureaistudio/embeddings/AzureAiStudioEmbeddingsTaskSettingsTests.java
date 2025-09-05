/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AzureAiStudioEmbeddingsTaskSettings> {
    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(AzureAiStudioConstants.USER_FIELD, newSettings.user());
        }
        AzureAiStudioEmbeddingsTaskSettings updatedSettings = (AzureAiStudioEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new AzureAiStudioEmbeddingsTaskSettings("user"),
            AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")));

        var overriddenTaskSettings = AzureAiStudioEmbeddingsTaskSettings.of(
            taskSettings,
            AzureAiStudioEmbeddingsRequestTaskSettings.EMPTY_SETTINGS
        );
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")));

        var requestTaskSettings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user2"))
        );

        var overriddenTaskSettings = AzureAiStudioEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioEmbeddingsTaskSettings("user2")));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AzureAiStudioEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AzureAiStudioEmbeddingsTaskSettings.fromMap(getTaskSettingsMap("testuser"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"user":"testuser"}"""));
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable String user) {
        Map<String, Object> map = new HashMap<>();
        if (user != null) {
            map.put(AzureAiStudioConstants.USER_FIELD, user);
        }
        return map;
    }

    @Override
    protected Writeable.Reader<AzureAiStudioEmbeddingsTaskSettings> instanceReader() {
        return AzureAiStudioEmbeddingsTaskSettings::new;
    }

    @Override
    protected AzureAiStudioEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioEmbeddingsTaskSettings mutateInstance(AzureAiStudioEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureAiStudioEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected AzureAiStudioEmbeddingsTaskSettings mutateInstanceForVersion(
        AzureAiStudioEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiStudioEmbeddingsTaskSettings createRandom() {
        return new AzureAiStudioEmbeddingsTaskSettings(randomFrom(new String[] { null, randomAlphaOfLength(15) }));
    }
}
