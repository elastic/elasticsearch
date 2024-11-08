/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_P_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AmazonBedrockChatCompletionTaskSettingsTests extends AbstractBWCWireSerializationTestCase<
    AmazonBedrockChatCompletionTaskSettings> {

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void updatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = createRandom();
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(Map.of());
        assertEquals(initialSettings, updatedSettings);
    }

    public void updatedTaskSettings_WithNewTemperature_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TEMPERATURE_FIELD, 0.7);
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettings);
        assertEquals(0.7, (double) updatedSettings.temperature(), 0.001);
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewTopP_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TOP_P_FIELD, 0.8);
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettings);
        assertEquals(0.8, (double) updatedSettings.topP(), 0.001);
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewTopK_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TOP_K_FIELD, 0.9);
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettings);
        assertEquals(0.9, (double) updatedSettings.topK(), 0.001);
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewMaxNewTokens_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(MAX_NEW_TOKENS_FIELD, 256);
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettings);
        assertEquals(256, (double) updatedSettings.maxNewTokens(), 0.001);
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
    }

    public void updatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TEMPERATURE_FIELD, 0.7, TOP_P_FIELD, 0.8, TOP_K_FIELD, 0.9, MAX_NEW_TOKENS_FIELD, 256);
        AmazonBedrockChatCompletionTaskSettings updatedSettings = (AmazonBedrockChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(newSettings);
        assertEquals(0.7, (double) updatedSettings.temperature(), 0.001);
        assertEquals(0.8, (double) updatedSettings.topP(), 0.001);
        assertEquals(0.9, (double) updatedSettings.topK(), 0.001);
        assertEquals(256, (int) updatedSettings.maxNewTokens(), 0.001);
    }

    public void testFromMap_AllValues() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        assertEquals(
            new AmazonBedrockChatCompletionTaskSettings(1.0, 0.5, 0.6, 512),
            AmazonBedrockChatCompletionTaskSettings.fromMap(taskMap)
        );
    }

    public void testFromMap_TemperatureIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(TEMPERATURE_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [temperature] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_TopPIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(TOP_P_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [top_p] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_TopKIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(TOP_K_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [top_k] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
        );
    }

    public void testFromMap_MaxNewTokensIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(MAX_NEW_TOKENS_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [max_new_tokens] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
            )
        );
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        var taskMap = AmazonBedrockChatCompletionTaskSettings.fromMap(new HashMap<String, Object>(Map.of()));
        assertNull(taskMap.temperature());
        assertNull(taskMap.topP());
        assertNull(taskMap.topK());
        assertNull(taskMap.maxNewTokens());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockChatCompletionTaskSettings.of(settings, AmazonBedrockChatCompletionTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesTemperatureOverride() {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(
            getChatCompletionTaskSettingsMap(0.3, null, null, null)
        );
        var overriddenTaskSettings = AmazonBedrockChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockChatCompletionTaskSettings(0.3, 0.5, 0.6, 512)));
    }

    public void testOverrideWith_UsesTopPOverride() {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(
            getChatCompletionTaskSettingsMap(null, 0.2, null, null)
        );
        var overriddenTaskSettings = AmazonBedrockChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockChatCompletionTaskSettings(1.0, 0.2, 0.6, 512)));
    }

    public void testOverrideWith_UsesDoSampleOverride() {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(
            getChatCompletionTaskSettingsMap(null, null, 0.1, null)
        );
        var overriddenTaskSettings = AmazonBedrockChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockChatCompletionTaskSettings(1.0, 0.5, 0.1, 512)));
    }

    public void testOverrideWith_UsesMaxNewTokensOverride() {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(
            getChatCompletionTaskSettingsMap(null, null, null, 128)
        );
        var overriddenTaskSettings = AmazonBedrockChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockChatCompletionTaskSettings(1.0, 0.5, 0.6, 128)));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(null, null, null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AmazonBedrockChatCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"temperature":1.0,"top_p":0.5,"top_k":0.6,"max_new_tokens":512}"""));
    }

    public static Map<String, Object> getChatCompletionTaskSettingsMap(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Double topK,
        @Nullable Integer maxNewTokens
    ) {
        var map = new HashMap<String, Object>();

        if (temperature != null) {
            map.put(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            map.put(TOP_P_FIELD, topP);
        }

        if (topK != null) {
            map.put(TOP_K_FIELD, topK);
        }

        if (maxNewTokens != null) {
            map.put(MAX_NEW_TOKENS_FIELD, maxNewTokens);
        }

        return map;
    }

    @Override
    protected AmazonBedrockChatCompletionTaskSettings mutateInstanceForVersion(
        AmazonBedrockChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockChatCompletionTaskSettings> instanceReader() {
        return AmazonBedrockChatCompletionTaskSettings::new;
    }

    @Override
    protected AmazonBedrockChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockChatCompletionTaskSettings mutateInstance(AmazonBedrockChatCompletionTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AmazonBedrockChatCompletionTaskSettingsTests::createRandom);
    }

    private static AmazonBedrockChatCompletionTaskSettings createRandom() {
        return new AmazonBedrockChatCompletionTaskSettings(
            randomFrom(new Double[] { null, randomDouble() }),
            randomFrom(new Double[] { null, randomDouble() }),
            randomFrom(new Double[] { null, randomDouble() }),
            randomFrom(new Integer[] { null, randomNonNegativeInt() })
        );
    }
}
