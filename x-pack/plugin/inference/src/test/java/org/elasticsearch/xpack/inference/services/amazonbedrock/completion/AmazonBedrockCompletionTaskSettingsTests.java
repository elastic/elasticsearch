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
import org.elasticsearch.test.ESTestCase;
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

public class AmazonBedrockCompletionTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AmazonBedrockCompletionTaskSettings> {

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AmazonBedrockCompletionTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertThat(settings, is(AmazonBedrockCompletionTaskSettings.EMPTY_SETTINGS));
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void updatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = createRandom();
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            Map.of()
        );
        assertEquals(initialSettings, updatedSettings);
    }

    public void updatedTaskSettings_WithNewTemperature_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TEMPERATURE_FIELD, 0.7);
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings
        );
        assertThat(updatedSettings.temperature(), is(0.7));
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewTopP_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TOP_P_FIELD, 0.8);
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings
        );
        assertThat(updatedSettings.topP(), is(0.8));
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewTopK_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TOP_K_FIELD, 0.9);
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings
        );
        assertThat(updatedSettings.topK(), is(0.9));
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.maxNewTokens(), updatedSettings.maxNewTokens());
    }

    public void updatedTaskSettings_WithNewMaxNewTokens_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(MAX_NEW_TOKENS_FIELD, 256);
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings
        );
        assertThat(updatedSettings.maxNewTokens(), is(256));
        assertEquals(initialSettings.temperature(), updatedSettings.temperature());
        assertEquals(initialSettings.topP(), updatedSettings.topP());
        assertEquals(initialSettings.topK(), updatedSettings.topK());
    }

    public void updatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = createRandom();
        Map<String, Object> newSettings = Map.of(TEMPERATURE_FIELD, 0.7, TOP_P_FIELD, 0.8, TOP_K_FIELD, 0.9, MAX_NEW_TOKENS_FIELD, 256);
        AmazonBedrockCompletionTaskSettings updatedSettings = (AmazonBedrockCompletionTaskSettings) initialSettings.updatedTaskSettings(
            newSettings
        );
        assertThat(updatedSettings.temperature(), is(0.7));
        assertThat(updatedSettings.topP(), is(0.8));
        assertThat(updatedSettings.topK(), is(0.9));
        assertThat(updatedSettings.maxNewTokens(), is(256));
    }

    public void testFromMap_AllValues() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        assertEquals(new AmazonBedrockCompletionTaskSettings(1.0, 0.5, 0.6, 512), AmazonBedrockCompletionTaskSettings.fromMap(taskMap));
    }

    public void testFromMap_TemperatureIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(TEMPERATURE_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockCompletionTaskSettings.fromMap(taskMap));

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

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockCompletionTaskSettings.fromMap(taskMap));

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

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [top_k] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
        );
    }

    public void testFromMap_MaxNewTokensIsInvalidValue_ThrowsValidationException() {
        var taskMap = getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512);
        taskMap.put(MAX_NEW_TOKENS_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AmazonBedrockCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [max_new_tokens] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
            )
        );
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        var taskMap = AmazonBedrockCompletionTaskSettings.fromMap(new HashMap<String, Object>(Map.of()));
        assertNull(taskMap.temperature());
        assertNull(taskMap.topP());
        assertNull(taskMap.topK());
        assertNull(taskMap.maxNewTokens());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockCompletionTaskSettings.of(settings, AmazonBedrockCompletionTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesTemperatureOverride() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(0.3, null, null, null));
        var overriddenTaskSettings = AmazonBedrockCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockCompletionTaskSettings(0.3, 0.5, 0.6, 512)));
    }

    public void testOverrideWith_UsesTopPOverride() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(null, 0.2, null, null));
        var overriddenTaskSettings = AmazonBedrockCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockCompletionTaskSettings(1.0, 0.2, 0.6, 512)));
    }

    public void testOverrideWith_UsesDoSampleOverride() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(null, null, 0.1, null));
        var overriddenTaskSettings = AmazonBedrockCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockCompletionTaskSettings(1.0, 0.5, 0.1, 512)));
    }

    public void testOverrideWith_UsesMaxNewTokensOverride() {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));
        var overrideSettings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(null, null, null, 128));
        var overriddenTaskSettings = AmazonBedrockCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AmazonBedrockCompletionTaskSettings(1.0, 0.5, 0.6, 128)));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(null, null, null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AmazonBedrockCompletionTaskSettings.fromMap(getChatCompletionTaskSettingsMap(1.0, 0.5, 0.6, 512));

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
    protected AmazonBedrockCompletionTaskSettings mutateInstanceForVersion(
        AmazonBedrockCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<AmazonBedrockCompletionTaskSettings> instanceReader() {
        return AmazonBedrockCompletionTaskSettings::new;
    }

    @Override
    protected AmazonBedrockCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AmazonBedrockCompletionTaskSettings mutateInstance(AmazonBedrockCompletionTaskSettings instance) throws IOException {
        var temperature = instance.temperature();
        var topP = instance.topP();
        var topK = instance.topK();
        var maxNewTokens = instance.maxNewTokens();
        switch (randomInt(3)) {
            case 0 -> temperature = randomValueOtherThan(temperature, ESTestCase::randomOptionalDouble);
            case 1 -> topP = randomValueOtherThan(topP, ESTestCase::randomOptionalDouble);
            case 2 -> topK = randomValueOtherThan(topK, ESTestCase::randomOptionalDouble);
            case 3 -> maxNewTokens = randomValueOtherThan(maxNewTokens, ESTestCase::randomNonNegativeIntOrNull);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AmazonBedrockCompletionTaskSettings(temperature, topP, topK, maxNewTokens);
    }

    private static AmazonBedrockCompletionTaskSettings createRandom() {
        return new AmazonBedrockCompletionTaskSettings(
            randomOptionalDouble(),
            randomOptionalDouble(),
            randomOptionalDouble(),
            randomNonNegativeIntOrNull()
        );
    }

}
