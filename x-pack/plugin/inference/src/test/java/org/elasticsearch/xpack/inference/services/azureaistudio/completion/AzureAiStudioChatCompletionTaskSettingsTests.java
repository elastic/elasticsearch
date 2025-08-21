/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioChatCompletionTaskSettingsTests extends AbstractBWCWireSerializationTestCase<
    AzureAiStudioChatCompletionTaskSettings> {

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        var settingsMap = new HashMap<String, Object>();
        if (newSettings.doSample() != null) settingsMap.put(DO_SAMPLE_FIELD, newSettings.doSample());
        if (newSettings.temperature() != null) settingsMap.put(TEMPERATURE_FIELD, newSettings.temperature());
        if (newSettings.topP() != null) settingsMap.put(TOP_P_FIELD, newSettings.topP());
        if (newSettings.maxNewTokens() != null) settingsMap.put(MAX_NEW_TOKENS_FIELD, newSettings.maxNewTokens());

        AzureAiStudioChatCompletionTaskSettings updatedSettings = (AzureAiStudioChatCompletionTaskSettings) initialSettings
            .updatedTaskSettings(Collections.unmodifiableMap(settingsMap));

        assertEquals(
            newSettings.temperature() == null ? initialSettings.temperature() : newSettings.temperature(),
            updatedSettings.temperature()
        );
        assertEquals(newSettings.topP() == null ? initialSettings.topP() : newSettings.topP(), updatedSettings.topP());
        assertEquals(newSettings.doSample() == null ? initialSettings.doSample() : newSettings.doSample(), updatedSettings.doSample());
        assertEquals(
            newSettings.maxNewTokens() == null ? initialSettings.maxNewTokens() : newSettings.maxNewTokens(),
            updatedSettings.maxNewTokens()
        );
    }

    public void testFromMap_AllValues() {
        var taskMap = getTaskSettingsMap(1.0, 2.0, true, 512);
        assertEquals(
            new AzureAiStudioChatCompletionTaskSettings(1.0, 2.0, true, 512),
            AzureAiStudioChatCompletionTaskSettings.fromMap(taskMap)
        );
    }

    public void testFromMap_TemperatureIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(null, 2.0, true, 512);
        taskMap.put(TEMPERATURE_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [temperature] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_TopPIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(null, 2.0, true, 512);
        taskMap.put(TOP_P_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [top_p] is not of the expected type. The value [invalid] cannot be converted to a [Double]")
            )
        );
    }

    public void testFromMap_DoSampleIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(null, 2.0, true, 512);
        taskMap.put(DO_SAMPLE_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString("field [do_sample] is not of the expected type. The value [invalid] cannot be converted to a [Boolean]")
        );
    }

    public void testFromMap_MaxNewTokensIsInvalidValue_ThrowsValidationException() {
        var taskMap = getTaskSettingsMap(null, 2.0, true, 512);
        taskMap.put(MAX_NEW_TOKENS_FIELD, "invalid");

        var thrownException = expectThrows(ValidationException.class, () -> AzureAiStudioChatCompletionTaskSettings.fromMap(taskMap));

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("field [max_new_tokens] is not of the expected type. The value [invalid] cannot be converted to a [Integer]")
            )
        );
    }

    public void testFromMap_WithNoValues_DoesNotThrowException() {
        var taskMap = AzureAiStudioChatCompletionTaskSettings.fromMap(new HashMap<String, Object>(Map.of()));
        assertNull(taskMap.temperature());
        assertNull(taskMap.topP());
        assertNull(taskMap.doSample());
        assertNull(taskMap.maxNewTokens());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));
        var overrideSettings = AzureAiStudioChatCompletionTaskSettings.of(
            settings,
            AzureAiStudioChatCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        MatcherAssert.assertThat(overrideSettings, is(settings));
    }

    public void testOverrideWith_UsesTemperatureOverride() {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));
        var overrideSettings = AzureAiStudioChatCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(1.5, null, null, null));
        var overriddenTaskSettings = AzureAiStudioChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioChatCompletionTaskSettings(1.5, 2.0, true, 512)));
    }

    public void testOverrideWith_UsesTopPOverride() {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));
        var overrideSettings = AzureAiStudioChatCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, 0.2, null, null));
        var overriddenTaskSettings = AzureAiStudioChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioChatCompletionTaskSettings(1.0, 0.2, true, 512)));
    }

    public void testOverrideWith_UsesDoSampleOverride() {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));
        var overrideSettings = AzureAiStudioChatCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, null, false, null));
        var overriddenTaskSettings = AzureAiStudioChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioChatCompletionTaskSettings(1.0, 2.0, false, 512)));
    }

    public void testOverrideWith_UsesMaxNewTokensOverride() {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));
        var overrideSettings = AzureAiStudioChatCompletionRequestTaskSettings.fromMap(getTaskSettingsMap(null, null, null, 128));
        var overriddenTaskSettings = AzureAiStudioChatCompletionTaskSettings.of(settings, overrideSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioChatCompletionTaskSettings(1.0, 2.0, true, 128)));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(null, null, null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AzureAiStudioChatCompletionTaskSettings.fromMap(getTaskSettingsMap(1.0, 2.0, true, 512));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"temperature":1.0,"top_p":2.0,"do_sample":true,"max_new_tokens":512}"""));
    }

    public static Map<String, Object> getTaskSettingsMap(
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens
    ) {
        var map = new HashMap<String, Object>();

        if (temperature != null) {
            map.put(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            map.put(TOP_P_FIELD, topP);
        }

        if (doSample != null) {
            map.put(DO_SAMPLE_FIELD, doSample);
        }

        if (maxNewTokens != null) {
            map.put(MAX_NEW_TOKENS_FIELD, maxNewTokens);
        }

        return map;
    }

    @Override
    protected Writeable.Reader<AzureAiStudioChatCompletionTaskSettings> instanceReader() {
        return AzureAiStudioChatCompletionTaskSettings::new;
    }

    @Override
    protected AzureAiStudioChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioChatCompletionTaskSettings mutateInstance(AzureAiStudioChatCompletionTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureAiStudioChatCompletionTaskSettingsTests::createRandom);
    }

    @Override
    protected AzureAiStudioChatCompletionTaskSettings mutateInstanceForVersion(
        AzureAiStudioChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiStudioChatCompletionTaskSettings createRandom() {
        return new AzureAiStudioChatCompletionTaskSettings(
            randomFrom(new Double[] { null, randomDouble() }),
            randomFrom(new Double[] { null, randomDouble() }),
            randomFrom(randomFrom(new Boolean[] { null, randomBoolean() })),
            randomFrom(new Integer[] { null, randomNonNegativeInt() })
        );
    }
}
