/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;
import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    private static final URI TEST_URI = createUri("https://www.test.com");
    private static final URI INITIAL_TEST_URI = createUri("https://www.initial.com");

    private static final String TEST_ORGANIZATION_ID = "test-organization";
    private static final String INITIAL_TEST_ORGANIZATION_ID = "initial-organization";

    private static final int TEST_MAX_INPUT_TOKENS = 4096;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    private static final int TEST_RATE_LIMIT = 250;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 500;

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiChatCompletionServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new OpenAiChatCompletionServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null)));
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MODEL_ID))
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_URI.toString(),
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new OpenAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new OpenAiChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    TEST_ORGANIZATION_ID,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new OpenAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "organization_id": "%s",
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT)));
    }

    public void testToXContent_DoesNotWriteOptionalValues() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, DEFAULT_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionServiceSettings> instanceReader() {
        return OpenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenAiChatCompletionServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected OpenAiChatCompletionServiceSettings mutateInstance(OpenAiChatCompletionServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var organizationId = instance.organizationId();
        var maxInputTokens = instance.maxInputTokens();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(4)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 2 -> organizationId = randomValueOtherThan(organizationId, () -> randomAlphaOfLengthOrNull(15));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 4096), null));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new OpenAiChatCompletionServiceSettings(modelId, uri, organizationId, maxInputTokens, rateLimitSettings);
    }

    private static OpenAiChatCompletionServiceSettings createRandomWithNonNullUrl() {
        return createRandom(createUri("https://" + randomAlphaOfLength(10) + ".example"));
    }

    private static OpenAiChatCompletionServiceSettings createRandom(URI uri) {
        var modelId = randomAlphaOfLength(8);
        var organizationId = randomAlphaOfLengthOrNull(15);
        var maxInputTokens = randomFrom(randomIntBetween(128, 4096), null);

        return new OpenAiChatCompletionServiceSettings(modelId, uri, organizationId, maxInputTokens, RateLimitSettingsTests.createRandom());
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String organizationId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(URL, url);
        }
        if (organizationId != null) {
            map.put(ORGANIZATION, organizationId);
        }
        if (maxInputTokens != null) {
            map.put(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
