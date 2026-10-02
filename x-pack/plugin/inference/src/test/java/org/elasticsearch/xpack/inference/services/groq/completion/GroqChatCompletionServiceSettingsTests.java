/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.ORGANIZATION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class GroqChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<GroqChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    public static final URI TEST_URI = createUri("https://api.groq.com/openai/v1/chat/completions");
    public static final URI INITIAL_TEST_URI = createUri("https://api.initial-groq.com/v1/chat/completions");

    private static final String TEST_ORGANIZATION_ID = "test-org";
    private static final String INITIAL_TEST_ORGANIZATION_ID = "initial-org";

    private static final int TEST_RATE_LIMIT = 10;
    private static final int INITIAL_TEST_RATE_LIMIT = 20;
    private static final int DEFAULT_RATE_LIMIT = 1_000;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = GroqChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );
        assertThat(
            serviceSettings,
            is(new GroqChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, TEST_ORGANIZATION_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = GroqChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );
        assertThat(
            serviceSettings,
            is(new GroqChatCompletionServiceSettings(TEST_MODEL_ID, null, null, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_Failure() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> GroqChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.getMessage(), containsString(MODEL_ID));
    }

    public void testUpdateServiceSettings_OnlyRateLimit_IsUpdated() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(null, null, null, TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(
                new GroqChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    INITIAL_TEST_ORGANIZATION_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_RateLimitNull_ResetsToDefault() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(RateLimitSettings.FIELD_NAME, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GroqChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    INITIAL_TEST_ORGANIZATION_ID,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_RateLimitAbsent_KeepsExisting() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings.rateLimitSettings(), is(new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)));
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        for (String immutableField : List.of(MODEL_ID, URL)) {
            var e = expectThrows(
                XContentParseException.class,
                () -> originalServiceSettings.updateServiceSettings(new HashMap<>(Map.of(immutableField, "value")))
            );
            assertThat(
                e.getMessage(),
                endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, immutableField))
            );
        }
    }

    public void testUpdateServiceSettings_OrganizationId_IsUpdated() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(ORGANIZATION, TEST_ORGANIZATION_ID);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GroqChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    TEST_ORGANIZATION_ID,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_OrganizationIdNull_ClearsExisting() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(ORGANIZATION, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GroqChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    null,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_OrganizationIdAbsent_KeepsExisting() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings.organizationId(), is(INITIAL_TEST_ORGANIZATION_ID));
    }

    public void testUpdateServiceSettings_OrganizationIdEmptyString_Rejected() {
        var originalServiceSettings = new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(ORGANIZATION, "");

        var e = expectThrows(XContentParseException.class, () -> originalServiceSettings.updateServiceSettings(updateMap));
        assertThat(
            e.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    ORGANIZATION
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GroqChatCompletionServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_ORGANIZATION_ID,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "organization_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_ORGANIZATION_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<GroqChatCompletionServiceSettings> instanceReader() {
        return GroqChatCompletionServiceSettings::new;
    }

    @Override
    protected GroqChatCompletionServiceSettings createTestInstance() {
        String modelId = randomAlphaOfLength(8);
        URI uri = randomBoolean() ? URI.create("https://api.groq.com/" + randomAlphaOfLength(5)) : null;
        String organizationId = randomAlphaOfLengthOrNull(6);
        RateLimitSettings rateLimitSettings = RateLimitSettingsTests.createRandom();
        return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
    }

    @Override
    protected GroqChatCompletionServiceSettings mutateInstance(GroqChatCompletionServiceSettings instance) {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        String organizationId = instance.organizationId();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 3)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> URI.create("https://api.groq.com/" + randomAlphaOfLength(6)));
            case 2 -> organizationId = randomValueOtherThan(organizationId, () -> randomAlphaOfLengthOrNull(5));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Unsupported branch");
        }

        return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String organization,
        @Nullable Integer rateLimit
    ) {
        HashMap<String, Object> map = new HashMap<>();
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(URL, url);
        }
        if (organization != null) {
            map.put(ORGANIZATION, organization);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
