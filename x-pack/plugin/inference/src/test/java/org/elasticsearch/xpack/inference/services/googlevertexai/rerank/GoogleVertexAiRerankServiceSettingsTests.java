/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiRerankServiceSettings> {

    private static final String TEST_PROJECT_ID = "some-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-project-id";

    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    private static final Integer TEST_RATE_LIMIT = 10;
    private static final Integer INITIAL_TEST_RATE_LIMIT = 20;
    private static final Integer DEFAULT_RATE_LIMIT = 300;

    public void testFromMap_Request_WithAllFields_Success() {
        GoogleVertexAiRerankServiceSettings serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            serviceSettings,
            is(new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_NoRateLimitInMap_UsesDefaultRateLimit_Success() {
        GoogleVertexAiRerankServiceSettings serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_MODEL_ID, null),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            serviceSettings,
            is(new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_NoModelId_Success() {
        GoogleVertexAiRerankServiceSettings serviceSettings = GoogleVertexAiRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_PROJECT_ID, null, TEST_RATE_LIMIT),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            serviceSettings,
            is(new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, null, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_Request_MissingProjectId_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(null, TEST_MODEL_ID, null),
            "Validation Failed: 1: [service_settings] does not contain the required setting [project_id];"
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_PROJECT_ID, TEST_MODEL_ID, TEST_RATE_LIMIT);
        var originalServiceSettings = new GoogleVertexAiRerankServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GoogleVertexAiRerankServiceSettings(
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GoogleVertexAiRerankServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "project_id": "%s",
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_PROJECT_ID, TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    public void testToXContent_DoesNotWriteModelIfNull() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "project_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_PROJECT_ID, DEFAULT_RATE_LIMIT)));
    }

    public void testFilteredXContentObject_WritesAllValues() throws IOException {
        var entity = new GoogleVertexAiRerankServiceSettings(TEST_PROJECT_ID, TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = entity.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "project_id": "%s",
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_PROJECT_ID, TEST_MODEL_ID, DEFAULT_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiRerankServiceSettings> instanceReader() {
        return GoogleVertexAiRerankServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings mutateInstance(GoogleVertexAiRerankServiceSettings instance) throws IOException {
        var projectId = instance.projectId();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(2)) {
            case 0 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(10));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new GoogleVertexAiRerankServiceSettings(projectId, modelId, rateLimitSettings);
    }

    @Override
    protected GoogleVertexAiRerankServiceSettings mutateInstanceForVersion(
        GoogleVertexAiRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static void assertValidationFailure(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiRerankServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), is(expectedErrorMessage));
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String projectId,
        @Nullable String modelId,
        @Nullable Integer rateLimit
    ) {
        HashMap<String, Object> map = new HashMap<>();
        if (projectId != null) {
            map.put(PROJECT_ID, projectId);
        }
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

    private static GoogleVertexAiRerankServiceSettings createRandom() {
        return new GoogleVertexAiRerankServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() })
        );
    }
}
