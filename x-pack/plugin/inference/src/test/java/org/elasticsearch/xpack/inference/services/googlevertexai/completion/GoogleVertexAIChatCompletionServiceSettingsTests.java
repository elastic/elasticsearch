/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.LOCATION;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROJECT_ID;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME;
import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils.ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAIChatCompletionServiceSettingsTests extends InferenceSettingsTestCase<
    GoogleVertexAiChatCompletionServiceSettings> {
    private static final String TEST_PROJECT_ID = "some-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-project-id";

    private static final String TEST_LOCATION = "us-central1";
    private static final String INITIAL_TEST_LOCATION = "europe-west1";

    private static final String TEST_MODEL_ID = "some-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model-id";

    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    private static final String TEST_STREAMING_URL = "https://www.test-streaming.com";
    private static final String INITIAL_TEST_STREAMING_URL = "https://www.initial-test-streaming.com";

    private static final Integer TEST_RATE_LIMIT = 10;
    private static final Integer INITIAL_TEST_RATE_LIMIT = 20;
    private static final Integer DEFAULT_RATE_LIMIT = 1000;

    @Override
    protected Writeable.Reader<GoogleVertexAiChatCompletionServiceSettings> instanceReader() {
        return GoogleVertexAiChatCompletionServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        return GoogleVertexAiChatCompletionServiceSettings.fromMap(mutableMap, ConfigurationParseContext.PERSISTENT);
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings mutateInstanceForVersion(
        GoogleVertexAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED)) {
            return instance;
        } else {
            return new GoogleVertexAiChatCompletionServiceSettings(
                instance.projectId(),
                instance.location(),
                instance.modelId(),
                null,
                null,
                null,
                instance.rateLimitSettings()
            );
        }
    }

    public void testFromMapGoogleVertexAi_NoProvider_Success() {
        assertFromMapGoogleVertexAi_Success(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_LOCATION, TEST_MODEL_ID, null, null, null, TEST_RATE_LIMIT)
        );
    }

    public void testFromMapGoogleVertexAi_ProviderGoogle_Success() {
        assertFromMapGoogleVertexAi_Success(
            buildServiceSettingsMap(
                TEST_PROJECT_ID,
                TEST_LOCATION,
                TEST_MODEL_ID,
                null,
                null,
                GoogleModelGardenProvider.GOOGLE.toString(),
                TEST_RATE_LIMIT
            )
        );
    }

    public void testFromMapGoogleVertexAi_NoRateLimitInMap_UsesDefaultRateLimit_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_LOCATION, TEST_MODEL_ID, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            settings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    TEST_PROJECT_ID,
                    TEST_LOCATION,
                    TEST_MODEL_ID,
                    null,
                    null,
                    GoogleModelGardenProvider.GOOGLE,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    private static void assertFromMapGoogleVertexAi_Success(Map<String, Object> settingsMap) {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            settingsMap,
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            settings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    TEST_PROJECT_ID,
                    TEST_LOCATION,
                    TEST_MODEL_ID,
                    null,
                    null,
                    GoogleModelGardenProvider.GOOGLE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMapGoogleVertexAi_UrlPresent_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_LOCATION, TEST_MODEL_ID, TEST_URL, null, null, null),
            Strings.format("""
                Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=%s;""", TEST_URL, null)
        );
    }

    public void testFromMapGoogleVertexAi_StreamingUrlPresent_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(TEST_PROJECT_ID, TEST_LOCATION, TEST_MODEL_ID, null, TEST_STREAMING_URL, null, null),
            Strings.format(
                """
                    Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                    not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=%s;""",
                null,
                TEST_STREAMING_URL
            )
        );
    }

    public void testFromMapGoogleModelGarden_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(
                null,
                null,
                null,
                TEST_URL,
                TEST_STREAMING_URL,
                GoogleModelGardenProvider.ANTHROPIC.toString(),
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            settings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    createUri(TEST_URL),
                    createUri(TEST_STREAMING_URL),
                    GoogleModelGardenProvider.ANTHROPIC,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMapGoogleModelGarden_NoProvider_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(null, null, null, TEST_URL, TEST_STREAMING_URL, null, null),
            Strings.format(
                """
                    Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                    not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=%s;""",
                TEST_URL,
                TEST_STREAMING_URL
            )
        );
    }

    public void testFromMapGoogleModelGarden_GoogleProvider_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(null, null, null, TEST_URL, TEST_STREAMING_URL, GoogleModelGardenProvider.GOOGLE.toString(), null),
            Strings.format(
                """
                    Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                    not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=%s;""",
                TEST_URL,
                TEST_STREAMING_URL
            )
        );
    }

    public void testFromMapGoogleModelGarden_NoUrl_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(
                null,
                null,
                null,
                null,
                TEST_STREAMING_URL,
                GoogleModelGardenProvider.ANTHROPIC.toString(),
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            settings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    null,
                    createUri(TEST_STREAMING_URL),
                    GoogleModelGardenProvider.ANTHROPIC,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMapGoogleModelGarden_NoStreamingUrl_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(null, null, null, TEST_URL, null, GoogleModelGardenProvider.ANTHROPIC.toString(), TEST_RATE_LIMIT),
            ConfigurationParseContext.REQUEST
        );
        assertThat(
            settings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    createUri(TEST_URL),
                    null,
                    GoogleModelGardenProvider.ANTHROPIC,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMapGoogleModelGarden_NoUrls_Failure() {
        assertValidationFailure(
            buildServiceSettingsMap(null, null, null, null, null, GoogleModelGardenProvider.ANTHROPIC.toString(), null),
            Strings.format(
                """
                    Validation Failed: 1: Google Model Garden provider=%s selected. Either 'uri' or 'streaming_uri' must be provided;""",
                GoogleModelGardenProvider.ANTHROPIC.toString()
            )
        );
    }

    public void testFromMapGoogleVertexAi_NoModel_Failure() {
        assertValidationFailure(buildServiceSettingsMap(TEST_PROJECT_ID, TEST_LOCATION, null, null, null, null, null), Strings.format("""
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=%s, project_id=%s, model_id=%s;""", TEST_LOCATION, TEST_PROJECT_ID, null));
    }

    public void testFromMapGoogleVertexAi_NoLocation_Failure() {
        assertValidationFailure(buildServiceSettingsMap(TEST_PROJECT_ID, null, TEST_MODEL_ID, null, null, null, null), Strings.format("""
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=%s, project_id=%s, model_id=%s;""", null, TEST_PROJECT_ID, TEST_MODEL_ID));
    }

    public void testFromMapGoogleVertexAi_NoProject_Failure() {
        assertValidationFailure(buildServiceSettingsMap(null, TEST_LOCATION, TEST_MODEL_ID, null, null, null, null), Strings.format("""
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=%s, project_id=%s, model_id=%s;""", TEST_LOCATION, null, TEST_MODEL_ID));
    }

    public void testUpdateServiceSettings_GoogleVertexAi_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_URL,
            TEST_STREAMING_URL,
            GoogleModelGardenProvider.META.toString(),
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            GoogleModelGardenProvider.GOOGLE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_LOCATION,
                    INITIAL_TEST_MODEL_ID,
                    null,
                    null,
                    GoogleModelGardenProvider.GOOGLE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GoogleVertexAi_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            GoogleModelGardenProvider.GOOGLE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_GoogleModelGarden_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_MODEL_ID,
            TEST_URL,
            TEST_STREAMING_URL,
            GoogleModelGardenProvider.META.toString(),
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            null,
            null,
            null,
            createUri(INITIAL_TEST_URL),
            createUri(INITIAL_TEST_STREAMING_URL),
            GoogleModelGardenProvider.ANTHROPIC,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    createUri(INITIAL_TEST_URL),
                    createUri(INITIAL_TEST_STREAMING_URL),
                    GoogleModelGardenProvider.ANTHROPIC,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GoogleModelGarden_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            null,
            null,
            null,
            createUri(INITIAL_TEST_URL),
            createUri(INITIAL_TEST_STREAMING_URL),
            GoogleModelGardenProvider.ANTHROPIC,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    private static void assertValidationFailure(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiChatCompletionServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), is(expectedErrorMessage));
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    private static GoogleVertexAiChatCompletionServiceSettings createRandom() {
        return randomBoolean() ? createRandomWithGoogleVertexAiSettings() : createRandomWithGoogleModelGardenSettings();
    }

    private static GoogleVertexAiChatCompletionServiceSettings createRandomWithGoogleVertexAiSettings() {
        return new GoogleVertexAiChatCompletionServiceSettings(
            randomString(),
            randomString(),
            randomString(),
            null,
            null,
            randomFrom(GoogleModelGardenProvider.GOOGLE, null),
            new RateLimitSettings(randomIntBetween(1, DEFAULT_RATE_LIMIT))
        );
    }

    private static GoogleVertexAiChatCompletionServiceSettings createRandomWithGoogleModelGardenSettings() {
        URI optionalUri = createOptionalUri(randomOptionalString());
        return new GoogleVertexAiChatCompletionServiceSettings(
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            optionalUri,
            optionalUri == null ? createUri(randomString()) : createOptionalUri(randomOptionalString()),
            randomFrom(
                GoogleModelGardenProvider.ANTHROPIC,
                GoogleModelGardenProvider.META,
                GoogleModelGardenProvider.MISTRAL,
                GoogleModelGardenProvider.HUGGING_FACE,
                GoogleModelGardenProvider.AI21
            ),
            new RateLimitSettings(randomIntBetween(1, DEFAULT_RATE_LIMIT))
        );
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String projectId,
        @Nullable String locationId,
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String streamingUrl,
        @Nullable String provider,
        @Nullable Integer rateLimit
    ) {
        HashMap<String, Object> map = new HashMap<>();
        if (projectId != null) {
            map.put(PROJECT_ID, projectId);
        }
        if (locationId != null) {
            map.put(LOCATION, locationId);
        }
        if (modelId != null) {
            map.put(MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(URL, url);
        }
        if (streamingUrl != null) {
            map.put(STREAMING_URL_SETTING_NAME, streamingUrl);
        }
        if (provider != null) {
            map.put(PROVIDER_SETTING_NAME, provider);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
