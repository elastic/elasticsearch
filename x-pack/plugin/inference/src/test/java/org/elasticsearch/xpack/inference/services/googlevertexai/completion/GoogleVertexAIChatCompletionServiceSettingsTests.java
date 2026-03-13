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
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.Matchers;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils.ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAIChatCompletionServiceSettingsTests extends InferenceSettingsTestCase<
    GoogleVertexAiChatCompletionServiceSettings> {

    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-test-project-id";
    private static final String TEST_LOCATION = "test-location";
    private static final String INITIAL_TEST_LOCATION = "initial-test-location";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final URI TEST_STREAMING_URL = ServiceUtils.createUri("test-streaming-url");
    private static final URI INITIAL_TEST_STREAMING_URL = ServiceUtils.createUri("initial-test-streaming-url");
    private static final URI TEST_URL = ServiceUtils.createUri("test-url");
    private static final URI INITIAL_TEST_URL = ServiceUtils.createUri("initial-test-url");
    private static final GoogleModelGardenProvider TEST_PROVIDER = GoogleModelGardenProvider.ANTHROPIC;
    private static final GoogleModelGardenProvider INITIAL_TEST_PROVIDER = GoogleModelGardenProvider.MISTRAL;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

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

    public void testUpdateServiceSettings_DefaultProvider_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            null,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    TEST_PROJECT_ID,
                    TEST_LOCATION,
                    TEST_MODEL_ID,
                    null,
                    null,
                    null,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_DefaultProvider_EmptyMap_Success() {
        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            null,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_LOCATION,
                    INITIAL_TEST_MODEL_ID,
                    null,
                    null,
                    null,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GoogleProvider_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                GoogleModelGardenProvider.GOOGLE.toString()
            )
        );

        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            GoogleModelGardenProvider.GOOGLE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(
            serviceSettings,
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

    public void testUpdateServiceSettings_GoogleProvider_EmptyMap_Success() {
        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            INITIAL_TEST_PROJECT_ID,
            INITIAL_TEST_LOCATION,
            INITIAL_TEST_MODEL_ID,
            null,
            null,
            GoogleModelGardenProvider.GOOGLE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    INITIAL_TEST_PROJECT_ID,
                    INITIAL_TEST_LOCATION,
                    INITIAL_TEST_MODEL_ID,
                    null,
                    null,
                    GoogleModelGardenProvider.GOOGLE,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_NonGoogleProvider_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URL.toString(),
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                TEST_PROVIDER.toString()
            )
        );

        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            null,
            null,
            null,
            INITIAL_TEST_URL,
            INITIAL_TEST_STREAMING_URL,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    TEST_URL,
                    TEST_STREAMING_URL,
                    TEST_PROVIDER,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_NonGoogleProvider_NoUrls_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                TEST_PROVIDER.toString()
            )
        );

        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            null,
            null,
            null,
            INITIAL_TEST_URL,
            INITIAL_TEST_STREAMING_URL,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    TEST_URL,
                    TEST_STREAMING_URL,
                    TEST_PROVIDER,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_NonGoogleProvider_EmptyMap_Success() {
        var serviceSettings = new GoogleVertexAiChatCompletionServiceSettings(
            null,
            null,
            null,
            INITIAL_TEST_URL,
            INITIAL_TEST_STREAMING_URL,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new GoogleVertexAiChatCompletionServiceSettings(
                    null,
                    null,
                    null,
                    INITIAL_TEST_URL,
                    INITIAL_TEST_STREAMING_URL,
                    INITIAL_TEST_PROVIDER,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GoogleProvider_UrlPresent_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                ServiceFields.URL,
                TEST_URL.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                GoogleModelGardenProvider.GOOGLE.toString()
            )
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new GoogleVertexAiChatCompletionServiceSettings(
                INITIAL_TEST_PROJECT_ID,
                INITIAL_TEST_LOCATION,
                INITIAL_TEST_MODEL_ID,
                null,
                null,
                GoogleModelGardenProvider.GOOGLE,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ).updateServiceSettings(settingsMap, TaskType.COMPLETION)
        );

        assertThat(thrownException.getMessage(), is(Strings.format("""
            Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
            not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=null;""", TEST_URL)));
    }

    public void testUpdateServiceSettings_GoogleProvider_StreamingUrlPresent_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                GoogleModelGardenProvider.GOOGLE.toString()
            )
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new GoogleVertexAiChatCompletionServiceSettings(
                INITIAL_TEST_PROJECT_ID,
                INITIAL_TEST_LOCATION,
                INITIAL_TEST_MODEL_ID,
                null,
                null,
                GoogleModelGardenProvider.GOOGLE,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ).updateServiceSettings(settingsMap, TaskType.COMPLETION)
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    """
                        Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' \
                        must not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=null, streaming_uri=%s;""",
                    TEST_STREAMING_URL
                )
            )
        );
    }

    public void testUpdateServiceSettings_NoProvider_UrlPresent_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                ServiceFields.URL,
                TEST_URL.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new GoogleVertexAiChatCompletionServiceSettings(
                INITIAL_TEST_PROJECT_ID,
                INITIAL_TEST_LOCATION,
                INITIAL_TEST_MODEL_ID,
                null,
                null,
                null,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ).updateServiceSettings(settingsMap, TaskType.COMPLETION)
        );

        assertThat(thrownException.getMessage(), is(Strings.format("""
            Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
            not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=null;""", TEST_URL)));
    }

    public void testUpdateServiceSettings_NoProvider_StreamingUrlPresent_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> new GoogleVertexAiChatCompletionServiceSettings(
                INITIAL_TEST_PROJECT_ID,
                INITIAL_TEST_LOCATION,
                INITIAL_TEST_MODEL_ID,
                null,
                null,
                null,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            ).updateServiceSettings(settingsMap, TaskType.COMPLETION)
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    """
                        Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' \
                        must not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=null, streaming_uri=%s;""",
                    TEST_STREAMING_URL
                )
            )
        );
    }

    public void testFromMapGoogleVertexAi_NoProvider_Success() {
        testFromMapGoogleVertexAi_Success(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID
            )
        );
    }

    public void testFromMapGoogleVertexAi_ProviderGoogle_Success() {
        testFromMapGoogleVertexAi_Success(
            Map.of(
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                GoogleModelGardenProvider.GOOGLE.toString()
            )
        );
    }

    private static void testFromMapGoogleVertexAi_Success(Map<String, String> settingsMap) {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(settingsMap),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.projectId(), is(TEST_PROJECT_ID));
        assertThat(settings.location(), is(TEST_LOCATION));
        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertThat(settings.provider(), is(GoogleModelGardenProvider.GOOGLE));
        assertNull(settings.streamingUri());
        assertNull(settings.uri());
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleVertexAi_UrlPresent_Failure() {
        testValidationFailure(
            Map.of(
                ServiceFields.URL,
                TEST_URL.toString(),
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID
            ),
            Strings.format("""
                Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=%s, streaming_uri=null;""", TEST_URL)
        );
    }

    public void testFromMapGoogleVertexAi_StreamingUrlPresent_Failure() {
        testValidationFailure(
            Map.of(
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString(),
                GoogleVertexAiServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                GoogleVertexAiServiceFields.LOCATION,
                TEST_LOCATION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID
            ),
            Strings.format(
                """
                    Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                    not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=null, streaming_uri=%s;""",
                TEST_STREAMING_URL
            )
        );
    }

    public void testFromMapGoogleModelGarden_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    TEST_URL.toString(),
                    GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                    TEST_STREAMING_URL.toString(),
                    GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                    TEST_PROVIDER.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(TEST_PROVIDER));
        assertThat(settings.uri(), is(TEST_URL));
        assertThat(settings.streamingUri(), is(TEST_STREAMING_URL));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoProvider_Failure() {
        testValidationFailure(
            Map.of(
                ServiceFields.URL,
                TEST_URL.toString(),
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString()
            ),
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
        testValidationFailure(
            Map.of(
                ServiceFields.URL,
                TEST_URL.toString(),
                GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                TEST_STREAMING_URL.toString(),
                GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                GoogleModelGardenProvider.GOOGLE.toString()
            ),
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
            new HashMap<>(
                Map.of(
                    GoogleVertexAiServiceFields.STREAMING_URL_SETTING_NAME,
                    TEST_STREAMING_URL.toString(),
                    GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME,
                    TEST_PROVIDER.toString()
                )
            ),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(TEST_PROVIDER));
        assertNull(settings.uri());
        assertThat(settings.streamingUri(), is(TEST_STREAMING_URL));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoStreamingUrl_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(ServiceFields.URL, TEST_URL.toString(), GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME, TEST_PROVIDER.toString())
            ),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(TEST_PROVIDER));
        assertNull(settings.streamingUri());
        assertThat(settings.uri(), is(TEST_URL));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoUrls_Failure() {
        testValidationFailure(
            Map.of(GoogleVertexAiServiceFields.PROVIDER_SETTING_NAME, TEST_PROVIDER.toString()),
            Strings.format(
                """
                    Validation Failed: 1: Google Model Garden provider=%s selected. Either 'uri' or 'streaming_uri' must be provided;""",
                TEST_PROVIDER.toString()
            )
        );
    }

    public void testFromMapGoogleVertexAi_NoModel_Failure() {
        testValidationFailure(
            Map.of(GoogleVertexAiServiceFields.PROJECT_ID, TEST_PROJECT_ID, GoogleVertexAiServiceFields.LOCATION, TEST_LOCATION),
            Strings.format("""
                Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
                Provided values: location=%s, project_id=%s, model_id=null;""", TEST_LOCATION, TEST_PROJECT_ID)
        );
    }

    public void testFromMapGoogleVertexAi_NoLocation_Failure() {
        testValidationFailure(
            Map.of(GoogleVertexAiServiceFields.PROJECT_ID, TEST_PROJECT_ID, ServiceFields.MODEL_ID, TEST_MODEL_ID),
            Strings.format("""
                Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
                Provided values: location=null, project_id=%s, model_id=%s;""", TEST_PROJECT_ID, TEST_MODEL_ID)
        );
    }

    public void testFromMapGoogleVertexAi_NoProject_Failure() {
        testValidationFailure(
            Map.of(GoogleVertexAiServiceFields.LOCATION, TEST_LOCATION, ServiceFields.MODEL_ID, TEST_MODEL_ID),
            Strings.format("""
                Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
                Provided values: location=%s, project_id=null, model_id=%s;""", TEST_LOCATION, TEST_MODEL_ID)
        );
    }

    private static void testValidationFailure(Map<String, String> taskSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiChatCompletionServiceSettings.fromMap(new HashMap<>(taskSettingsMap), ConfigurationParseContext.REQUEST)
        );
        assertThat(thrownException.getMessage(), Matchers.is(expectedErrorMessage));
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
            new RateLimitSettings(randomIntBetween(1, 1000))
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
                TEST_PROVIDER,
                GoogleModelGardenProvider.META,
                GoogleModelGardenProvider.MISTRAL,
                GoogleModelGardenProvider.HUGGING_FACE,
                GoogleModelGardenProvider.AI21
            ),
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }
}
