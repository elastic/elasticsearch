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
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
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
        testFromMapGoogleVertexAi_Success(Map.of("project_id", "my-project", "location", "us-central1", "model_id", "my-model"));
    }

    public void testFromMapGoogleVertexAi_ProviderGoogle_Success() {
        testFromMapGoogleVertexAi_Success(
            Map.of("project_id", "my-project", "location", "us-central1", "model_id", "my-model", "provider", "google")
        );
    }

    private static void testFromMapGoogleVertexAi_Success(Map<String, String> settingsMap) {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(settingsMap),
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.projectId(), is("my-project"));
        assertThat(settings.location(), is("us-central1"));
        assertThat(settings.modelId(), is("my-model"));
        assertThat(settings.provider(), is(GoogleModelGardenProvider.GOOGLE));
        assertNull(settings.streamingUri());
        assertNull(settings.uri());
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleVertexAi_UrlPresent_Failure() {
        testValidationFailure(Map.of("url", "url", "project_id", "my-project", "location", "us-central1", "model_id", "my-model"), """
            Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
            not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=url, streaming_uri=null;""");
    }

    public void testFromMapGoogleVertexAi_StreamingUrlPresent_Failure() {
        testValidationFailure(
            Map.of("streaming_url", "streaming_url", "project_id", "my-project", "location", "us-central1", "model_id", "my-model"),
            """
                Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
                not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=null, streaming_uri=streaming_url;"""
        );
    }

    public void testFromMapGoogleModelGarden_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of("url", "url", "streaming_url", "streaming_url", "provider", "anthropic")),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(GoogleModelGardenProvider.ANTHROPIC));
        assertThat(settings.uri().toString(), is("url"));
        assertThat(settings.streamingUri().toString(), is("streaming_url"));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoProvider_Failure() {
        testValidationFailure(Map.of("url", "url", "streaming_url", "streaming_url"), """
            Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
            not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=url, streaming_uri=streaming_url;""");
    }

    public void testFromMapGoogleModelGarden_GoogleProvider_Failure() {
        testValidationFailure(Map.of("url", "url", "streaming_url", "streaming_url", "provider", "google"), """
            Validation Failed: 1: 'provider' is either GOOGLE or null. For Google Vertex AI models 'uri' and 'streaming_uri' must \
            not be provided. Remove 'url' and 'streaming_url' fields. Provided values: uri=url, streaming_uri=streaming_url;""");
    }

    public void testFromMapGoogleModelGarden_NoUrl_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of("streaming_url", "streaming_url", "provider", "anthropic")),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(GoogleModelGardenProvider.ANTHROPIC));
        assertNull(settings.uri());
        assertThat(settings.streamingUri().toString(), is("streaming_url"));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoStreamingUrl_Success() {
        GoogleVertexAiChatCompletionServiceSettings settings = GoogleVertexAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of("url", "url", "provider", "anthropic")),
            ConfigurationParseContext.REQUEST
        );
        assertNull(settings.projectId());
        assertNull(settings.location());
        assertNull(settings.modelId());
        assertThat(settings.provider(), is(GoogleModelGardenProvider.ANTHROPIC));
        assertNull(settings.streamingUri());
        assertThat(settings.uri().toString(), is("url"));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(1000)));
    }

    public void testFromMapGoogleModelGarden_NoUrls_Failure() {
        testValidationFailure(Map.of("provider", "anthropic"), """
            Validation Failed: 1: Google Model Garden provider=anthropic selected. Either 'uri' or 'streaming_uri' must be provided;""");
    }

    public void testFromMapGoogleVertexAi_NoModel_Failure() {
        testValidationFailure(Map.of("project_id", "my-project", "location", "us-central1"), """
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=us-central1, project_id=my-project, model_id=null;""");
    }

    public void testFromMapGoogleVertexAi_NoLocation_Failure() {
        testValidationFailure(Map.of("project_id", "my-project", "model_id", "my-model"), """
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=null, project_id=my-project, model_id=my-model;""");
    }

    public void testFromMapGoogleVertexAi_NoProject_Failure() {
        testValidationFailure(Map.of("location", "us-central1", "model_id", "my-model"), """
            Validation Failed: 1: For Google Vertex AI models, you must provide 'location', 'project_id', and 'model_id'. \
            Provided values: location=us-central1, project_id=null, model_id=my-model;""");
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
                GoogleModelGardenProvider.ANTHROPIC,
                GoogleModelGardenProvider.META,
                GoogleModelGardenProvider.MISTRAL,
                GoogleModelGardenProvider.HUGGING_FACE,
                GoogleModelGardenProvider.AI21
            ),
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }
}
