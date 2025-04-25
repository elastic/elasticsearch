/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings.extractUri;

/**
 * Settings for the Hugging Face chat completion service.
 * <p>
 * This class contains the settings required to configure a Hugging Face chat completion service, including the model ID, URL, maximum input
 * tokens, and rate limit settings.
 * </p>
 */
public class HuggingFaceChatCompletionServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        HuggingFaceRateLimitServiceSettings {

    public static final String NAME = "hugging_face_completion_service_settings";
    public static final String URL = "url";
    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so the value his is only a guess
    // 3000 requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);
    private static final int DEFAULT_TOKEN_LIMIT = 512;

    /**
     * Creates a new instance of {@link HuggingFaceChatCompletionServiceSettings} from a map of settings.
     * @param map the map of settings
     * @param context the context for parsing the settings
     * @return a new instance of {@link HuggingFaceChatCompletionServiceSettings}
     */
    public static HuggingFaceChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var uri = extractUri(map, URL, validationException);

        Integer maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            HuggingFaceService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return new HuggingFaceChatCompletionServiceSettings(modelId, uri, maxInputTokens, rateLimitSettings);
    }

    private final String modelId;
    private final URI uri;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceChatCompletionServiceSettings(
        String modelId,
        String url,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(modelId, createUri(url), maxInputTokens, rateLimitSettings);
    }

    public HuggingFaceChatCompletionServiceSettings(
        String modelId,
        URI uri,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = modelId;
        this.uri = uri;
        this.maxInputTokens = Objects.requireNonNullElse(maxInputTokens, DEFAULT_TOKEN_LIMIT);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    /**
     * Creates a new instance of {@link HuggingFaceChatCompletionServiceSettings} from a stream input.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public HuggingFaceChatCompletionServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.uri = createUri(in.readString());

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.rateLimitSettings = new RateLimitSettings(in);
            this.maxInputTokens = in.readOptionalVInt();
        } else {
            this.rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
            this.maxInputTokens = DEFAULT_TOKEN_LIMIT;
        }
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public URI uri() {
        return uri;
    }

    public int maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();

        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);

        builder.field(URL, uri.toString());
        builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
        out.writeOptionalVInt(maxInputTokens);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        HuggingFaceChatCompletionServiceSettings that = (HuggingFaceChatCompletionServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, maxInputTokens, rateLimitSettings);
    }

}
