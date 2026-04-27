/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Service settings for DeepSeek's chat completion model, containing the model ID and an optional custom URI for the API endpoint.
 * @param modelId the DeepSeek model ID to use for chat completion requests
 * @param uri an optional custom URI for the DeepSeek chat completion API endpoint; if not provided, the default URI will be used
 */
public record DeepSeekServiceSettings(String modelId, URI uri) implements ServiceSettings {
    public static final String NAME = "deep_seek_service_settings";
    private static final TransportVersion ML_INFERENCE_DEEPSEEK = TransportVersion.fromName("ml_inference_deepseek");

    public DeepSeekServiceSettings {
        Objects.requireNonNull(modelId);
    }

    public DeepSeekServiceSettings(StreamInput in) throws IOException {
        this(in.readString(), in.readOptional(url -> URI.create(url.readString())));
    }

    /**
     * Builds a {@link DeepSeekServiceSettings} object from the given service settings map, validating required fields and formats.
     *
     * @param serviceSettings the service settings map
     * @return a {@link DeepSeekServiceSettings} object
     */
    static DeepSeekServiceSettings fromMap(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var model = extractRequiredString(serviceSettings, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(serviceSettings, URL, validationException);
        validationException.throwIfValidationErrorsExist();
        return new DeepSeekServiceSettings(model, uri);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_DEEPSEEK;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_DEEPSEEK);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalString(uri != null ? uri.toString() : null);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, modelId);
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        return builder.endObject();
    }

    /**
     * DeepSeek service settings are immutable, so this method simply returns the current instance without applying any updates.
     * @param serviceSettings a map with the new service settings
     * @return the current instance of {@link DeepSeekServiceSettings}, since the settings are immutable and cannot be updated
     */
    @Override
    public DeepSeekServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        // DeepSeek service settings don't have any mutable fields
        return this;
    }
}
