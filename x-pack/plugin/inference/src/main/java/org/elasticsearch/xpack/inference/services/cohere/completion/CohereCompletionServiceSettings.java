/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR;

/**
 * Settings for the Cohere completion service.
 */
public class CohereCompletionServiceSettings extends FilteredXContentObject implements ServiceSettings {

    public static final String NAME = "cohere_completion_service_settings";

    /**
     * Creates an instance of {@link CohereCompletionServiceSettings} from a map of settings.
     *
     * @param map The map containing the settings.
     * @param context The context for configuration parsing.
     * @return the created {@link CohereCompletionServiceSettings}.
     * @throws ValidationException If there are validation errors in the provided settings.
     */
    public static CohereCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return new CohereCompletionServiceSettings(CohereCommonServiceSettings.fromMap(map, context));
    }

    private final CohereCommonServiceSettings commonSettings;

    public CohereCompletionServiceSettings(CohereCommonServiceSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    public CohereCompletionServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            // Old format: uri, modelId, rateLimitSettings, [apiVersion]
            var uri = createOptionalUri(in.readOptionalString());
            var modelId = in.readOptionalString();
            var rateLimitSettings = new RateLimitSettings(in);
            var apiVersion = in.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)
                ? in.readEnum(CohereCommonServiceSettings.CohereApiVersion.class)
                : CohereCommonServiceSettings.CohereApiVersion.V1;
            this.commonSettings = new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
        } else {
            this.commonSettings = new CohereCommonServiceSettings(in);
        }
    }

    CohereCommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    public CohereCommonServiceSettings.CohereApiVersion apiVersion() {
        return commonSettings.apiVersion();
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public CohereCompletionServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        var validationException = new ValidationException();
        var updated = commonSettings.update(serviceSettings, validationException);
        validationException.throwIfValidationErrorsExist();
        return new CohereCompletionServiceSettings(updated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            out.writeOptionalString(commonSettings.uri() != null ? commonSettings.uri().toString() : null);
            out.writeOptionalString(commonSettings.modelId());
            commonSettings.rateLimitSettings().writeTo(out);
            if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
                out.writeEnum(commonSettings.apiVersion());
            }
        } else {
            commonSettings.writeTo(out);
        }
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        CohereCompletionServiceSettings that = (CohereCompletionServiceSettings) object;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
