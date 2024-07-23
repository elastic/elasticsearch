/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elser.ElserModels;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class ElasticInferenceServiceSparseEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        ElasticInferenceServiceRateLimitServiceSettings {

    public static final String NAME = "elastic_inference_service_sparse_embeddings_service_settings";

    // TODO: to be determined
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1_000);

    public static ElasticInferenceServiceSparseEmbeddingsServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context
    ) {
        ValidationException validationException = new ValidationException();

        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        // TODO: max input tokens?
        // TODO: dims?

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            OpenAiService.NAME,
            context
        );

        // TODO: add "ELSERv2" or change in model-server?
        if (modelId != null && ElserModels.isValidModel(modelId) == false) {
            validationException.addValidationError("unknown ELSER model id [" + modelId + "]");
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, rateLimitSettings);
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public ElasticInferenceServiceSparseEmbeddingsServiceSettings(String modelId, RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public ElasticInferenceServiceSparseEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_EIS_INTEGRATION_ADDED;
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
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ElasticInferenceServiceSparseEmbeddingsServiceSettings that = (ElasticInferenceServiceSparseEmbeddingsServiceSettings) object;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
