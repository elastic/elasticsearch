/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class CohereServiceSettings extends FilteredXContentObject implements ServiceSettings, CohereRateLimitServiceSettings {

    public static final String NAME = "cohere_service_settings";
    public static final String OLD_MODEL_ID_FIELD = "model";
    public static final String MODEL_ID = "model_id";
    public static final String API_VERSION = "api_version";
    public static final String MODEL_REQUIRED_FOR_V2_API = "The [service_settings.model_id] field is required for the Cohere V2 API.";

    public enum CohereApiVersion {
        V1,
        V2;

        public static CohereApiVersion fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
    }

    private static final Logger logger = LogManager.getLogger(CohereServiceSettings.class);
    // Production key rate limits for all endpoints: https://docs.cohere.com/docs/going-live#production-key-specifications
    // 10K requests a minute
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String oldModelId = extractOptionalString(map, OLD_MODEL_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            CohereService.NAME,
            context
        );

        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (context == ConfigurationParseContext.REQUEST && oldModelId != null) {
            logger.info("The cohere [service_settings.model] field is deprecated. Please use [service_settings.model_id] instead.");
        }

        var resolvedModelId = modelId(oldModelId, modelId);
        var apiVersion = apiVersionFromMap(map, context, validationException);
        if (apiVersion == CohereApiVersion.V2) {
            if (resolvedModelId == null) {
                validationException.addValidationError(MODEL_REQUIRED_FOR_V2_API);
            }
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereServiceSettings(uri, similarity, dims, maxInputTokens, resolvedModelId, rateLimitSettings, apiVersion);
    }

    public static CohereApiVersion apiVersionFromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        return switch (context) {
            case REQUEST -> CohereApiVersion.V2; // new endpoints all use the V2 API.
            case PERSISTENT -> {
                var apiVersion = ServiceUtils.extractOptionalEnum(
                    map,
                    API_VERSION,
                    ModelConfigurations.SERVICE_SETTINGS,
                    CohereApiVersion::fromString,
                    EnumSet.allOf(CohereApiVersion.class),
                    validationException
                );

                if (apiVersion == null) {
                    yield CohereApiVersion.V1; // If the API version is not persisted then it must be V1
                } else {
                    yield apiVersion;
                }
            }
        };
    }

    private static String modelId(@Nullable String model, @Nullable String modelId) {
        return modelId != null ? modelId : model;
    }

    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;
    private final CohereApiVersion apiVersion;

    public CohereServiceSettings(
        @Nullable URI uri,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this.uri = uri;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.apiVersion = apiVersion;
    }

    public CohereServiceSettings(
        @Nullable String url,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings,
        CohereApiVersion apiVersion
    ) {
        this(createOptionalUri(url), similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }

    public CohereServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
        dimensions = in.readOptionalVInt();
        maxInputTokens = in.readOptionalVInt();
        modelId = in.readOptionalString();

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_COHERE_API_VERSION_8_19)) {
            this.apiVersion = in.readEnum(CohereServiceSettings.CohereApiVersion.class);
        } else {
            this.apiVersion = CohereServiceSettings.CohereApiVersion.V1;
        }
    }

    // should only be used for testing, public because it's accessed outside of the package
    public CohereServiceSettings(CohereApiVersion apiVersion) {
        this((URI) null, null, null, null, null, null, apiVersion);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public CohereApiVersion apiVersion() {
        return apiVersion;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        toXContentFragmentOfExposedFields(builder, params);
        return builder.field(API_VERSION, apiVersion); // API version is persisted but not exposed to the user
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalString(modelId);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings.writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_COHERE_API_VERSION_8_19)) {
            out.writeEnum(apiVersion);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereServiceSettings that = (CohereServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && apiVersion == that.apiVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, similarity, dimensions, maxInputTokens, modelId, rateLimitSettings, apiVersion);
    }
}
