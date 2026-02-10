/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxService;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.API_VERSION;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields.PROJECT_ID;

public class IbmWatsonxEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        IbmWatsonxRateLimitServiceSettings {

    public static final String NAME = "ibmwatsonx_embeddings_service_settings";

    /**
     * Rate limits are defined at
     * <a href="https://www.ibm.com/docs/en/watsonx/saas?topic=learning-watson-machine-plans">Watson Machine Learning plans</a>.
     * For the Lite plan, the limit is 120 requests per minute.
     */
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(120);

    public static IbmWatsonxEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();

        var modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var projectId = extractRequiredString(map, PROJECT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var url = extractUri(map, URL, validationException);
        var apiVersion = extractRequiredString(map, API_VERSION, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var dimensions = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            IbmWatsonxService.NAME,
            context
        );

        validationException.throwIfValidationErrorsExist();

        return new IbmWatsonxEmbeddingsServiceSettings(
            modelId,
            projectId,
            url,
            apiVersion,
            maxInputTokens,
            dimensions,
            similarity,
            rateLimitSettings
        );
    }

    public static URI extractUri(Map<String, Object> map, String fieldName, ValidationException validationException) {
        String parsedUrl = extractRequiredString(map, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return convertToUri(parsedUrl, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);
    }

    private final String modelId;

    private final String projectId;

    private final URI url;

    private final String apiVersion;

    private final RateLimitSettings rateLimitSettings;

    private final Integer dimensions;

    private final Integer maxInputTokens;

    private final SimilarityMeasure similarity;

    public IbmWatsonxEmbeddingsServiceSettings(
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarity,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.modelId = modelId;
        this.projectId = projectId;
        this.url = uri;
        this.apiVersion = apiVersion;
        this.maxInputTokens = maxInputTokens;
        this.dimensions = dimensions;
        this.similarity = similarity;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public IbmWatsonxEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.projectId = in.readString();
        this.url = createUri(in.readString());
        this.apiVersion = in.readString();
        this.maxInputTokens = in.readOptionalVInt();
        this.dimensions = in.readOptionalVInt();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings, TaskType taskType) {
        var validationException = new ValidationException();

        var extractedModelId = extractOptionalString(serviceSettings, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var extractedProjectId = extractOptionalString(
            serviceSettings,
            PROJECT_ID,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedUrl = extractOptionalUri(serviceSettings, URL, validationException);
        var extractedApiVersion = extractOptionalString(
            serviceSettings,
            API_VERSION,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var extractedMaxInputTokens = extractOptionalPositiveInteger(
            serviceSettings,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedSimilarity = extractSimilarity(serviceSettings, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var extractedDimensions = extractOptionalPositiveInteger(
            serviceSettings,
            DIMENSIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var extractedRateLimitSettings = RateLimitSettings.of(
            serviceSettings,
            this.rateLimitSettings,
            validationException,
            IbmWatsonxService.NAME,
            ConfigurationParseContext.REQUEST
        );

        validationException.throwIfValidationErrorsExist();

        return new IbmWatsonxEmbeddingsServiceSettings(
            extractedModelId != null ? extractedModelId : this.modelId,
            extractedProjectId != null ? extractedProjectId : this.projectId,
            extractedUrl != null ? extractedUrl : this.url,
            extractedApiVersion != null ? extractedApiVersion : this.apiVersion,
            extractedMaxInputTokens != null ? extractedMaxInputTokens : this.maxInputTokens,
            extractedDimensions != null ? extractedDimensions : this.dimensions,
            extractedSimilarity != null ? extractedSimilarity : this.similarity,
            extractedRateLimitSettings
        );
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String projectId() {
        return projectId;
    }

    public URI url() {
        return url;
    }

    public String apiVersion() {
        return apiVersion;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

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
        out.writeString(modelId);
        out.writeString(projectId);
        out.writeString(url.toString());
        out.writeString(apiVersion);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalEnum(similarity);
        rateLimitSettings.writeTo(out);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        builder.field(PROJECT_ID, projectId);
        builder.field(URL, url.toString());
        builder.field(API_VERSION, apiVersion);

        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }

        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        IbmWatsonxEmbeddingsServiceSettings that = (IbmWatsonxEmbeddingsServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(projectId, that.projectId)
            && Objects.equals(url, that.url)
            && Objects.equals(apiVersion, that.apiVersion)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && similarity == that.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, projectId, url, apiVersion, rateLimitSettings, dimensions, maxInputTokens, similarity);
    }
}
