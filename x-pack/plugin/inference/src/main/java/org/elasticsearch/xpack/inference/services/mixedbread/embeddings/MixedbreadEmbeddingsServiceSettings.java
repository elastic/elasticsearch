/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadServiceSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

/**
 * Settings for the Mixedbread embeddings service.
 * This class encapsulates the configuration settings required to use Mixedbread for generating embeddings.
 */
public class MixedbreadEmbeddingsServiceSettings extends MixedbreadServiceSettings {
    public static final String NAME = "mixedbread_embeddings_service_settings";
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(MixedbreadUtils.HOST)
        .setPathSegments(MixedbreadUtils.VERSION_1, MixedbreadUtils.EMBEDDINGS_PATH);

    private final Integer dimensions;
    private final String encodingFormat;
    private final SimilarityMeasure similarity;
    private final Integer maxInputTokens;
    private final Boolean dimensionsSetByUser;

    /**
     * Creates a new instance of {@link MixedbreadEmbeddingsServiceSettings} from a map of settings.
     *
     * @param map the map containing the settings
     * @param context the context for parsing configuration settings
     * @return a new instance of {@link MixedbreadEmbeddingsServiceSettings}
     * @throws ValidationException if any required fields are missing or invalid
     */
    public static MixedbreadEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var validationException = new ValidationException();
        var commonServiceSettings = extractMixedbreadCommonServiceSettings(map, context, validationException);

        var dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var encodingFormat = extractOptionalString(
            map,
            MixedbreadUtils.ENCODING_FORMAT_FIELD,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var maxInputTokens = extractOptionalPositiveInteger(
            map,
            MAX_INPUT_TOKENS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        Boolean dimensionsSetByUser = extractOptionalBoolean(map, ServiceFields.DIMENSIONS_SET_BY_USER, validationException);

        switch (context) {
            case REQUEST -> {
                if (dimensionsSetByUser != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }

                if (dims != null) {
                    validationException.addValidationError(
                        ServiceUtils.invalidSettingError(DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
                dimensionsSetByUser = false;
            }
            case PERSISTENT -> {
                if (dimensionsSetByUser == null) {
                    validationException.addValidationError(
                        InferenceUtils.missingSettingErrorMsg(ServiceFields.DIMENSIONS_SET_BY_USER, ModelConfigurations.SERVICE_SETTINGS)
                    );
                }
            }
        }

        validationException.throwIfValidationErrorsExist();

        return new MixedbreadEmbeddingsServiceSettings(
            commonServiceSettings.model(),
            commonServiceSettings.uri(),
            dims,
            encodingFormat,
            similarity,
            maxInputTokens,
            commonServiceSettings.rateLimitSettings(),
            dimensionsSetByUser
        );
    }

    /**
     * Constructs a new instance of {@link MixedbreadEmbeddingsServiceSettings} from a StreamInput.
     *
     * @param in the {@link StreamInput} to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public MixedbreadEmbeddingsServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.dimensions = in.readOptionalVInt();
        this.encodingFormat = in.readOptionalString();
        this.similarity = in.readOptionalEnum(SimilarityMeasure.class);
        this.maxInputTokens = in.readOptionalVInt();
        this.dimensionsSetByUser = in.readBoolean();
    }

    @Override
    protected URI buildDefaultUri() throws URISyntaxException {
        return DEFAULT_URI_BUILDER.build();
    }

    /**
     * Constructs a new instance of {@link MixedbreadEmbeddingsServiceSettings} with the specified parameters.
     *
     * @param modelId the model identifier
     * @param uri the URI of the Mixedbread service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param encodingFormat specifies the encoding format of the embeddings, can be null
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadEmbeddingsServiceSettings(
        String modelId,
        @Nullable URI uri,
        @Nullable Integer dimensions,
        @Nullable String encodingFormat,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings,
        Boolean dimensionsSetByUser
    ) {
        super(modelId, uri, rateLimitSettings);
        this.dimensions = dimensions;
        this.encodingFormat = encodingFormat;
        this.similarity = similarity;
        this.maxInputTokens = maxInputTokens;
        this.dimensionsSetByUser = dimensionsSetByUser;
    }

    /**
     * Constructs a new instance of {@link MixedbreadEmbeddingsServiceSettings} with the specified parameters.
     *
     * @param modelId the model identifier
     * @param url the URL of the Mixedbread service
     * @param dimensions the number of dimensions for the embeddings, can be null
     * @param similarity the similarity measure to use, can be null
     * @param maxInputTokens the maximum number of input tokens, can be null
     * @param rateLimitSettings the rate limit settings for the service, can be null
     */
    public MixedbreadEmbeddingsServiceSettings(
        String modelId,
        @Nullable String url,
        @Nullable Integer dimensions,
        @Nullable String encodingFormat,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings,
        Boolean dimensionsSetByUser
    ) {
        this(modelId, createOptionalUri(url), dimensions, encodingFormat,
            similarity, maxInputTokens, rateLimitSettings, dimensionsSetByUser);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Integer dimensions() {
        return this.dimensions;
    }

    public String encodingFormat() {
        return this.encodingFormat;
    }

    @Override
    public SimilarityMeasure similarity() {
        return this.similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    /**
     * Returns the maximum number of input tokens allowed for this service.
     *
     * @return the maximum input tokens, or null if not specified
     */
    public Integer maxInputTokens() {
        return this.maxInputTokens;
    }

    /**
     * Returns the rate limit settings for this service.
     *
     * @return the rate limit settings, never null
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalString(encodingFormat);
        out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
        out.writeOptionalVInt(maxInputTokens);
        out.writeBoolean(dimensionsSetByUser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);
        builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (encodingFormat != null) {
            builder.field(MixedbreadUtils.ENCODING_FORMAT_FIELD, encodingFormat);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadEmbeddingsServiceSettings that = (MixedbreadEmbeddingsServiceSettings) o;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(uri, that.uri)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(encodingFormat, that.encodingFormat)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, uri, dimensions, encodingFormat, maxInputTokens, similarity, rateLimitSettings, dimensionsSetByUser);
    }

}
