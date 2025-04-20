/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;

public class HuggingFaceServiceSettings extends FilteredXContentObject implements ServiceSettings, HuggingFaceRateLimitServiceSettings {
    public static final String NAME = "hugging_face_service_settings";

    // At the time of writing HuggingFace hasn't posted the default rate limit for inference endpoints so the value here is only a guess
    // 3000 requests per minute
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    public static HuggingFaceServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();
        var uri = extractUri(map, URL, validationException);

        SimilarityMeasure similarityMeasure = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = extractOptionalPositiveInteger(map, DIMENSIONS, ModelConfigurations.SERVICE_SETTINGS, validationException);
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
        return new HuggingFaceServiceSettings(uri, similarityMeasure, dims, maxInputTokens, rateLimitSettings);
    }

    public static URI extractUri(Map<String, Object> map, String fieldName, ValidationException validationException) {
        String parsedUrl = extractRequiredString(map, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return convertToUri(parsedUrl, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);
    }

    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;

    public HuggingFaceServiceSettings(URI uri) {
        this.uri = Objects.requireNonNull(uri);
        this.similarity = null;
        this.dimensions = null;
        this.maxInputTokens = null;
        rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
    }

    public HuggingFaceServiceSettings(
        URI uri,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.uri = Objects.requireNonNull(uri);
        this.similarity = similarityMeasure;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public HuggingFaceServiceSettings(String url) {
        this(createUri(url));
    }

    public HuggingFaceServiceSettings(StreamInput in) throws IOException {
        this.uri = createUri(in.readString());
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            similarity = in.readOptionalEnum(SimilarityMeasure.class);
            dimensions = in.readOptionalVInt();
            maxInputTokens = in.readOptionalVInt();
        } else {
            similarity = null;
            dimensions = null;
            maxInputTokens = null;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
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
        builder.field(URL, uri.toString());
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
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
        out.writeString(uri.toString());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalEnum(SimilarityMeasure.translateSimilarity(similarity, out.getTransportVersion()));
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings.writeTo(out);
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

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public String modelId() {
        return null;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuggingFaceServiceSettings that = (HuggingFaceServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, similarity, dimensions, maxInputTokens, rateLimitSettings);
    }
}
