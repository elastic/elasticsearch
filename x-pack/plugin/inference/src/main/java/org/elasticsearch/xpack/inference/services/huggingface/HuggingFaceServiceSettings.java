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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class HuggingFaceServiceSettings implements ServiceSettings {
    public static final String NAME = "hugging_face_service_settings";

    public static HuggingFaceServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var uri = extractUri(map, URL, validationException);

        SimilarityMeasure similarityMeasure = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return new HuggingFaceServiceSettings(uri, similarityMeasure, dims, maxInputTokens);
    }

    public static URI extractUri(Map<String, Object> map, String fieldName, ValidationException validationException) {
        String parsedUrl = extractRequiredString(map, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);

        return convertToUri(parsedUrl, fieldName, ModelConfigurations.SERVICE_SETTINGS, validationException);
    }

    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;

    public HuggingFaceServiceSettings(URI uri) {
        this.uri = Objects.requireNonNull(uri);
        this.similarity = null;
        this.dimensions = null;
        this.maxInputTokens = null;
    }

    public HuggingFaceServiceSettings(
        URI uri,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens
    ) {
        this.uri = Objects.requireNonNull(uri);
        this.similarity = similarityMeasure;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
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
            out.writeOptionalEnum(similarity);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
        }
    }

    public URI uri() {
        return uri;
    }

    public SimilarityMeasure similarity() {
        return similarity;
    }

    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuggingFaceServiceSettings that = (HuggingFaceServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && similarity == that.similarity
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, similarity, dimensions, maxInputTokens);
    }
}
