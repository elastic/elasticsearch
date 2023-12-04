/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.SimilarityMeasure;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

/**
 * Defines the base settings for interacting with OpenAI.
 */
public class OpenAiServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_service_settings";

    public static final String URL = "url";
    public static final String ORGANIZATION = "organization_id";
    public static final String SIMILARITY = "similarity";
    public static final String DIMENSIONS = "dimensions";

    public static OpenAiServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String similarity = extractOptionalString(map, SIMILARITY, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);

        SimilarityMeasure similarityMeasure = null;
        if (similarity != null) {
            try {
                similarityMeasure = SimilarityMeasure.fromString(similarity);
            } catch (IllegalArgumentException iae) {
                validationException.addValidationError("Unknown similarity measure [" + similarity + "]");
            }
        }

        // Throw if any of the settings were empty strings or invalid
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        // the url is optional and only for testing
        if (url == null) {
            return new OpenAiServiceSettings((URI) null, organizationId, similarityMeasure, dims);
        }

        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiServiceSettings(uri, organizationId, similarityMeasure, dims);
    }

    private final URI uri;
    private final String organizationId;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;

    public OpenAiServiceSettings(
        @Nullable URI uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions
    ) {
        this.uri = uri;
        this.organizationId = organizationId;
        this.similarity = similarity;
        this.dimensions = dimensions;
    }

    public OpenAiServiceSettings(
        @Nullable String uri,
        @Nullable String organizationId,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions
    ) {
        this(createOptionalUri(uri), organizationId, similarity, dimensions);
    }

    private static URI createOptionalUri(String url) {
        if (url == null) {
            return null;
        }

        return createUri(url);
    }

    public OpenAiServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        organizationId = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_SERVICE_EMBEDDING_SIZE_ADDED)) {
            similarity = in.readOptionalEnum(SimilarityMeasure.class);
            dimensions = in.readOptionalVInt();
        } else {
            similarity = null;
            dimensions = null;
        }
    }

    public URI uri() {
        return uri;
    }

    public String organizationId() {
        return organizationId;
    }

    public SimilarityMeasure similarity() {
        return similarity;
    }

    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (organizationId != null) {
            builder.field(ORGANIZATION, organizationId);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_OPENAI_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(organizationId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_SERVICE_EMBEDDING_SIZE_ADDED)) {
            out.writeOptionalEnum(similarity);
            out.writeOptionalVInt(dimensions);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiServiceSettings that = (OpenAiServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(organizationId, that.organizationId)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, organizationId, similarity, dimensions);
    }
}
