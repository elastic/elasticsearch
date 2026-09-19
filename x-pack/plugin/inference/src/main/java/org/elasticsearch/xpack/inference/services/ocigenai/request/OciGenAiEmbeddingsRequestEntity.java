/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Body of an OCI Generative AI {@code embedText} request.
 *
 * @param inputs          the texts to embed
 * @param serviceSettings the settings providing the compartment, serving mode and optional output dimensions
 * @param inputType       the resolved input type, or {@code null} to let the service use its default
 * @param truncation      the truncation strategy, or {@code null} to let the service use its default
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/EmbedTextDetails">EmbedTextDetails</a>
 */
public record OciGenAiEmbeddingsRequestEntity(
    List<String> inputs,
    OciGenAiEmbeddingsServiceSettings serviceSettings,
    @Nullable InputType inputType,
    @Nullable Truncation truncation
) implements ToXContentObject {

    static final String INPUTS_FIELD = "inputs";
    static final String INPUT_TYPE_FIELD = "inputType";
    static final String TRUNCATE_FIELD = "truncate";
    static final String OUTPUT_DIMENSIONS_FIELD = "outputDimensions";

    static final String SEARCH_DOCUMENT = "SEARCH_DOCUMENT";
    static final String SEARCH_QUERY = "SEARCH_QUERY";
    static final String CLASSIFICATION = "CLASSIFICATION";
    static final String CLUSTERING = "CLUSTERING";

    public OciGenAiEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(serviceSettings);
    }

    /**
     * Resolves the input type to send: an input type specified on the request takes precedence over the one configured in the task
     * settings.
     */
    public static InputType resolveInputType(@Nullable InputType requestInputType, OciGenAiEmbeddingsTaskSettings taskSettings) {
        if (InputType.isSpecified(requestInputType)) {
            return requestInputType;
        }
        return taskSettings.getInputType();
    }

    /**
     * Maps an Elasticsearch {@link InputType} to the OCI Generative AI {@code inputType} value.
     *
     * @return the OCI input type, or {@code null} if the input type has no OCI equivalent
     */
    @Nullable
    public static String convertToOciInputType(@Nullable InputType inputType) {
        if (inputType == null) {
            return null;
        }
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> SEARCH_DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> SEARCH_QUERY;
            case CLASSIFICATION -> CLASSIFICATION;
            case CLUSTERING -> CLUSTERING;
            default -> null;
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUTS_FIELD, inputs);
        OciGenAiRequestUtils.writeCompartmentAndServingMode(builder, serviceSettings);

        var ociInputType = convertToOciInputType(inputType);
        if (ociInputType != null) {
            builder.field(INPUT_TYPE_FIELD, ociInputType);
        }
        if (truncation != null) {
            builder.field(TRUNCATE_FIELD, truncation.name().toUpperCase(Locale.ROOT));
        }
        if (Boolean.TRUE.equals(serviceSettings.dimensionsSetByUser()) && serviceSettings.dimensions() != null) {
            builder.field(OUTPUT_DIMENSIONS_FIELD, serviceSettings.dimensions());
        }

        builder.endObject();
        return builder;
    }
}
