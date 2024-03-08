/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;

public class CohereRerankServiceSettings implements ServiceSettings {
    public static final String NAME = "cohere_embeddings_service_settings";

    static final String EMBEDDING_TYPE = "embedding_type";

    public static CohereRerankServiceSettings fromMap(Map<String, Object> map, boolean logDeprecations) {
        ValidationException validationException = new ValidationException();
        var commonServiceSettings = CohereServiceSettings.fromMap(map, logDeprecations);
        CohereRerankType embeddingTypes = extractOptionalEnum(
            map,
            EMBEDDING_TYPE,
            ModelConfigurations.SERVICE_SETTINGS,
            CohereRerankType::fromString,
            EnumSet.allOf(CohereRerankType.class),
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CohereRerankServiceSettings(commonServiceSettings, embeddingTypes);
    }

    private final CohereServiceSettings commonSettings;
    private final CohereRerankType embeddingType;

    public CohereRerankServiceSettings(CohereServiceSettings commonSettings, @Nullable CohereRerankType embeddingType) {
        this.commonSettings = commonSettings;
        this.embeddingType = embeddingType;
    }

    public CohereRerankServiceSettings(StreamInput in) throws IOException {
        commonSettings = new CohereServiceSettings(in);
        embeddingType = in.readOptionalEnum(CohereRerankType.class);
    }

    public CohereServiceSettings getCommonSettings() {
        return commonSettings;
    }

    public CohereRerankType getEmbeddingType() {
        return embeddingType;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        commonSettings.toXContentFragment(builder);
        builder.field(EMBEDDING_TYPE, embeddingType);

        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_COHERE_EMBEDDINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commonSettings.writeTo(out);
        out.writeOptionalEnum(embeddingType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereRerankServiceSettings that = (CohereRerankServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings) && Objects.equals(embeddingType, that.embeddingType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings, embeddingType);
    }
}
