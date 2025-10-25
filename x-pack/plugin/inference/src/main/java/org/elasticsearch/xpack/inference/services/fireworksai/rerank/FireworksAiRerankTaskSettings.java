/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Defines the task settings for FireworksAI rerank models.
 * Supports top_n and return_documents parameters for reranking.
 */
public class FireworksAiRerankTaskSettings implements TaskSettings {

    public static final String NAME = "fireworksai_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";

    public static final FireworksAiRerankTaskSettings EMPTY_SETTINGS = new FireworksAiRerankTaskSettings(null, null);

    public static FireworksAiRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topN = extractOptionalPositiveInteger(map, TOP_N_DOCS_ONLY, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new FireworksAiRerankTaskSettings(returnDocuments, topN);
    }

    public static FireworksAiRerankTaskSettings of(
        FireworksAiRerankTaskSettings originalSettings,
        FireworksAiRerankTaskSettings requestSettings
    ) {
        var returnDocuments = requestSettings.getReturnDocuments() != null
            ? requestSettings.getReturnDocuments()
            : originalSettings.getReturnDocuments();
        var topN = requestSettings.getTopN() != null ? requestSettings.getTopN() : originalSettings.getTopN();

        return new FireworksAiRerankTaskSettings(returnDocuments, topN);
    }

    private final Boolean returnDocuments;
    private final Integer topN;

    public FireworksAiRerankTaskSettings(@Nullable Boolean returnDocuments, @Nullable Integer topN) {
        this.returnDocuments = returnDocuments;
        this.topN = topN;
    }

    public FireworksAiRerankTaskSettings(StreamInput in) throws IOException {
        this.returnDocuments = in.readOptionalBoolean();
        this.topN = in.readOptionalVInt();
    }

    @Nullable
    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    @Nullable
    public Integer getTopN() {
        return topN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalVInt(topN);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        if (topN != null) {
            builder.field(TOP_N_DOCS_ONLY, topN);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        FireworksAiRerankTaskSettings that = (FireworksAiRerankTaskSettings) object;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topN, that.topN);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topN);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings);
    }

    @Override
    public boolean isEmpty() {
        return returnDocuments == null && topN == null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }
}
