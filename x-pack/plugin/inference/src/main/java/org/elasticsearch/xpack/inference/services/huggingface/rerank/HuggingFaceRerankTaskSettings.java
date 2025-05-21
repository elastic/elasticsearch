/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class HuggingFaceRerankTaskSettings implements TaskSettings {

    public static final String NAME = "hugging_face_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";

    static final HuggingFaceRerankTaskSettings EMPTY_SETTINGS = new HuggingFaceRerankTaskSettings(null, null);

    public static HuggingFaceRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topNDocumentsOnly = extractOptionalPositiveInteger(
            map,
            TOP_N_DOCS_ONLY,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topNDocumentsOnly, returnDocuments);
    }

    /**
     * Creates a new {@link HuggingFaceRerankTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link HuggingFaceRerankTaskSettings}
     */
    public static HuggingFaceRerankTaskSettings of(
        HuggingFaceRerankTaskSettings originalSettings,
        HuggingFaceRerankTaskSettings requestTaskSettings
    ) {
        return new HuggingFaceRerankTaskSettings(
            requestTaskSettings.getTopNDocumentsOnly() != null
                ? requestTaskSettings.getTopNDocumentsOnly()
                : originalSettings.getTopNDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    public static HuggingFaceRerankTaskSettings of(Integer topNDocumentsOnly, Boolean returnDocuments) {
        return new HuggingFaceRerankTaskSettings(topNDocumentsOnly, returnDocuments);
    }

    private final Integer topNDocumentsOnly;
    private final Boolean returnDocuments;

    public HuggingFaceRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalVInt(), in.readOptionalBoolean());
    }

    public HuggingFaceRerankTaskSettings(@Nullable Integer topNDocumentsOnly, @Nullable Boolean doReturnDocuments) {
        this.topNDocumentsOnly = topNDocumentsOnly;
        this.returnDocuments = doReturnDocuments;
    }

    @Override
    public boolean isEmpty() {
        return topNDocumentsOnly == null && returnDocuments == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (topNDocumentsOnly != null) {
            builder.field(TOP_N_DOCS_ONLY, topNDocumentsOnly);
        }
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_HUGGING_FACE_RERANK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(topNDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HuggingFaceRerankTaskSettings that = (HuggingFaceRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topNDocumentsOnly);
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        HuggingFaceRerankTaskSettings updatedSettings = HuggingFaceRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return HuggingFaceRerankTaskSettings.of(this, updatedSettings);
    }
}
