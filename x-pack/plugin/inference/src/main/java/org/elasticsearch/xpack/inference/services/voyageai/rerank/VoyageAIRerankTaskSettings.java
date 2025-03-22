/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

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
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceFields.TRUNCATION;

/**
 * Defines the task settings for the VoyageAI rerank service.
 *
 */
public class VoyageAIRerankTaskSettings implements TaskSettings {

    public static final String NAME = "voyageai_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_K_DOCS_ONLY = "top_k";

    public static final VoyageAIRerankTaskSettings EMPTY_SETTINGS = new VoyageAIRerankTaskSettings(null, null, null);

    public static VoyageAIRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topKDocumentsOnly = extractOptionalPositiveInteger(
            map,
            TOP_K_DOCS_ONLY,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        Boolean truncation = extractOptionalBoolean(map, TRUNCATION, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topKDocumentsOnly, returnDocuments, truncation);
    }

    /**
     * Creates a new {@link VoyageAIRerankTaskSettings} by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link VoyageAIRerankTaskSettings}
     */
    public static VoyageAIRerankTaskSettings of(
        VoyageAIRerankTaskSettings originalSettings,
        VoyageAIRerankTaskSettings requestTaskSettings
    ) {
        return new VoyageAIRerankTaskSettings(
            requestTaskSettings.getTopKDocumentsOnly() != null
                ? requestTaskSettings.getTopKDocumentsOnly()
                : originalSettings.getTopKDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments(),
            requestTaskSettings.getTruncation() != null ? requestTaskSettings.getTruncation() : originalSettings.getTruncation()

        );
    }

    public static VoyageAIRerankTaskSettings of(Integer topKDocumentsOnly, Boolean returnDocuments, Boolean truncation) {
        return new VoyageAIRerankTaskSettings(topKDocumentsOnly, returnDocuments, truncation);
    }

    private final Integer topKDocumentsOnly;
    private final Boolean returnDocuments;
    private final Boolean truncation;

    public VoyageAIRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean(), in.readOptionalBoolean());
    }

    public VoyageAIRerankTaskSettings(
        @Nullable Integer topKDocumentsOnly,
        @Nullable Boolean doReturnDocuments,
        @Nullable Boolean truncation
    ) {
        this.topKDocumentsOnly = topKDocumentsOnly;
        this.returnDocuments = doReturnDocuments;
        this.truncation = truncation;
    }

    @Override
    public boolean isEmpty() {
        return topKDocumentsOnly == null && returnDocuments == null && truncation == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (topKDocumentsOnly != null) {
            builder.field(TOP_K_DOCS_ONLY, topKDocumentsOnly);
        }
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        if (truncation != null) {
            builder.field(TRUNCATION, truncation);
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
        return TransportVersions.VOYAGE_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topKDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalBoolean(truncation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoyageAIRerankTaskSettings that = (VoyageAIRerankTaskSettings) o;
        return Objects.equals(topKDocumentsOnly, that.topKDocumentsOnly)
            && Objects.equals(returnDocuments, that.returnDocuments)
            && Objects.equals(truncation, that.truncation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(truncation, returnDocuments, topKDocumentsOnly);
    }

    public Integer getTopKDocumentsOnly() {
        return topKDocumentsOnly;
    }

    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Boolean getTruncation() {
        return truncation;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        VoyageAIRerankTaskSettings updatedSettings = VoyageAIRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return VoyageAIRerankTaskSettings.of(this, updatedSettings);
    }
}
