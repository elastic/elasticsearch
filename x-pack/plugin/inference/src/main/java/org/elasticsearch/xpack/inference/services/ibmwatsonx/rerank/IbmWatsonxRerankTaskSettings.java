/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

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

public class IbmWatsonxRerankTaskSettings implements TaskSettings {

    public static final String NAME = "ibm_watsonx_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";
    public static final String TRUNCATE_INPUT_TOKENS = "truncate_input_tokens";

    static final IbmWatsonxRerankTaskSettings EMPTY_SETTINGS = new IbmWatsonxRerankTaskSettings(null, null, null);

    public static IbmWatsonxRerankTaskSettings fromMap(Map<String, Object> map) {
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
        Integer truncateInputTokens = extractOptionalPositiveInteger(
            map,
            TRUNCATE_INPUT_TOKENS,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topNDocumentsOnly, returnDocuments, truncateInputTokens);
    }

    /**
     * Creates a new {@link IbmWatsonxRerankTaskSettings}
     * by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link IbmWatsonxRerankTaskSettings}
     */
    public static IbmWatsonxRerankTaskSettings of(
        IbmWatsonxRerankTaskSettings originalSettings,
        IbmWatsonxRerankTaskSettings requestTaskSettings
    ) {
        return new IbmWatsonxRerankTaskSettings(
            requestTaskSettings.getTopNDocumentsOnly() != null
                ? requestTaskSettings.getTopNDocumentsOnly()
                : originalSettings.getTopNDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments(),
            requestTaskSettings.getTruncateInputTokens() != null
                ? requestTaskSettings.getTruncateInputTokens()
                : originalSettings.getTruncateInputTokens()
        );
    }

    public static IbmWatsonxRerankTaskSettings of(Integer topNDocumentsOnly, Boolean returnDocuments, Integer maxChunksPerDoc) {
        return new IbmWatsonxRerankTaskSettings(topNDocumentsOnly, returnDocuments, maxChunksPerDoc);
    }

    private final Integer topNDocumentsOnly;
    private final Boolean returnDocuments;
    private final Integer truncateInputTokens;

    public IbmWatsonxRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean(), in.readOptionalInt());
    }

    public IbmWatsonxRerankTaskSettings(
        @Nullable Integer topNDocumentsOnly,
        @Nullable Boolean doReturnDocuments,
        @Nullable Integer truncateInputTokens
    ) {
        this.topNDocumentsOnly = topNDocumentsOnly;
        this.returnDocuments = doReturnDocuments;
        this.truncateInputTokens = truncateInputTokens;
    }

    @Override
    public boolean isEmpty() {
        return topNDocumentsOnly == null && returnDocuments == null && truncateInputTokens == null;
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
        if (truncateInputTokens != null) {
            builder.field(TRUNCATE_INPUT_TOKENS, truncateInputTokens);
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
        return TransportVersions.ML_INFERENCE_IBM_WATSONX_RERANK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topNDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalInt(truncateInputTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IbmWatsonxRerankTaskSettings that = (IbmWatsonxRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments)
            && Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly)
            && Objects.equals(truncateInputTokens, that.truncateInputTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topNDocumentsOnly, truncateInputTokens);
    }

    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTruncateInputTokens() {
        return truncateInputTokens;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        IbmWatsonxRerankTaskSettings updatedSettings = IbmWatsonxRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return IbmWatsonxRerankTaskSettings.of(this, updatedSettings);
    }
}
