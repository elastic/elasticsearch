/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

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

/**
 * Defines the task settings for the JinaAI rerank service.
 *
 */
public class JinaAIRerankTaskSettings implements TaskSettings {

    public static final String NAME = "jinaai_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";

    public static final JinaAIRerankTaskSettings EMPTY_SETTINGS = new JinaAIRerankTaskSettings(null, null);

    public static JinaAIRerankTaskSettings fromMap(Map<String, Object> map) {
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
     * Creates a new {@link JinaAIRerankTaskSettings} by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link JinaAIRerankTaskSettings}
     */
    public static JinaAIRerankTaskSettings of(JinaAIRerankTaskSettings originalSettings, JinaAIRerankTaskSettings requestTaskSettings) {
        return new JinaAIRerankTaskSettings(
            requestTaskSettings.getTopNDocumentsOnly() != null
                ? requestTaskSettings.getTopNDocumentsOnly()
                : originalSettings.getTopNDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments()
        );
    }

    public static JinaAIRerankTaskSettings of(Integer topNDocumentsOnly, Boolean returnDocuments) {
        return new JinaAIRerankTaskSettings(topNDocumentsOnly, returnDocuments);
    }

    private final Integer topNDocumentsOnly;
    private final Boolean returnDocuments;

    public JinaAIRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean());
    }

    public JinaAIRerankTaskSettings(@Nullable Integer topNDocumentsOnly, @Nullable Boolean doReturnDocuments) {
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
        return TransportVersions.JINA_AI_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topNDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIRerankTaskSettings that = (JinaAIRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments) && Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topNDocumentsOnly);
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

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        JinaAIRerankTaskSettings updatedSettings = JinaAIRerankTaskSettings.fromMap(new HashMap<>(newSettings));
        return JinaAIRerankTaskSettings.of(this, updatedSettings);
    }
}
