/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class ContextualAiRerankTaskSettings implements TaskSettings, TopNProvider {

    public static final String NAME = "contextualai_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";
    public static final String INSTRUCTION = "instruction";

    // Default hardcoded instruction for reranking
    private static final String DEFAULT_INSTRUCTION = "Rerank the given documents based on their relevance to the query.";

    public static final ContextualAiRerankTaskSettings EMPTY_SETTINGS = new ContextualAiRerankTaskSettings(null, null, null);

    public static ContextualAiRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topN = extractOptionalPositiveInteger(map, TOP_N_DOCS_ONLY, ModelConfigurations.TASK_SETTINGS, validationException);
        String instruction = ServiceUtils.extractOptionalString(map, INSTRUCTION, ModelConfigurations.TASK_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
    }

    public static ContextualAiRerankTaskSettings of(
        ContextualAiRerankTaskSettings originalSettings,
        ContextualAiRerankTaskSettings requestSettings
    ) {
        var returnDocuments = requestSettings.getReturnDocuments() != null
            ? requestSettings.getReturnDocuments()
            : originalSettings.getReturnDocuments();
        var topN = requestSettings.getTopN() != null ? requestSettings.getTopN() : originalSettings.getTopN();
        var instruction = requestSettings.getInstruction() != null ? requestSettings.getInstruction() : originalSettings.getInstruction();

        return new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
    }

    private final Boolean returnDocuments;
    private final Integer topN;
    private final String instruction;

    public ContextualAiRerankTaskSettings(@Nullable Boolean returnDocuments, @Nullable Integer topN, @Nullable String instruction) {
        this.returnDocuments = returnDocuments;
        this.topN = topN;
        this.instruction = instruction;
    }

    public ContextualAiRerankTaskSettings(StreamInput in) throws IOException {
        this.returnDocuments = in.readOptionalBoolean();
        this.topN = in.readOptionalVInt();
        this.instruction = in.readOptionalString();
    }

    @Nullable
    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    @Override
    @Nullable
    public Integer getTopN() {
        return topN;
    }

    // Return custom instruction if provided, otherwise use default
    public String getInstruction() {
        return instruction != null ? instruction : DEFAULT_INSTRUCTION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalVInt(topN);
        out.writeOptionalString(instruction);
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
        if (instruction != null) {
            builder.field(INSTRUCTION, instruction);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ContextualAiRerankTaskSettings that = (ContextualAiRerankTaskSettings) object;
        return Objects.equals(returnDocuments, that.returnDocuments)
            && Objects.equals(topN, that.topN)
            && Objects.equals(instruction, that.instruction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topN, instruction);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings);
    }

    @Override
    public boolean isEmpty() {
        return returnDocuments == null && topN == null && instruction == null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }
}
