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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.INFERENCE_CONTEXTUAL_AI_ADDED;

public class ContextualAiRerankTaskSettings implements TopNProvider, TaskSettings {

    public static final String NAME = "contextualai_rerank_task_settings";

    protected static final String RETURN_DOCUMENTS_FIELD = "return_documents";
    protected static final String TOP_N_FIELD = "top_n";
    protected static final String INSTRUCTION_FIELD = "instruction";

    public static final ContextualAiRerankTaskSettings EMPTY_SETTINGS = new ContextualAiRerankTaskSettings(null, null, null);

    public static ContextualAiRerankTaskSettings fromMap(Map<String, Object> map) {
        var validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS_FIELD, validationException);
        var topN = extractOptionalPositiveInteger(map, TOP_N_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        var instruction = extractOptionalString(map, INSTRUCTION_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new ContextualAiRerankTaskSettings(returnDocuments, topN, instruction);
    }

    public static ContextualAiRerankTaskSettings of(
        ContextualAiRerankTaskSettings originalSettings,
        ContextualAiRerankTaskSettings requestSettings
    ) {
        var returnDocumentsToUse = requestSettings.getReturnDocuments() != null
            ? requestSettings.getReturnDocuments()
            : originalSettings.getReturnDocuments();
        var topNToUse = requestSettings.getTopN() != null ? requestSettings.getTopN() : originalSettings.getTopN();
        var instructionToUse = requestSettings.getInstruction() != null
            ? requestSettings.getInstruction()
            : originalSettings.getInstruction();

        // If none of the settings have changed, return the original settings to avoid unnecessary object creation
        if (Objects.equals(returnDocumentsToUse, originalSettings.getReturnDocuments())
            && Objects.equals(topNToUse, originalSettings.getTopN())
            && Objects.equals(instructionToUse, originalSettings.getInstruction())) {
            return originalSettings;
        }

        return new ContextualAiRerankTaskSettings(returnDocumentsToUse, topNToUse, instructionToUse);
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

    @Nullable
    public String getInstruction() {
        return instruction;
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
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }
        if (instruction != null) {
            builder.field(INSTRUCTION_FIELD, instruction);
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
        return this.equals(EMPTY_SETTINGS);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_CONTEXTUAL_AI_ADDED;
    }
}
