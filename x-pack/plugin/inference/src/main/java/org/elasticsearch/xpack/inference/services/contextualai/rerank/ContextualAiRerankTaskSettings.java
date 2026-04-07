/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiTaskSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.ML_INFERENCE_CONTEXTUAL_AI_RETURN_DOCUMENTS_REMOVED;

public class ContextualAiRerankTaskSettings extends ContextualAiTaskSettings implements TopNProvider {

    public static final String NAME = "contextualai_rerank_task_settings";
    protected static final String TOP_N_FIELD = "top_n";
    protected static final String INSTRUCTION_FIELD = "instruction";

    public static final ContextualAiRerankTaskSettings EMPTY_SETTINGS = new ContextualAiRerankTaskSettings(null, null);

    public static ContextualAiRerankTaskSettings fromMap(Map<String, Object> map) {
        var validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        var topN = extractOptionalPositiveInteger(map, TOP_N_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        var instruction = extractOptionalString(map, INSTRUCTION_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);

        validationException.throwIfValidationErrorsExist();

        return new ContextualAiRerankTaskSettings(topN, instruction);
    }

    public static ContextualAiRerankTaskSettings of(
        ContextualAiRerankTaskSettings originalSettings,
        ContextualAiRerankTaskSettings requestSettings
    ) {
        var topN = requestSettings.getTopN() != null ? requestSettings.getTopN() : originalSettings.getTopN();
        var instruction = requestSettings.getInstruction() != null ? requestSettings.getInstruction() : originalSettings.getInstruction();

        // If none of the settings have changed, return the original settings to avoid unnecessary object creation
        if (Objects.equals(topN, originalSettings.getTopN()) && Objects.equals(instruction, originalSettings.getInstruction())) {
            return originalSettings;
        }

        return new ContextualAiRerankTaskSettings(topN, instruction);
    }

    private final Integer topN;
    private final String instruction;

    public ContextualAiRerankTaskSettings(@Nullable Integer topN, @Nullable String instruction) {
        this.topN = topN;
        this.instruction = instruction;
    }

    public ContextualAiRerankTaskSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ML_INFERENCE_CONTEXTUAL_AI_RETURN_DOCUMENTS_REMOVED) == false) {
            in.readOptionalBoolean();
        }
        this.topN = in.readOptionalVInt();
        this.instruction = in.readOptionalString();
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
        if (out.getTransportVersion().supports(ML_INFERENCE_CONTEXTUAL_AI_RETURN_DOCUMENTS_REMOVED) == false) {
            out.writeOptionalBoolean(null);
        }
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
        return Objects.equals(topN, that.topN) && Objects.equals(instruction, that.instruction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topN, instruction);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return fromMap(newSettings);
    }

    @Override
    public boolean isEmpty() {
        return this.equals(EMPTY_SETTINGS);
    }
}
