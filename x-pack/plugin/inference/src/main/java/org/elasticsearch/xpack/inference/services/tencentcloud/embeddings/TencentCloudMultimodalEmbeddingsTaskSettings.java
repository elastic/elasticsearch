/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.SettingsScope;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;

/**
 * Task settings for the TencentCloud {@code embedding} task: an {@code input_type} and a {@code prompt_name}, which select the text
 * prefix applied by models that support one (EmbeddingGemma-2).
 */
public class TencentCloudMultimodalEmbeddingsTaskSettings implements TaskSettings {

    public static final String NAME = "tencentcloud_multimodal_embeddings_task_settings";
    public static final TencentCloudMultimodalEmbeddingsTaskSettings EMPTY_SETTINGS = new TencentCloudMultimodalEmbeddingsTaskSettings(
        null,
        null
    );

    public static final String INPUT_TYPE = "input_type";
    public static final String PROMPT_NAME = "prompt_name";

    static final EnumSet<InputType> VALID_INPUT_TYPE_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    /**
     * The task prefixes accepted by the multimodal embeddings API. Values are case-sensitive.
     */
    public static final Set<String> VALID_PROMPT_NAMES = Set.of(
        "SearchQuery",
        "Document",
        "document",
        "query",
        "Retrieval",
        "Retrieval-query",
        "Retrieval-document",
        "CodeRetrieval",
        "Clustering",
        "Classification",
        "STS",
        "SentenceSimilarity",
        "QuestionAnswering",
        "BitextMining",
        "Reranking",
        "FactChecking",
        "InstructionRetrieval",
        "MultilabelClassification",
        "PairClassification",
        "Summarization"
    );

    public static TencentCloudMultimodalEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        InputType inputType = extractOptionalEnum(
            map,
            INPUT_TYPE,
            SettingsScope.TASK_SETTINGS,
            InputType::fromString,
            VALID_INPUT_TYPE_VALUES,
            validationException
        );
        String promptName = extractOptionalString(map, PROMPT_NAME, SettingsScope.TASK_SETTINGS, validationException);
        if (promptName != null && VALID_PROMPT_NAMES.contains(promptName) == false) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] Invalid value [%s] received. [%s] must be one of %s",
                    SettingsScope.TASK_SETTINGS,
                    promptName,
                    PROMPT_NAME,
                    VALID_PROMPT_NAMES.stream().sorted().toList()
                )
            );
        }

        validationException.throwIfValidationErrorsExist();

        return new TencentCloudMultimodalEmbeddingsTaskSettings(inputType, promptName);
    }

    /**
     * Creates task settings where any field set in {@code requestTaskSettings} overrides the field in {@code originalSettings}.
     */
    public static TencentCloudMultimodalEmbeddingsTaskSettings of(
        TencentCloudMultimodalEmbeddingsTaskSettings originalSettings,
        TencentCloudMultimodalEmbeddingsTaskSettings requestTaskSettings
    ) {
        return new TencentCloudMultimodalEmbeddingsTaskSettings(
            requestTaskSettings.inputType != null ? requestTaskSettings.inputType : originalSettings.inputType,
            requestTaskSettings.promptName != null ? requestTaskSettings.promptName : originalSettings.promptName
        );
    }

    @Nullable
    private final InputType inputType;
    @Nullable
    private final String promptName;

    public TencentCloudMultimodalEmbeddingsTaskSettings(@Nullable InputType inputType, @Nullable String promptName) {
        this.inputType = inputType;
        this.promptName = promptName;
    }

    public TencentCloudMultimodalEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.inputType = in.readOptionalEnum(InputType.class);
        this.promptName = in.readOptionalString();
    }

    @Nullable
    public InputType getInputType() {
        return inputType;
    }

    @Nullable
    public String getPromptName() {
        return promptName;
    }

    /**
     * Resolves the {@code prompt_name} to send. An explicit {@code prompt_name} wins over the request input type, which wins over
     * the {@code input_type} in these settings.
     *
     * @param requestInputType the input type of the inference request, may be {@code null} or {@link InputType#UNSPECIFIED}
     * @return the prompt name, or {@code null} when none applies
     */
    @Nullable
    public String resolvePromptName(@Nullable InputType requestInputType) {
        if (promptName != null) {
            return promptName;
        }
        var inputTypeToUse = InputType.isSpecified(requestInputType) ? requestInputType : inputType;
        if (inputTypeToUse == null) {
            return null;
        }
        return switch (inputTypeToUse) {
            case SEARCH, INTERNAL_SEARCH -> "SearchQuery";
            case INGEST, INTERNAL_INGEST -> "Document";
            case CLUSTERING -> "Clustering";
            case CLASSIFICATION -> "Classification";
            case UNSPECIFIED -> null;
        };
    }

    @Override
    public boolean isEmpty() {
        return inputType == null && promptName == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputType != null) {
            builder.field(INPUT_TYPE, inputType);
        }
        if (promptName != null) {
            builder.field(PROMPT_NAME, promptName);
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
        return TencentCloudService.TENCENT_CLOUD_EMBEDDING_TASK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(inputType);
        out.writeOptionalString(promptName);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(new HashMap<>(newSettings)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TencentCloudMultimodalEmbeddingsTaskSettings that = (TencentCloudMultimodalEmbeddingsTaskSettings) o;
        return inputType == that.inputType && Objects.equals(promptName, that.promptName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputType, promptName);
    }
}
