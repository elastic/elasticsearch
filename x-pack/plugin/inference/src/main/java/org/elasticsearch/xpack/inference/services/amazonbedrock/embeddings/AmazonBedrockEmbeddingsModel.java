/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.VALID_INTERNAL_INPUT_TYPE_VALUES;

public class AmazonBedrockEmbeddingsModel extends AmazonBedrockModel {
    static final String PROVIDER_WITH_TASK_TYPE = "cohere";
    static final EnumSet<InputType> VALID_REQUEST_VALUES = EnumSet.of(
        InputType.INGEST,
        InputType.SEARCH,
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INTERNAL_INGEST,
        InputType.INTERNAL_SEARCH
    );

    public static AmazonBedrockEmbeddingsModel of(
        AmazonBedrockEmbeddingsModel embeddingsModel,
        Map<String, Object> taskSettings,
        InputType inputType
    ) {
        if (taskSettings != null && taskSettings.isEmpty() == false) {
            // no task settings allowed
            var validationException = new ValidationException();
            validationException.addValidationError("Amazon Bedrock embeddings model cannot have task settings");
            throw validationException;
        }

        // InputType is only respected when provider=cohere for text embeddings
        ValidationException validationException = new ValidationException();
        if (Objects.equals(embeddingsModel.provider(), PROVIDER_WITH_TASK_TYPE) == false) {
            // this model does not accept input type parameter
            if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_INTERNAL_INPUT_TYPE_VALUES.contains(inputType) == false) {
                // throw validation exception if ingest type is specified
                validationException.addValidationError(
                    Strings.format(
                        "Invalid value [%s] received. [%s] is not allowed for provider [%s]",
                        inputType,
                        "input_type",
                        embeddingsModel.provider()
                    )
                );
            } else {
                return embeddingsModel;
            }
        } else {
            if (inputType != null && inputType != InputType.UNSPECIFIED && VALID_REQUEST_VALUES.contains(inputType) == false) {
                validationException.addValidationError(
                    Strings.format("Invalid value [%s] received. [%s] is not allowed", inputType, "input_type")
                );
            }
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockEmbeddingsModel(
            embeddingsModel,
            embeddingsModel.getServiceSettings(),
            inputType == InputType.UNSPECIFIED ? null : inputType
        );
    }

    private InputType inputType;

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AmazonBedrockEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            new EmptyTaskSettings(),
            chunkingSettings,
            AmazonBedrockSecretSettings.fromMap(secretSettings),
            null
        );
    }

    public AmazonBedrockEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        AmazonBedrockSecretSettings secrets,
        @Nullable InputType inputType
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings(), chunkingSettings),
            new ModelSecrets(secrets)
        );
        this.inputType = inputType;
    }

    public AmazonBedrockEmbeddingsModel(Model model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public AmazonBedrockEmbeddingsModel(Model model, ServiceSettings serviceSettings, InputType inputType) {
        this(model, serviceSettings);
        this.inputType = inputType;
    }

    @Override
    public ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings, InputType inputType) {
        return creator.create(this, taskSettings, inputType);
    }

    @Override
    public AmazonBedrockEmbeddingsServiceSettings getServiceSettings() {
        return (AmazonBedrockEmbeddingsServiceSettings) super.getServiceSettings();
    }

    public InputType getInputType() {
        return inputType;
    }
}
