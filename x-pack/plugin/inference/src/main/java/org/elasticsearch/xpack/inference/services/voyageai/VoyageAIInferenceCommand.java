/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionCreator;

import java.util.Map;
import java.util.Objects;

/**
 * Concrete implementation of VoyageAI command that encapsulates the execution logic.
 * This command pattern implementation allows for better organization, logging, and potential
 * features like retry mechanisms, batching, or macro operations.
 */
public class VoyageAIInferenceCommand implements VoyageAICommand {
    
    private final VoyageAIModel model;
    private final Map<String, Object> taskSettings;
    private final VoyageAIActionCreator actionCreator;
    private final CommandType commandType;
    private final String description;

    public VoyageAIInferenceCommand(
        VoyageAIModel model,
        Map<String, Object> taskSettings,
        VoyageAIActionCreator actionCreator,
        CommandType commandType
    ) {
        this.model = Objects.requireNonNull(model, "Model cannot be null");
        this.taskSettings = Objects.requireNonNull(taskSettings, "Task settings cannot be null");
        this.actionCreator = Objects.requireNonNull(actionCreator, "Action creator cannot be null");
        this.commandType = Objects.requireNonNull(commandType, "Command type cannot be null");
        this.description = String.format("VoyageAI %s inference for model: %s", 
                                        commandType.getName(), 
                                        model.getServiceSettings().modelId());
    }

    @Override
    public void execute(InferenceInputs inputs, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        try {
            ExecutableAction action = actionCreator.create(model, taskSettings);
            action.execute(inputs, timeout, listener);
        } catch (Exception e) {
            listener.onFailure(new RuntimeException("Failed to execute " + description, e));
        }
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public CommandType getCommandType() {
        return commandType;
    }

    /**
     * Builder pattern for creating VoyageAI inference commands.
     * This provides a fluent interface for command creation while ensuring all required fields are set.
     */
    public static class Builder {
        private VoyageAIModel model;
        private Map<String, Object> taskSettings;
        private VoyageAIActionCreator actionCreator;
        private CommandType commandType;

        public Builder withModel(VoyageAIModel model) {
            this.model = model;
            return this;
        }

        public Builder withTaskSettings(Map<String, Object> taskSettings) {
            this.taskSettings = taskSettings;
            return this;
        }

        public Builder withActionCreator(VoyageAIActionCreator actionCreator) {
            this.actionCreator = actionCreator;
            return this;
        }

        public Builder withCommandType(CommandType commandType) {
            this.commandType = commandType;
            return this;
        }

        /**
         * Automatically determines command type based on model type.
         */
        public Builder withAutoDetectedCommandType() {
            if (model == null) {
                throw new IllegalStateException("Model must be set before auto-detecting command type");
            }
            
            String modelClass = model.getClass().getSimpleName();
            if (modelClass.contains("Embeddings")) {
                if (modelClass.contains("Multimodal")) {
                    this.commandType = CommandType.MULTIMODAL_EMBEDDINGS;
                } else if (modelClass.contains("Contextual")) {
                    this.commandType = CommandType.CONTEXTUAL_EMBEDDINGS;
                } else {
                    this.commandType = CommandType.TEXT_EMBEDDINGS;
                }
            } else if (modelClass.contains("Rerank")) {
                this.commandType = CommandType.RERANK;
            } else {
                throw new IllegalArgumentException("Cannot auto-detect command type for model: " + modelClass);
            }
            return this;
        }

        public VoyageAIInferenceCommand build() {
            validateRequiredFields();
            return new VoyageAIInferenceCommand(model, taskSettings, actionCreator, commandType);
        }

        private void validateRequiredFields() {
            if (model == null) throw new IllegalStateException("Model is required");
            if (taskSettings == null) throw new IllegalStateException("Task settings are required");
            if (actionCreator == null) throw new IllegalStateException("Action creator is required");
            if (commandType == null) throw new IllegalStateException("Command type is required");
        }
    }

    /**
     * Factory method for creating commands with auto-detected type.
     */
    public static VoyageAIInferenceCommand create(
        VoyageAIModel model,
        Map<String, Object> taskSettings,
        VoyageAIActionCreator actionCreator
    ) {
        return new Builder()
            .withModel(model)
            .withTaskSettings(taskSettings)
            .withActionCreator(actionCreator)
            .withAutoDetectedCommandType()
            .build();
    }
}