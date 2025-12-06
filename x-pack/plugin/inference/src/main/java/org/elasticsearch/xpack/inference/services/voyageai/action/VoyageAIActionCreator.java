/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIResponseHandler;
import org.elasticsearch.xpack.inference.services.voyageai.response.VoyageAIEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Factory for creating {@link ExecutableAction} instances for VoyageAI models.
 * Uses the factory pattern with strategy delegation to handle different model types.
 */
public class VoyageAIActionCreator {
    
    // Response handlers - kept for backward compatibility with tests
    public static final ResponseHandler EMBEDDINGS_HANDLER = new VoyageAIResponseHandler(
        "voyageai text embedding",
        VoyageAIEmbeddingsResponseEntity::fromResponse
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public VoyageAIActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    /**
     * Creates an ExecutableAction for any VoyageAI model type using the factory pattern.
     * This single method replaces the multiple overloaded create methods by delegating
     * to model-specific strategies.
     * 
     * @param model the VoyageAI model
     * @param taskSettings task-specific settings to override model defaults
     * @return an ExecutableAction configured for the specific model type
     */
    public <T extends VoyageAIModel> ExecutableAction create(T model, Map<String, Object> taskSettings) {
        Objects.requireNonNull(model, "Model cannot be null");
        Objects.requireNonNull(taskSettings, "Task settings cannot be null");
        
        // Get the appropriate strategy for this model type
        VoyageAIModelStrategy<T> strategy = VoyageAIModelStrategyFactory.getStrategy(model);
        
        // Use the strategy to create the overridden model and request manager
        T overriddenModel = strategy.createOverriddenModel(model, taskSettings);
        var requestManager = strategy.createRequestManager(overriddenModel, serviceComponents.threadPool());
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(strategy.getServiceName());
        
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }
}
