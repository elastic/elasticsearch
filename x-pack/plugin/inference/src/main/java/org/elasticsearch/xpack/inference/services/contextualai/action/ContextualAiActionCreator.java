/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.action;

import static org.elasticsearch.core.Strings.format;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequest;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;

public class ContextualAiActionCreator {

    private static final Logger logger = LogManager.getLogger(ContextualAiActionCreator.class);
    
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public ContextualAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    public ExecutableAction create(ContextualAiRerankModel model, Map<String, Object> taskSettings, ResponseHandler rerankHandler) {
        // Debug logging to see what task settings we receive
        logger.debug("ContextualAiActionCreator - received taskSettings: {}", taskSettings);
        
        var overriddenModel = ContextualAiRerankModel.of(model, taskSettings);
        
        // Debug logging to see the parsed task settings
        logger.debug("ContextualAiActionCreator - overriddenModel task settings: {}", 
            overriddenModel.getTaskSettings() != null ? 
                "topN=" + overriddenModel.getTaskSettings().getTopN() + 
                ", returnDocs=" + overriddenModel.getTaskSettings().getReturnDocuments() +
                ", instruction=" + overriddenModel.getTaskSettings().getInstruction() : 
                "null");
                
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            rerankHandler,
            (rerankInput) -> {
                if (rerankInput == null) {
                    throw new IllegalArgumentException("Rerank input cannot be null");
                }
                String query = rerankInput.getQuery();
                if (query == null) {
                    throw new IllegalArgumentException("Query cannot be null for rerank request");
                }
                List<String> chunks = rerankInput.getChunks();
                if (chunks == null) {
                    throw new IllegalArgumentException("Documents/chunks cannot be null for rerank request");
                }
                return new ContextualAiRerankRequest(
                    query,
                    chunks,
                    rerankInput.getReturnDocuments(),
                    rerankInput.getTopN(),
                    overriddenModel.getTaskSettings() != null ? overriddenModel.getTaskSettings().getInstruction() : null,
                    overriddenModel
                );
            },
            QueryAndDocsInputs.class
        );

        var failedToSendRequestErrorMessage = buildErrorMessage(TaskType.RERANK, overriddenModel.getInferenceEntityId());
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format("Failed to send Contextual AI %s request from inference entity id [%s]", requestType.toString(), inferenceId);
    }
}