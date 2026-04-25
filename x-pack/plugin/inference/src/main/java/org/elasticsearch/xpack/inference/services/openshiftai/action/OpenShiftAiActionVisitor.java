/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModel;

import java.util.Map;

/**
 * Visitor interface for creating executable actions for OpenShift AI inference models.
 * This interface defines methods to create actions for embeddings, reranking and completion models.
 */
public interface OpenShiftAiActionVisitor {

    /**
     * Creates an executable action for the given OpenShift AI embeddings model.
     *
     * @param model The OpenShift AI embeddings model.
     * @return An executable action for the embeddings model.
     */
    ExecutableAction create(OpenShiftAiEmbeddingsModel model);

    /**
     * Creates an executable action for the given OpenShift AI chat completion model.
     *
     * @param model The OpenShift AI chat completion model.
     * @return An executable action for the chat completion model.
     */
    ExecutableAction create(OpenShiftAiChatCompletionModel model);

    /**
     * Creates an executable action for the given OpenShift AI rerank model.
     *
     * @param model The OpenShift AI rerank model.
     * @param taskSettings The task settings for the rerank action.
     * @return An executable action for the rerank model.
     */
    ExecutableAction create(OpenShiftAiRerankModel model, Map<String, Object> taskSettings);
}
