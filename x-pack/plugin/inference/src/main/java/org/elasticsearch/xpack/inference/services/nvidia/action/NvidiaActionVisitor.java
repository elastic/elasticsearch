/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModel;

import java.util.Map;

/**
 * Visitor interface for creating executable actions for Nvidia inference services.
 * This interface defines methods to create actions for embeddings, reranking and completion models.
 */
public interface NvidiaActionVisitor {

    /**
     * Creates an executable action for the given Nvidia embeddings model.
     *
     * @param model the Nvidia embeddings model
     * @param taskSettings the task settings for the embeddings model
     * @return an executable action for the embeddings model
     */
    ExecutableAction create(NvidiaEmbeddingsModel model, Map<String, Object> taskSettings);

    /**
     * Creates an executable action for the given Nvidia chat completion model.
     *
     * @param model the Nvidia chat completion model
     * @return an executable action for the chat completion model
     */
    ExecutableAction create(NvidiaChatCompletionModel model);

    /**
     * Creates an executable action for the given Nvidia rerank model.
     *
     * @param model The Nvidia rerank model
     * @return An executable action for the rerank model
     */
    ExecutableAction create(NvidiaRerankModel model);
}
