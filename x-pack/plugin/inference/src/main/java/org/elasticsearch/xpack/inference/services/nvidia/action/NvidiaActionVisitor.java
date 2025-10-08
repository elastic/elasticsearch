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

import java.util.Map;

/**
 * Visitor interface for creating executable actions for Nvidia inference services.
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
}
