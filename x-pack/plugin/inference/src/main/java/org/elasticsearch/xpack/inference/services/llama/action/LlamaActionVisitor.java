/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModel;

/**
 * Visitor interface for creating executable actions for Llama inference models.
 * This interface defines methods to create actions for both embeddings and chat completion models.
 */
public interface LlamaActionVisitor {
    /**
     * Creates an executable action for the given Llama embeddings model.
     *
     * @param model the Llama embeddings model
     * @return an executable action for the embeddings model
     */
    ExecutableAction create(LlamaEmbeddingsModel model);

    /**
     * Creates an executable action for the given Llama chat completion model.
     *
     * @param model the Llama chat completion model
     * @return an executable action for the chat completion model
     */
    ExecutableAction create(LlamaChatCompletionModel model);
}
