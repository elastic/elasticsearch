/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModel;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;

import java.util.Map;

/**
 * Interface for creating {@link ExecutableAction} instances for Mistral models.
 * <p>
 * This interface is used to create {@link ExecutableAction} instances for different types of Mistral models, such as
 * {@link MistralEmbeddingsModel} and {@link MistralChatCompletionModel}.
 */
public interface MistralActionVisitor {

    /**
     * Creates an {@link ExecutableAction} for the given {@link MistralEmbeddingsModel}.
     *
     * @param embeddingsModel The model to create the action for.
     * @param taskSettings    The task settings to use.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(MistralEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings);

    /**
     * Creates an {@link ExecutableAction} for the given {@link MistralChatCompletionModel}.
     *
     * @param chatCompletionModel The model to create the action for.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(MistralChatCompletionModel chatCompletionModel);
}
