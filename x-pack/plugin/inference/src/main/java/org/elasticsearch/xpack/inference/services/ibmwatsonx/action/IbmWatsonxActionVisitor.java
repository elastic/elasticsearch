/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.completion.IbmWatsonxChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;

import java.util.Map;

/**
 * Interface for creating {@link ExecutableAction} instances for Watsonx models.
 * <p>
 * This interface is used to create {@link ExecutableAction} instances for different types of Watsonx models, such as
 * {@link IbmWatsonxEmbeddingsModel} and {@link IbmWatsonxRerankModel} and {@link IbmWatsonxChatCompletionModel}.
 */
public interface IbmWatsonxActionVisitor {

    /**
     * Creates an {@link ExecutableAction} for the given {@link IbmWatsonxEmbeddingsModel}.
     *
     * @param model The model to create the action for.
     * @param taskSettings    The task settings to use.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(IbmWatsonxEmbeddingsModel model, Map<String, Object> taskSettings);

    /**
     * Creates an {@link ExecutableAction} for the given {@link IbmWatsonxRerankModel}.
     *
     * @param model The model to create the action for.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(IbmWatsonxRerankModel model, Map<String, Object> taskSettings);

    /**
     * Creates an {@link ExecutableAction} for the given {@link IbmWatsonxChatCompletionModel}.
     *
     * @param model The model to create the action for.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(IbmWatsonxChatCompletionModel model);
}
