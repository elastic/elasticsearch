/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModel;

/**
 * Interface for creating {@link ExecutableAction} instances for AI21 models {@link Ai21ChatCompletionModel}.
 */
public interface Ai21ActionVisitor {

    /**
     * Creates an {@link ExecutableAction} for the given {@link Ai21ChatCompletionModel}.
     *
     * @param chatCompletionModel The model to create the action for.
     * @return An {@link ExecutableAction} for the given model.
     */
    ExecutableAction create(Ai21ChatCompletionModel chatCompletionModel);
}
