/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.groq.completion.GroqChatCompletionModel;

/**
 * Visitor interface for creating executable actions for Groq inference services.
 * This interface defines methods to create actions for completion models.
 */
public interface GroqActionVisitor {

    /**
     * Creates an executable action for the given Groq chat completion model.
     *
     * @param model the Groq chat completion model
     * @return an executable action for the chat completion model
     */
    ExecutableAction create(GroqChatCompletionModel model);
}
