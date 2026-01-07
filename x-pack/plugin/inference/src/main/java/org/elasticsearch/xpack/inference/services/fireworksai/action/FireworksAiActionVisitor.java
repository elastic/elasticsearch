/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;

import java.util.Map;

/**
 * Visitor interface for creating executable actions for FireworksAI models.
 * Supports embeddings task type.
 */
public interface FireworksAiActionVisitor {
    /**
     * Creates an executable action for embeddings models.
     *
     * @param model        the embeddings model
     * @param taskSettings task-specific settings to override model defaults
     * @return an executable action for embeddings
     */
    ExecutableAction create(FireworksAiEmbeddingsModel model, Map<String, Object> taskSettings);
}
