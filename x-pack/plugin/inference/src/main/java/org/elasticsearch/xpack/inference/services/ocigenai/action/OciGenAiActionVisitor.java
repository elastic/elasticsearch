/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModel;

import java.util.Map;

/**
 * Creates {@link ExecutableAction} instances for the OCI Generative AI models.
 */
public interface OciGenAiActionVisitor {

    ExecutableAction create(OciGenAiEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(OciGenAiRerankModel model, Map<String, Object> taskSettings);

    /**
     * Creates the action of the legacy {@code completion} task; the {@code chat_completion} task is served directly by the service.
     */
    ExecutableAction create(OciGenAiChatCompletionModel model);
}
