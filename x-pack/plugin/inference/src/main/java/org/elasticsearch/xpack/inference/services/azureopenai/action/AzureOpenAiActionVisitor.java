/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.util.Map;

public interface AzureOpenAiActionVisitor {
    ExecutableAction create(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(AzureOpenAiCompletionModel model, Map<String, Object> taskSettings);
}
