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

public interface LlamaActionVisitor {
    ExecutableAction create(LlamaEmbeddingsModel model);

    ExecutableAction create(LlamaChatCompletionModel model);
}
