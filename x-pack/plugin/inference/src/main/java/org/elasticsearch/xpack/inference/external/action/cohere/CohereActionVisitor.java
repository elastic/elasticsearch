/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.cohere;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankModel;

import java.util.Map;

public interface CohereActionVisitor {
    ExecutableAction create(CohereEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType);

    ExecutableAction create(CohereRerankModel model, Map<String, Object> taskSettings);

    ExecutableAction create(CohereCompletionModel model, Map<String, Object> taskSettings);
}
