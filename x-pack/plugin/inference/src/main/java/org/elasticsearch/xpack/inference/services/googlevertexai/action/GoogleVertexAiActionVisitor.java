/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;

import java.util.Map;

public interface GoogleVertexAiActionVisitor {

    ExecutableAction create(GoogleVertexAiEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(GoogleVertexAiRerankModel model, Map<String, Object> taskSettings);
}
