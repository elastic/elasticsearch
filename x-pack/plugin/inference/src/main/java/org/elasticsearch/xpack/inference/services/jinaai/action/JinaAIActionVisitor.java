/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankModel;

import java.util.Map;

public interface JinaAIActionVisitor {
    ExecutableAction create(JinaAIEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(JinaAIRerankModel model, Map<String, Object> taskSettings);
}
