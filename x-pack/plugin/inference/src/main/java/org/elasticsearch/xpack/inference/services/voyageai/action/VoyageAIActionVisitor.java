/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModel;

import java.util.Map;

public interface VoyageAIActionVisitor {
    ExecutableAction create(VoyageAIEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(VoyageAIRerankModel model, Map<String, Object> taskSettings);
}
