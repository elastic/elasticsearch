/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank.IbmWatsonxRerankModel;

import java.util.Map;

public interface IbmWatsonxActionVisitor {
    ExecutableAction create(IbmWatsonxEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(IbmWatsonxRerankModel model, Map<String, Object> taskSettings);
}
