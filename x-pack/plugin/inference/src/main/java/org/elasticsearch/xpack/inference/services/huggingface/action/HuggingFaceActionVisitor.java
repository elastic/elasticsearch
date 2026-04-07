/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModel;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModel;

public interface HuggingFaceActionVisitor {
    ExecutableAction create(HuggingFaceRerankModel model);

    ExecutableAction create(HuggingFaceEmbeddingsModel model);

    ExecutableAction create(HuggingFaceElserModel model);

    ExecutableAction create(HuggingFaceChatCompletionModel model);
}
