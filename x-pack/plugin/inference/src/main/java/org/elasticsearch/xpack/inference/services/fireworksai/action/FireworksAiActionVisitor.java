/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;

import java.util.Map;

public interface FireworksAiActionVisitor {
    ExecutableAction create(FireworksAiEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(FireworksAiChatCompletionModel model, Map<String, Object> taskSettings);
}
