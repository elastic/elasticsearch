/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;

import java.util.Map;

public interface AmazonBedrockActionVisitor {
    ExecutableAction create(AmazonBedrockEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings);

    ExecutableAction create(AmazonBedrockChatCompletionModel completionModel, Map<String, Object> taskSettings);
}
