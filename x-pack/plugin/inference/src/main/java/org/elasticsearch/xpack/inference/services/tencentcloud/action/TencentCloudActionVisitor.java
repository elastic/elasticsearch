/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;

import java.util.Map;

/**
 * Visitor interface for creating executable actions for TencentCloud inference models.
 * This interface defines methods to create actions for embeddings, rerank, and chat completion models, each taking the
 * request's task settings.
 */
public interface TencentCloudActionVisitor {

    /**
     * Creates an executable action for the given TencentCloud embeddings model.
     *
     * @param model the TencentCloud embeddings model
     * @param taskSettings the task-level settings for this request
     * @return an executable action for the embeddings model
     */
    ExecutableAction create(TencentCloudEmbeddingsModel model, Map<String, Object> taskSettings);

    /**
     * Creates an executable action for the given TencentCloud rerank model.
     *
     * @param model the TencentCloud rerank model
     * @param taskSettings the task-level settings for this request
     * @return an executable action for the rerank model
     */
    ExecutableAction create(TencentCloudRerankModel model, Map<String, Object> taskSettings);

    /**
     * Creates an executable action for the given TencentCloud chat completion model.
     *
     * @param model the TencentCloud chat completion model
     * @param taskSettings the task-level settings for this request
     * @return an executable action for the chat completion model
     */
    ExecutableAction create(TencentCloudChatCompletionModel model, Map<String, Object> taskSettings);
}
