/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;

public abstract class GoogleVertexAiRequestManager extends BaseRequestManager {

    GoogleVertexAiRequestManager(ThreadPool threadPool, GoogleVertexAiModel model, Object rateLimitGroup) {
        super(threadPool, model.getInferenceEntityId(), rateLimitGroup, model.rateLimitServiceSettings().rateLimitSettings());
    }
}
