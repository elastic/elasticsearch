/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIModel;

import java.util.Objects;

abstract class JinaAIRequestManager extends BaseRequestManager {

    protected JinaAIRequestManager(ThreadPool threadPool, JinaAIModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int apiKeyHash) {
        public static RateLimitGrouping of(JinaAIModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.apiKey().hashCode());
        }
    }
}
