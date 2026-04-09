/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;

import java.util.Objects;

abstract class CohereRequestManager extends BaseRequestManager {

    protected CohereRequestManager(ThreadPool threadPool, CohereModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int apiKeyHash) {
        public static RateLimitGrouping of(CohereModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.apiKey().hashCode());
        }
    }
}
