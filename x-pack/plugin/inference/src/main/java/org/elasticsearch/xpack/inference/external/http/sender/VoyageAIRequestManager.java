/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;

import java.util.Objects;

abstract class VoyageAIRequestManager extends BaseRequestManager {

    protected VoyageAIRequestManager(ThreadPool threadPool, VoyageAIModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int apiKeyHash) {
        public static RateLimitGrouping of(VoyageAIModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.apiKey().hashCode());
        }
    }
}
