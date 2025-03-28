/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiModel;

import java.util.Objects;

public abstract class GoogleVertexAiRequestManager extends BaseRequestManager {

    GoogleVertexAiRequestManager(ThreadPool threadPool, GoogleVertexAiModel model, Object rateLimitGroup) {
        super(
            threadPool,
            model.getInferenceEntityId(),
            GoogleVertexAiRequestManager.RateLimitGrouping.of(model),
            model.rateLimitServiceSettings().rateLimitSettings(),
            model.getConfigurations().getService(),
            model.getConfigurations().getTaskType()
        );
    }

    record RateLimitGrouping(int modelIdHash) {
        public static GoogleVertexAiRequestManager.RateLimitGrouping of(GoogleVertexAiModel model) {
            Objects.requireNonNull(model);

            return new GoogleVertexAiRequestManager.RateLimitGrouping(model.rateLimitServiceSettings().modelId().hashCode());
        }
    }
}
