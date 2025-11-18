/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;

import java.util.Objects;

abstract class AnthropicRequestManager extends BaseRequestManager {

    protected AnthropicRequestManager(ThreadPool threadPool, AnthropicModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int accountHash, int modelIdHash) {
        public static RateLimitGrouping of(AnthropicModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(AnthropicAccount.of(model).hashCode(), model.rateLimitServiceSettings().modelId().hashCode());
        }
    }
}
