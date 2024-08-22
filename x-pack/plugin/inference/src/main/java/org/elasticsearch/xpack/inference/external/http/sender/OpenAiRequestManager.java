/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

abstract class OpenAiRequestManager extends BaseRequestManager {

    protected OpenAiRequestManager(ThreadPool threadPool, OpenAiModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        super(
            threadPool,
            model.getInferenceEntityId(),
            RateLimitGrouping.of(model, uriBuilder),
            model.rateLimitServiceSettings().rateLimitSettings()
        );
    }

    record RateLimitGrouping(int accountHash, int modelIdHash) {
        public static RateLimitGrouping of(OpenAiModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(
                OpenAiAccount.of(model, uriBuilder).hashCode(),
                model.rateLimitServiceSettings().modelId().hashCode()
            );
        }
    }
}
