/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.CohereModel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

abstract class CohereRequestManager extends BaseRequestManager {

    protected CohereRequestManager(ThreadPool threadPool, CohereModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model, uriBuilder));
    }

    record RateLimitGrouping(CohereAccount account) {
        public static RateLimitGrouping of(CohereModel model, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(CohereAccount.of(model, uriBuilder));
        }

        public RateLimitGrouping {
            Objects.requireNonNull(account);
        }
    }
}
