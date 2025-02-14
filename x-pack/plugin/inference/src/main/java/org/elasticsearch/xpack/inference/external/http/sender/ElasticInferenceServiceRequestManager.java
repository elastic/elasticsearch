/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;

import java.util.Objects;

public abstract class ElasticInferenceServiceRequestManager extends BaseRequestManager {

    protected ElasticInferenceServiceRequestManager(ThreadPool threadPool, ElasticInferenceServiceModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
    }

    record RateLimitGrouping(int modelIdHash) {
        public static RateLimitGrouping of(ElasticInferenceServiceModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.rateLimitServiceSettings().modelId().hashCode());
        }
    }
}
