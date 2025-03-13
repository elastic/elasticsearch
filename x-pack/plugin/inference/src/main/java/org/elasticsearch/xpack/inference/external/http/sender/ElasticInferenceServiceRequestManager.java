/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceRequestMetadata;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

public abstract class ElasticInferenceServiceRequestManager extends BaseRequestManager {

    private final ElasticInferenceServiceRequestMetadata requestMetadata;

    protected ElasticInferenceServiceRequestManager(ThreadPool threadPool, ElasticInferenceServiceModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitServiceSettings().rateLimitSettings());
        this.requestMetadata = extractRequestMetadataFromThreadContext(threadPool.getThreadContext());
    }

    public ElasticInferenceServiceRequestMetadata requestMetadata() {
        return requestMetadata;
    }

    record RateLimitGrouping(int modelIdHash) {
        public static RateLimitGrouping of(ElasticInferenceServiceModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.rateLimitServiceSettings().modelId().hashCode());
        }
    }
}
