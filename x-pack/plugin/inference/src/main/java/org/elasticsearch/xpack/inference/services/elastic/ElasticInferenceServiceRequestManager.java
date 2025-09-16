/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestMetadata;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.extractRequestMetadataFromThreadContext;

public abstract class ElasticInferenceServiceRequestManager extends BaseRequestManager {

    private final ElasticInferenceServiceRequestMetadata requestMetadata;

    protected ElasticInferenceServiceRequestManager(ThreadPool threadPool, ElasticInferenceServiceModel model) {
        super(threadPool, model.getInferenceEntityId(), RateLimitGrouping.of(model), model.rateLimitSettings());
        this.requestMetadata = extractRequestMetadataFromThreadContext(threadPool.getThreadContext());
    }

    public ElasticInferenceServiceRequestMetadata requestMetadata() {
        return requestMetadata;
    }

    record RateLimitGrouping(int modelIdHash) {
        public static RateLimitGrouping of(ElasticInferenceServiceModel model) {
            Objects.requireNonNull(model);

            return new RateLimitGrouping(model.rateLimitGroupingHash());
        }
    }
}
