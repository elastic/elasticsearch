/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

public abstract class AmazonBedrockRequestManager implements RequestManager {

    protected final ThreadPool threadPool;
    protected final TimeValue timeout;
    private final AmazonBedrockModel baseModel;

    protected AmazonBedrockRequestManager(AmazonBedrockModel baseModel, ThreadPool threadPool, @Nullable TimeValue timeout) {
        this.baseModel = Objects.requireNonNull(baseModel);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.timeout = timeout;
    }

    @Override
    public String inferenceEntityId() {
        return baseModel.getInferenceEntityId();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return baseModel.rateLimitSettings();
    }

    record RateLimitGrouping(int keyHash) {
        public static AmazonBedrockRequestManager.RateLimitGrouping of(AmazonBedrockModel model) {
            Objects.requireNonNull(model);

            var awsSecretSettings = model.getSecretSettings();

            return new RateLimitGrouping(Objects.hash(awsSecretSettings.accessKey, awsSecretSettings.secretKey));
        }
    }

    @Override
    public Object rateLimitGrouping() {
        return RateLimitGrouping.of(this.baseModel);
    }
}
