/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

abstract class BaseRequestManager implements RequestManager {
    private final ThreadPool threadPool;
    private final String inferenceEntityId;
    // It's possible that two inference endpoints have the same information defining the group but have different
    // rate limits then they should be in different groups otherwise whoever initially created the group will set
    // the rate and the other inference endpoint's rate will be ignored
    private final EndpointGrouping endpointGrouping;
    private final RateLimitSettings rateLimitSettings;
    private final String service;
    private final TaskType taskType;

    BaseRequestManager(
        ThreadPool threadPool,
        String inferenceEntityId,
        Object rateLimitGroup,
        RateLimitSettings rateLimitSettings,
        String service,
        TaskType taskType
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);

        Objects.requireNonNull(rateLimitSettings);
        this.endpointGrouping = new EndpointGrouping(Objects.requireNonNull(rateLimitGroup).hashCode(), rateLimitSettings);
        this.rateLimitSettings = rateLimitSettings;
        this.service = service;
        this.taskType = taskType;
    }

    BaseRequestManager(ThreadPool threadPool, RateLimitGroupingModel rateLimitGroupingModel, String service, TaskType taskType) {
        this.threadPool = Objects.requireNonNull(threadPool);
        Objects.requireNonNull(rateLimitGroupingModel);

        this.inferenceEntityId = rateLimitGroupingModel.inferenceEntityId();
        this.endpointGrouping = new EndpointGrouping(
            rateLimitGroupingModel.rateLimitGroupingHash(),
            rateLimitGroupingModel.rateLimitSettings()
        );
        this.rateLimitSettings = rateLimitGroupingModel.rateLimitSettings();
        this.service = service;
        this.taskType = taskType;
    }

    protected void execute(Runnable runnable) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(runnable);
    }

    @Override
    public String inferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public Object rateLimitGrouping() {
        return endpointGrouping;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String service() {
        return this.service;
    }

    @Override
    public TaskType taskType() {
        return this.taskType;
    }

    private record EndpointGrouping(int group, RateLimitSettings settings) {}
}
