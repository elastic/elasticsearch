/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * @param elasticInferenceServiceUrl the upstream Elastic Inference Server's URL
 * @param authRequestInterval Amount of time to wait before making another authorization request
 * @param maxAuthRequestJitter The maximum amount of jitter to add
 */
public record ElasticInferenceServiceComponents(
    @Nullable String elasticInferenceServiceUrl,
    TimeValue authRequestInterval,
    TimeValue maxAuthRequestJitter
) {

    private static final TimeValue DEFAULT_AUTH_REQUEST_INTERVAL = TimeValue.timeValueMinutes(10);
    private static final TimeValue DEFAULT_AUTH_REQUEST_JITTER = TimeValue.timeValueMinutes(5);

    public static final ElasticInferenceServiceComponents EMPTY_INSTANCE = new ElasticInferenceServiceComponents(
        null,
        DEFAULT_AUTH_REQUEST_INTERVAL,
        DEFAULT_AUTH_REQUEST_JITTER
    );

    public static ElasticInferenceServiceComponents withDefaults(String elasticInferenceServiceUrl) {
        return new ElasticInferenceServiceComponents(
            elasticInferenceServiceUrl,
            DEFAULT_AUTH_REQUEST_INTERVAL,
            DEFAULT_AUTH_REQUEST_JITTER
        );
    }

    public ElasticInferenceServiceComponents {
        Objects.requireNonNull(authRequestInterval);
        Objects.requireNonNull(maxAuthRequestJitter);
    }
}
