/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * @param elasticInferenceServiceUrl the upstream Elastic Inference Server's URL
 * @param revokeAuthorizationDelay Amount of time to wait before attempting to revoke authorization to certain model ids.
 *                                 null indicates that there should be no delay
 */
public record ElasticInferenceServiceComponents(@Nullable String elasticInferenceServiceUrl, @Nullable TimeValue revokeAuthorizationDelay) {
    private static final TimeValue DEFAULT_REVOKE_AUTHORIZATION_DELAY = TimeValue.timeValueMinutes(10);

    public static final ElasticInferenceServiceComponents EMPTY_INSTANCE = new ElasticInferenceServiceComponents(null, null);

    public static ElasticInferenceServiceComponents withNoRevokeDelay(String elasticInferenceServiceUrl) {
        return new ElasticInferenceServiceComponents(elasticInferenceServiceUrl, null);
    }

    public static ElasticInferenceServiceComponents withDefaultRevokeDelay(String elasticInferenceServiceUrl) {
        return new ElasticInferenceServiceComponents(elasticInferenceServiceUrl, DEFAULT_REVOKE_AUTHORIZATION_DELAY);
    }
}
