/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.security.metric.SecurityAuthcFailureReason;
import org.elasticsearch.xpack.security.metric.SecurityAuthcFailureReasonClassifier;

/**
 * Failure reason classifier for API key authentication metrics.
 */
public final class ApiKeyAuthcFailureReasonClassifier implements SecurityAuthcFailureReasonClassifier {

    public static final SecurityAuthcFailureReasonClassifier INSTANCE = new ApiKeyAuthcFailureReasonClassifier();

    private ApiKeyAuthcFailureReasonClassifier() {}

    private enum ApiKeyAuthcFailureReason implements SecurityAuthcFailureReason {
        INVALID_CREDENTIALS("client.invalid_credentials"),
        INVALIDATED("client.api_key_invalidated"),
        EXPIRED("client.api_key_expired");

        private final String value;

        ApiKeyAuthcFailureReason(String value) {
            this.value = value;
        }

        @Override
        public String value() {
            return value;
        }
    }

    @Override
    public SecurityAuthcFailureReason fromResult(final AuthenticationResult<?> result) {
        final Exception ex = result.getException();
        if (ex != null) {
            if (ex instanceof EsRejectedExecutionException) {
                return SecurityAuthcFailureReason.SERVER_THREAD_POOL_REJECTED_EXECUTION;
            }
            if (ex instanceof ElasticsearchException ese && ese.status().getStatus() >= 500) {
                return SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR;
            }
        }
        final String message = result.getMessage();
        if (message != null) {
            if (message.contains("has been invalidated")) {
                return ApiKeyAuthcFailureReason.INVALIDATED;
            }
            if (message.contains("api key is expired")) {
                return ApiKeyAuthcFailureReason.EXPIRED;
            }
        }
        if (ex != null) {
            return fromException(ex);
        }
        return ApiKeyAuthcFailureReason.INVALID_CREDENTIALS;
    }
}
