/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;

/**
 * Maps failed authentication to a bounded failure reason for metrics.
 */
public interface SecurityAuthcFailureReasonClassifier {

    SecurityAuthcFailureReasonClassifier DEFAULT = new SecurityAuthcFailureReasonClassifier() {};

    default SecurityAuthcFailureReason fromResult(final AuthenticationResult<?> result) {
        assert result.isAuthenticated() == false : "should only be called for failed authentication results";
        final Exception ex = result.getException();
        if (ex != null) {
            return fromException(ex);
        }
        return SecurityAuthcFailureReason.CLIENT_AUTHENTICATION_FAILED;
    }

    default SecurityAuthcFailureReason fromException(final Throwable t) {
        if (t instanceof EsRejectedExecutionException) {
            return SecurityAuthcFailureReason.SERVER_THREAD_POOL_REJECTED_EXECUTION;
        }
        if (t instanceof ElasticsearchException ese) {
            final int status = ese.status().getStatus();
            if (status >= 400 && status < 500) {
                return SecurityAuthcFailureReason.CLIENT_AUTHENTICATION_FAILED;
            }
        }
        return SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR;
    }
}
