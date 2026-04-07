/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

/** Picks client / server / unknown for auth failure metrics without putting exception messages on labels. */
public final class SecurityAuthcFailureFaultClassifier {

    private SecurityAuthcFailureFaultClassifier() {}

    /** When the result is not authenticated. */
    public static SecurityAuthcFailureFault fromFailedResult(final AuthenticationResult<?> result) {
        if (result.getException() != null) {
            return fromThrowable(result.getException());
        }
        return SecurityAuthcFailureFault.CLIENT;
    }

    /** When the listener gets an exception instead of a normal result. */
    public static SecurityAuthcFailureFault fromThrowable(@Nullable final Throwable t) {
        if (t == null) {
            return SecurityAuthcFailureFault.UNKNOWN;
        }
        if (t instanceof ElasticsearchException elasticsearchException) {
            return fromElasticsearchException(elasticsearchException);
        }
        if (t instanceof TimeoutException || t instanceof SocketTimeoutException || t instanceof InterruptedException) {
            return SecurityAuthcFailureFault.SERVER;
        }
        if (t instanceof IOException) {
            return SecurityAuthcFailureFault.SERVER;
        }
        if (t instanceof EsRejectedExecutionException) {
            return SecurityAuthcFailureFault.SERVER;
        }
        return SecurityAuthcFailureFault.UNKNOWN;
    }

    private static SecurityAuthcFailureFault fromElasticsearchException(final ElasticsearchException ex) {
        final int status = ex.status().getStatus();
        if (status >= 500) {
            return SecurityAuthcFailureFault.SERVER;
        }
        if (status >= 400 && status < 500) {
            return SecurityAuthcFailureFault.CLIENT;
        }
        return SecurityAuthcFailureFault.UNKNOWN;
    }
}
