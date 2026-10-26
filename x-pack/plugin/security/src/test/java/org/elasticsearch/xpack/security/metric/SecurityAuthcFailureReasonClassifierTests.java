/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;

import static org.hamcrest.Matchers.equalTo;

public class SecurityAuthcFailureReasonClassifierTests extends ESTestCase {

    public void testFailedResultWithoutExceptionIsClientAuthenticationFailed() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromResult(AuthenticationResult.unsuccessful("nope", null)),
            equalTo(SecurityAuthcFailureReason.CLIENT_AUTHENTICATION_FAILED)
        );
    }

    public void testFailedResultWithFourxxExceptionIsClientAuthenticationFailed() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromResult(
                AuthenticationResult.unsuccessful("nope", new ElasticsearchSecurityException("x", RestStatus.UNAUTHORIZED))
            ),
            equalTo(SecurityAuthcFailureReason.CLIENT_AUTHENTICATION_FAILED)
        );
    }

    public void testFailedResultWithFivexxExceptionIsServerInternalError() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromResult(
                AuthenticationResult.unsuccessful("nope", new ElasticsearchSecurityException("x", RestStatus.INTERNAL_SERVER_ERROR))
            ),
            equalTo(SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR)
        );
    }

    public void testExceptionElasticsearchExceptionUsesStatus() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromException(new ElasticsearchSecurityException("bad", RestStatus.BAD_REQUEST)),
            equalTo(SecurityAuthcFailureReason.CLIENT_AUTHENTICATION_FAILED)
        );
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromException(new ElasticsearchSecurityException("oops", RestStatus.BAD_GATEWAY)),
            equalTo(SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR)
        );
    }

    public void testExceptionPlainElasticsearchExceptionDefaultsToServerInternalError() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromException(new ElasticsearchException("generic")),
            equalTo(SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR)
        );
    }

    public void testEsRejectedExecutionIsThreadPoolRejectedExecution() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromException(new EsRejectedExecutionException("busy", true)),
            equalTo(SecurityAuthcFailureReason.SERVER_THREAD_POOL_REJECTED_EXECUTION)
        );
    }

    public void testUnclassifiedThrowableIsServerInternalError() {
        assertThat(
            SecurityAuthcFailureReasonClassifier.DEFAULT.fromException(new IllegalStateException("bug")),
            equalTo(SecurityAuthcFailureReason.SERVER_INTERNAL_ERROR)
        );
    }
}
