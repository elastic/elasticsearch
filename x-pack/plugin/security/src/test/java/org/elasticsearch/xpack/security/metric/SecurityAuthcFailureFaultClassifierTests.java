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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;

public class SecurityAuthcFailureFaultClassifierTests extends ESTestCase {

    public void testFailedResultWithoutExceptionIsClient() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromFailedResult(AuthenticationResult.unsuccessful("nope", null)),
            equalTo(SecurityAuthcFailureFault.CLIENT)
        );
    }

    public void testFailedResultWithFourxxExceptionIsClient() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromFailedResult(
                AuthenticationResult.unsuccessful("nope", new ElasticsearchSecurityException("x", RestStatus.UNAUTHORIZED))
            ),
            equalTo(SecurityAuthcFailureFault.CLIENT)
        );
    }

    public void testFailedResultWithFivexxExceptionIsServer() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromFailedResult(
                AuthenticationResult.unsuccessful("nope", new ElasticsearchSecurityException("x", RestStatus.INTERNAL_SERVER_ERROR))
            ),
            equalTo(SecurityAuthcFailureFault.SERVER)
        );
    }

    public void testThrowableElasticsearchExceptionUsesStatus() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new ElasticsearchSecurityException("bad", RestStatus.BAD_REQUEST)),
            equalTo(SecurityAuthcFailureFault.CLIENT)
        );
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new ElasticsearchSecurityException("oops", RestStatus.BAD_GATEWAY)),
            equalTo(SecurityAuthcFailureFault.SERVER)
        );
    }

    public void testThrowablePlainElasticsearchExceptionDefaultsToServer() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new ElasticsearchException("generic")),
            equalTo(SecurityAuthcFailureFault.SERVER)
        );
    }

    public void testIoAndTimeoutsAreServer() {
        assertThat(SecurityAuthcFailureFaultClassifier.fromThrowable(new IOException("io")), equalTo(SecurityAuthcFailureFault.SERVER));
        assertThat(SecurityAuthcFailureFaultClassifier.fromThrowable(new TimeoutException("t")), equalTo(SecurityAuthcFailureFault.SERVER));
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new InterruptedException("i")),
            equalTo(SecurityAuthcFailureFault.SERVER)
        );
    }

    public void testEsRejectedExecutionIsServer() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new EsRejectedExecutionException("busy", true)),
            equalTo(SecurityAuthcFailureFault.SERVER)
        );
    }

    public void testNullThrowableIsUnknown() {
        assertThat(SecurityAuthcFailureFaultClassifier.fromThrowable(null), equalTo(SecurityAuthcFailureFault.UNKNOWN));
    }

    public void testUnclassifiedThrowableIsUnknown() {
        assertThat(
            SecurityAuthcFailureFaultClassifier.fromThrowable(new IllegalStateException("bug")),
            equalTo(SecurityAuthcFailureFault.UNKNOWN)
        );
    }
}
