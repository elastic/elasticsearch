/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class RetryHttpInitializerWrapper implements HttpRequestInitializer {
    private static final Logger logger = LogManager.getLogger(RetryHttpInitializerWrapper.class);

    // Intercepts the request for filling in the "Authorization"
    // header field, as well as recovering from certain unsuccessful
    // error codes wherein the Credential must refresh its token for a
    // retry.
    private final Credential wrappedCredential;

    // A sleeper; you can replace it with a mock in your test.
    private final Sleeper sleeper;

    private TimeValue maxWait;

    public RetryHttpInitializerWrapper(Credential wrappedCredential) {
        this(wrappedCredential, Sleeper.DEFAULT, TimeValue.timeValueMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS));
    }

    public RetryHttpInitializerWrapper(Credential wrappedCredential, TimeValue maxWait) {
        this(wrappedCredential, Sleeper.DEFAULT, maxWait);
    }

    // Use only for testing.
    RetryHttpInitializerWrapper(Credential wrappedCredential, Sleeper sleeper, TimeValue maxWait) {
        this.wrappedCredential = Objects.requireNonNull(wrappedCredential);
        this.sleeper = sleeper;
        this.maxWait = maxWait;
    }

    // Use only for testing
    static MockGoogleCredential.Builder newMockCredentialBuilder() {
        // TODO: figure out why GCE is so bad like this
        return Access.doPrivileged(MockGoogleCredential.Builder::new);
    }

    @Override
    public void initialize(HttpRequest httpRequest) {
        final HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(
            new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(((int) maxWait.getMillis())).build()
        ).setSleeper(sleeper);

        httpRequest.setInterceptor(wrappedCredential);
        httpRequest.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler() {
            int retry = 0;

            @Override
            public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry) throws IOException {
                if (wrappedCredential.handleResponse(request, response, supportsRetry)) {
                    // If credential decides it can handle it,
                    // the return code or message indicated
                    // something specific to authentication,
                    // and no backoff is desired.
                    return true;
                } else if (backoffHandler.handleResponse(request, response, supportsRetry)) {
                    // Otherwise, we defer to the judgement of
                    // our internal backoff handler.
                    logger.debug("Retrying [{}] times : [{}]", retry, request.getUrl());
                    return true;
                } else {
                    return false;
                }
            }
        });
        httpRequest.setIOExceptionHandler(
            new HttpBackOffIOExceptionHandler(new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(((int) maxWait.getMillis())).build())
                .setSleeper(sleeper)
        );
    }
}
