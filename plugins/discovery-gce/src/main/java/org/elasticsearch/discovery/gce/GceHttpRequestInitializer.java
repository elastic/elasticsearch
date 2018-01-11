/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;

public class GceHttpRequestInitializer implements HttpRequestInitializer {

    // Intercepts the request for filling in the "Authorization"
    // header field, as well as recovering from certain unsuccessful
    // error codes wherein the Credential must refresh its token for a
    // retry.
    private final Credential credential;

    // A sleeper; you can replace it with a mock in your test.
    private final Sleeper sleeper;

    private final TimeValue connectTimeout;
    private final TimeValue readTimeout;
    private final TimeValue maxElapsedTime;
    private final boolean retry;

    public GceHttpRequestInitializer(final Credential credential,
                                     final TimeValue connectTimeout,
                                     final TimeValue readTimeout,
                                     final TimeValue maxElapsedTime,
                                     final boolean retry) {
        this(credential, connectTimeout, readTimeout, maxElapsedTime, retry, null);
    }

    // pkg private for tests
    GceHttpRequestInitializer(final Credential credential,
                              final TimeValue connectTimeout,
                              final TimeValue readTimeout,
                              final TimeValue maxElapsedTime,
                              final boolean retry,
                              final Sleeper sleeper) {
        this.credential = Objects.requireNonNull(credential, "credential must be not null");
        this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout must be not null");
        this.readTimeout = Objects.requireNonNull(readTimeout, "readTimeout must be not null");
        this.maxElapsedTime = Objects.requireNonNull(maxElapsedTime, "maxElapsedTime must be not null");
        this.retry = retry;
        this.sleeper = sleeper;
    }

    @Override
    public void initialize(HttpRequest httpRequest) {
        if (retry) {
            final HttpBackOffIOExceptionHandler exceptionHandler = new HttpBackOffIOExceptionHandler(exponentialBackOff(maxElapsedTime));
            if (sleeper != null) {
                exceptionHandler.setSleeper(sleeper);
            }
            httpRequest.setIOExceptionHandler(exceptionHandler);

            final ExponentialBackOff backOff = exponentialBackOff(maxElapsedTime);
            final HttpBackOffUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(backOff);
            if (sleeper != null) {
                backoffHandler.setSleeper(sleeper);
            }

            httpRequest.setUnsuccessfulResponseHandler((request, response, retries) ->
                credential.handleResponse(request, response, retries) || backoffHandler.handleResponse(request, response, retries));
        } else {
            httpRequest.setUnsuccessfulResponseHandler(credential);
        }

        final long noTimeout = TimeValue.MINUS_ONE.getMillis();

        final long connectTimeoutMillis = connectTimeout.getMillis();
        if (connectTimeoutMillis > noTimeout) {
            httpRequest.setConnectTimeout((int) connectTimeoutMillis);
        }

        final long readTimeoutMillis = readTimeout.getMillis();
        if (readTimeoutMillis > noTimeout) {
            httpRequest.setReadTimeout((int) readTimeoutMillis);
        }

        httpRequest.setInterceptor(credential);
    }

    private ExponentialBackOff exponentialBackOff(final TimeValue maxElapsedTime) {
        final ExponentialBackOff.Builder backOff = new ExponentialBackOff.Builder();
        if (maxElapsedTime != null && maxElapsedTime.getMillis() > 0) {
            backOff.setMaxElapsedTimeMillis((int) maxElapsedTime.getMillis());
        }
        return backOff.build();
    }
}

