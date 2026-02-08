/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.settings.SecureReleasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.netty4.internal.HttpHeadersAuthenticatorUtils;
import org.elasticsearch.http.netty4.internal.HttpHeadersWithAuthenticationContext;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public final class HttpHeadersAuthenticatorUtilsTests extends ESTestCase {

    public void testRemoveHeaderPreservesValidationResult() {
        final ThreadContext.StoredContext dummyValidationContext = () -> {};
        final DefaultHttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        String header1 = "header1";
        String headerValue1 = "headerValue1";
        String header2 = "header2";
        String headerValue2 = "headerValue2";
        httpRequest.headers().add(header1, headerValue1);
        httpRequest.headers().add(header2, headerValue2);
        final DefaultHttpRequest validatableHttpRequest = (DefaultHttpRequest) HttpHeadersAuthenticatorUtils
            .wrapAsMessageWithAuthenticationContext(httpRequest);
        boolean validated = randomBoolean();
        if (validated) {
            ((HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers()).setAuthenticationContext(dummyValidationContext);
        }
        if (randomBoolean()) {
            validatableHttpRequest.headers().remove("header1");
            assertThat(validatableHttpRequest.headers().contains("header1"), is(false));
            assertThat(validatableHttpRequest.headers().contains("header2"), is(true));
        } else {
            validatableHttpRequest.headers().remove("header2");
            assertThat(validatableHttpRequest.headers().contains("header1"), is(true));
            assertThat(validatableHttpRequest.headers().contains("header2"), is(false));
        }
        if (validated) {
            assertThat(
                ((HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers()).authenticationContextSetOnce.get(),
                is(dummyValidationContext)
            );
        } else {
            assertThat(
                ((HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers()).authenticationContextSetOnce.get(),
                nullValue()
            );
        }
    }

    public void testCopyHeaderPreservesValidationResult() {
        final ThreadContext.StoredContext dummyValidationContext = () -> {};
        final DefaultHttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        String header = "header";
        String headerValue = "headerValue";
        httpRequest.headers().add(header, headerValue);
        final DefaultHttpRequest validatableHttpRequest = (DefaultHttpRequest) HttpHeadersAuthenticatorUtils
            .wrapAsMessageWithAuthenticationContext(httpRequest);
        boolean validated = randomBoolean();
        if (validated) {
            ((HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers()).setAuthenticationContext(dummyValidationContext);
        }
        HttpHeaders httpHeadersCopy = ((HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers()).copy();
        if (validated) {
            assertThat(
                ((HttpHeadersWithAuthenticationContext) httpHeadersCopy).authenticationContextSetOnce.get(),
                is(dummyValidationContext)
            );
        } else {
            assertThat(((HttpHeadersWithAuthenticationContext) httpHeadersCopy).authenticationContextSetOnce.get(), nullValue());
        }
    }

    public void testReleaseSecureReleasablesClosesAllReleasables() {
        final DefaultHttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        final DefaultHttpRequest validatableHttpRequest = (DefaultHttpRequest) HttpHeadersAuthenticatorUtils
            .wrapAsMessageWithAuthenticationContext(httpRequest);
        final HttpHeadersWithAuthenticationContext headers = (HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers();

        final AtomicBoolean releasableClosed = new AtomicBoolean(false);
        final SecureReleasable releasable = () -> releasableClosed.set(true);
        headers.setSecureReleasables(List.of(releasable));

        final Netty4HttpRequest netty4Request = new Netty4HttpRequest(0, validatableHttpRequest, HttpBody.empty());

        assertFalse(releasableClosed.get());

        HttpHeadersAuthenticatorUtils.releaseSecureReleasables(netty4Request);

        assertTrue(releasableClosed.get());
    }

    public void testReleaseSecureReleasablesHandlesExceptions() {
        final DefaultHttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        final DefaultHttpRequest validatableHttpRequest = (DefaultHttpRequest) HttpHeadersAuthenticatorUtils
            .wrapAsMessageWithAuthenticationContext(httpRequest);
        final HttpHeadersWithAuthenticationContext headers = (HttpHeadersWithAuthenticationContext) validatableHttpRequest.headers();

        // create releasables where one throws an exception
        final AtomicBoolean releasable1Closed = new AtomicBoolean(false);
        final AtomicBoolean releasable2Closed = new AtomicBoolean(false);
        final SecureReleasable throwingReleasable = () -> {
            releasable1Closed.set(true);
            throw new RuntimeException("test exception");
        };
        final SecureReleasable normalReleasable = () -> releasable2Closed.set(true);
        headers.setSecureReleasables(List.of(throwingReleasable, normalReleasable));
        final Netty4HttpRequest netty4Request = new Netty4HttpRequest(0, validatableHttpRequest, HttpBody.empty());

        // call releaseSecureReleasables - should not throw, and should close all releasables
        HttpHeadersAuthenticatorUtils.releaseSecureReleasables(netty4Request);
        assertTrue(releasable1Closed.get());
        assertTrue(releasable2Closed.get());
    }

}
