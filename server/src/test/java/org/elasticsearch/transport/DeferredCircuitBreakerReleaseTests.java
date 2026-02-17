/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeferredCircuitBreakerReleaseTests extends ESTestCase {

    public void testFetchSearchResultImplementsDeferredRelease() {
        AtomicBoolean released = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setDeferredCircuitBreakerRelease(() -> released.set(true));

            Releasable release = result.takeCircuitBreakerRelease();
            assertThat(release, notNullValue());

            // not released yet
            assertThat(released.get(), is(false));

            release.close();
            assertThat(released.get(), is(true));

            // second take should return null
            assertThat(result.takeCircuitBreakerRelease(), nullValue());
        } finally {
            result.decRef();
        }
    }

    public void testQueryFetchSearchResultDelegatesToFetchResult() {
        AtomicBoolean released = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        QueryFetchSearchResult queryFetchResult = null;
        try {
            fetchResult.setDeferredCircuitBreakerRelease(() -> released.set(true));

            queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);

            // Take release through QueryFetchSearchResult
            Releasable release = queryFetchResult.takeCircuitBreakerRelease();
            assertThat(release, notNullValue());
            assertThat(released.get(), is(false));

            release.close();
            assertThat(released.get(), is(true));

            // fetch result's take should now return null too
            assertThat(fetchResult.takeCircuitBreakerRelease(), nullValue());
        } finally {
            if (queryFetchResult != null) {
                queryFetchResult.decRef();
            } else {
                fetchResult.decRef();
            }
        }
    }

    public void testScrollQueryFetchSearchResultDelegates() {
        AtomicBoolean released = new AtomicBoolean(false);

        FetchSearchResult fetchResult = new FetchSearchResult();
        ScrollQueryFetchSearchResult scrollResult = null;
        try {
            fetchResult.setDeferredCircuitBreakerRelease(() -> released.set(true));

            QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(new QuerySearchResult(), fetchResult);
            scrollResult = new ScrollQueryFetchSearchResult(queryFetchResult, null);

            Releasable release = scrollResult.takeCircuitBreakerRelease();
            assertThat(release, notNullValue());
            assertThat(released.get(), is(false));

            release.close();
            assertThat(released.get(), is(true));
        } finally {
            if (scrollResult != null) {
                scrollResult.decRef();
            } else {
                fetchResult.decRef();
            }
        }
    }

    public void testChannelActionListenerPassesReleasableToChannel() {
        AtomicBoolean released = new AtomicBoolean(false);
        AtomicReference<Releasable> capturedReleasable = new AtomicReference<>();
        AtomicBoolean responseSent = new AtomicBoolean(false);

        TransportChannel channel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "test";
            }

            @Override
            public void sendResponse(TransportResponse response) {
                responseSent.set(true);
            }

            @Override
            public void sendResponse(TransportResponse response, Releasable onSendComplete) {
                capturedReleasable.set(onSendComplete);
                responseSent.set(true);
            }

            @Override
            public void sendResponse(Exception exception) {
                // not used in this test
            }

            @Override
            public TransportVersion getVersion() {
                return TransportVersion.current();
            }
        };

        ChannelActionListener<FetchSearchResult> listener = new ChannelActionListener<>(channel);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setDeferredCircuitBreakerRelease(() -> released.set(true));

            listener.onResponse(result);
            // response should be sent
            assertThat(responseSent.get(), is(true));

            // releasable should have been captured, but not release yet
            assertThat(capturedReleasable.get(), notNullValue());
            assertThat(released.get(), is(false));

            // simulate network send completion, to then verify the releaseble is closed
            capturedReleasable.get().close();
            assertThat(released.get(), is(true));
        } finally {
            result.decRef();
        }
    }

    public void testChannelActionListenerWithNoReleasable() {
        AtomicBoolean responseSent = new AtomicBoolean(false);
        AtomicBoolean sendResponseWithReleasableCalled = new AtomicBoolean(false);

        TransportChannel channel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "test";
            }

            @Override
            public void sendResponse(TransportResponse response) {
                responseSent.set(true);
            }

            @Override
            public void sendResponse(TransportResponse response, Releasable onSendComplete) {
                sendResponseWithReleasableCalled.set(true);
            }

            @Override
            public void sendResponse(Exception exception) {
                // not used
            }
        };

        ChannelActionListener<FetchSearchResult> listener = new ChannelActionListener<>(channel);

        FetchSearchResult result = new FetchSearchResult();
        try {
            // no deferred release set, so takeCircuitBreakerRelease will return null
            listener.onResponse(result);

            // should use the simple sendResponse, not the one with releasable
            assertThat(responseSent.get(), is(true));
            assertThat(sendResponseWithReleasableCalled.get(), is(false));
        } finally {
            result.decRef();
        }
    }

    public void testTakeReleasableIsIdempotent() {
        AtomicBoolean released = new AtomicBoolean(false);

        FetchSearchResult result = new FetchSearchResult();
        try {
            result.setDeferredCircuitBreakerRelease(() -> released.set(true));

            Releasable release1 = result.takeCircuitBreakerRelease();
            assertThat(release1, notNullValue());

            // second take should return null
            Releasable release2 = result.takeCircuitBreakerRelease();
            assertThat(release2, nullValue());

            // release should work
            release1.close();
            assertThat(released.get(), is(true));
        } finally {
            result.decRef();
        }
    }
}
