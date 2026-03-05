/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.TestDirectResponseChannel;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TestTransportChannels;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNodeUtils.builder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link SearchTransportService#asBytesResponse}, covering both the network-channel path
 * (pre-serializes into {@link BytesTransportResponse}) and the direct-channel path (forwards the
 * original response as-is for local/same-node requests). Tests verify {@link BytesTransportResponse}
 * creation, round-trip deserialization, {@code afterSerialize} callback semantics, and cleanup
 * on serialization failure.
 */
public class AsBytesResponseTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        var mockTransport = new MockTransport();
        transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> builder("test-node").build(),
            null,
            Collections.emptySet()
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testNetworkPathCallsAfterSerializeOnSuccess() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> afterSerializeCalled.set(true)
        );

        listener.onResponse(new SimpleTestResponse("hello"));

        assertTrue("afterSerialize must be called after successful serialization", afterSerializeCalled.get());
        assertThat(sentResponse.get(), notNullValue());
        assertThat(sentResponse.get(), instanceOf(BytesTransportResponse.class));
        sentResponse.get().decRef();
    }

    public void testNetworkPathCallsAfterSerializeOnSerializationFailure() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentException = new AtomicReference<Exception>();

        var channel = new TestTransportChannel(
            ActionListener.wrap(resp -> fail("should not succeed when serialization fails"), sentException::set)
        );

        ActionListener<FailingTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> afterSerializeCalled.set(true)
        );

        listener.onResponse(new FailingTestResponse());

        assertTrue("afterSerialize must be called even on serialization failure", afterSerializeCalled.get());
        assertThat(sentException.get(), notNullValue());
    }

    public void testNetworkPathBytesResponseRoundTrip() throws Exception {
        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));

        var original = new SimpleTestResponse("round-trip-test");
        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(transportService, channel, response -> {});

        listener.onResponse(original);

        assertThat(sentResponse.get(), instanceOf(BytesTransportResponse.class));
        var bytesResp = (BytesTransportResponse) sentResponse.get();
        try (StreamInput in = bytesResp.streamInput()) {
            var deserialized = new SimpleTestResponse(in);
            assertThat(deserialized.value, equalTo("round-trip-test"));
        } finally {
            bytesResp.decRef();
        }
    }

    public void testNetworkPathReleasesCircuitBreakerBytesAfterSerialization() {
        var breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));

        var contextId = new ShardSearchContextId("test-session", 1);
        var fetchResult = new FetchSearchResult(contextId, null);
        try {
            var hits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            fetchResult.shardResult(hits, null);
            fetchResult.setSearchHitsSizeBytes(5000L);

            ActionListener<FetchSearchResult> listener = SearchTransportService.asBytesResponse(
                transportService,
                channel,
                response -> response.releaseCircuitBreakerBytes(breaker)
            );

            listener.onResponse(fetchResult);

            assertThat("breaker bytes should be released after serialization", breakerUsed.get(), equalTo(0L));
            assertThat(fetchResult.getSearchHitsSizeBytes(), equalTo(0L));
            assertThat(sentResponse.get(), instanceOf(BytesTransportResponse.class));
        } finally {
            fetchResult.decRef();
            if (sentResponse.get() != null) {
                sentResponse.get().decRef();
            }
        }
    }

    public void testNetworkPathReleasesCircuitBreakerBytesOnSerializationFailure() {
        var breakerUsed = new AtomicLong(3000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        var sentException = new AtomicReference<Exception>();

        var channel = new TestTransportChannel(
            ActionListener.wrap(resp -> fail("should not succeed when serialization fails"), sentException::set)
        );

        ActionListener<FailingTestResponse> listener = SearchTransportService.asBytesResponse(transportService, channel, response -> {
            long bytes = breakerUsed.get();
            if (bytes > 0L) {
                breakerUsed.addAndGet(-bytes);
            }
        });

        listener.onResponse(new FailingTestResponse());

        assertThat("breaker bytes should be released even on serialization failure", breakerUsed.get(), equalTo(0L));
        assertThat(sentException.get(), notNullValue());
    }

    public void testNetworkPathOnFailureDoesNotCallAfterSerialize() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentException = new AtomicReference<Exception>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> fail("should not succeed"), sentException::set));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> afterSerializeCalled.set(true)
        );

        listener.onFailure(new RuntimeException("upstream failure"));

        assertFalse("afterSerialize must not be called on upstream failure", afterSerializeCalled.get());
        assertThat(sentException.get(), notNullValue());
    }

    public void testDirectPathForwardsOriginalResponse() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestDirectResponseChannel(ActionListener.wrap(sentResponse::set, e -> fail("unexpected failure: " + e)));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> afterSerializeCalled.set(true)
        );

        var original = new SimpleTestResponse("direct-test");
        listener.onResponse(original);

        assertTrue("afterSerialize must be called on direct path", afterSerializeCalled.get());
        assertSame("direct path must forward the original response, not BytesTransportResponse", original, sentResponse.get());
    }

    public void testDirectPathCallsAfterSerializeAfterOnResponse() {
        var callOrder = new java.util.ArrayList<String>();

        var channel = new TestDirectResponseChannel(
            ActionListener.wrap(resp -> callOrder.add("onResponse"), e -> fail("unexpected failure: " + e))
        );

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> callOrder.add("afterSerialize")
        );

        listener.onResponse(new SimpleTestResponse("order-test"));

        assertThat(callOrder, equalTo(java.util.List.of("onResponse", "afterSerialize")));
    }

    public void testDirectPathOnFailureDoesNotCallAfterSerialize() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentException = new AtomicReference<Exception>();

        var channel = new TestDirectResponseChannel(ActionListener.wrap(resp -> fail("should not succeed"), sentException::set));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            channel,
            response -> afterSerializeCalled.set(true)
        );

        listener.onFailure(new RuntimeException("upstream failure"));

        assertFalse("afterSerialize must not be called on upstream failure", afterSerializeCalled.get());
        assertThat(sentException.get(), notNullValue());
    }

    public void testDirectPathReleasesCircuitBreakerBytesAfterResponse() {
        var breakerUsed = new AtomicLong(5000);
        CircuitBreaker breaker = new TestCircuitBreaker(breakerUsed);

        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestDirectResponseChannel(ActionListener.wrap(sentResponse::set, e -> fail("unexpected failure: " + e)));

        var contextId = new ShardSearchContextId("test-session", 1);
        var fetchResult = new FetchSearchResult(contextId, null);
        try {
            var hits = SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
            fetchResult.shardResult(hits, null);
            fetchResult.setSearchHitsSizeBytes(5000L);

            ActionListener<FetchSearchResult> listener = SearchTransportService.asBytesResponse(
                transportService,
                channel,
                response -> response.releaseCircuitBreakerBytes(breaker)
            );

            listener.onResponse(fetchResult);

            assertThat("breaker bytes should be released after response on direct path", breakerUsed.get(), equalTo(0L));
            assertThat(fetchResult.getSearchHitsSizeBytes(), equalTo(0L));
            assertSame("direct path must forward original response", fetchResult, sentResponse.get());
        } finally {
            fetchResult.decRef();
        }
    }

    public void testTaskTransportChannelUnwrapsToDirectPath() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentResponse = new AtomicReference<TransportResponse>();

        var directChannel = new TestDirectResponseChannel(ActionListener.wrap(sentResponse::set, e -> fail("unexpected failure: " + e)));
        var taskChannel = TestTransportChannels.newTaskTransportChannel(directChannel, () -> {});

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            taskChannel,
            response -> afterSerializeCalled.set(true)
        );

        var original = new SimpleTestResponse("task-wrapped-test");
        listener.onResponse(original);

        assertTrue("afterSerialize must be called on task-wrapped direct path", afterSerializeCalled.get());
        assertSame("task-wrapped direct channel must forward original response, not BytesTransportResponse", original, sentResponse.get());
    }

    public void testTaskTransportChannelUnwrapsToNetworkPath() {
        var afterSerializeCalled = new AtomicBoolean(false);
        var sentResponse = new AtomicReference<TransportResponse>();

        var networkChannel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));
        var taskChannel = TestTransportChannels.newTaskTransportChannel(networkChannel, () -> {});

        ActionListener<SimpleTestResponse> listener = SearchTransportService.asBytesResponse(
            transportService,
            taskChannel,
            response -> afterSerializeCalled.set(true)
        );

        listener.onResponse(new SimpleTestResponse("task-network-test"));

        assertTrue("afterSerialize must be called on task-wrapped network path", afterSerializeCalled.get());
        assertThat(sentResponse.get(), instanceOf(BytesTransportResponse.class));
        sentResponse.get().decRef();
    }

    static class SimpleTestResponse extends TransportResponse {
        final String value;

        SimpleTestResponse(String value) {
            this.value = value;
        }

        SimpleTestResponse(StreamInput in) throws IOException {
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

    static class FailingTestResponse extends TransportResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IOException("simulated serialization failure");
        }
    }

    private static class TestCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used;

        TestCircuitBreaker(AtomicLong used) {
            super("test");
            this.used = used;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            used.addAndGet(bytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }
}
