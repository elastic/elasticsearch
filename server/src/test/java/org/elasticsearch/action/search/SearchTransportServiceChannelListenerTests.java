/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.node.DiscoveryNodeUtils.builder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SearchTransportServiceChannelListenerTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;

    @Before
    public void setUpResources() throws Exception {
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

    @After
    public void tearDownResources() throws Exception {
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testNetworkPathBytesResponseRoundTrip() throws Exception {
        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));

        var original = new SimpleTestResponse("test");
        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            channel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        listener.onResponse(original);

        assertThat(sentResponse.get(), instanceOf(BytesTransportResponse.class));
        var bytesResp = (BytesTransportResponse) sentResponse.get();
        try (StreamInput in = bytesResp.streamInput()) {
            var deserialized = new SimpleTestResponse(in);
            assertThat(deserialized.value, equalTo("test"));
        } finally {
            bytesResp.decRef();
        }
    }

    public void testNetworkPathSerializationFailureSendsFailure() {
        var sentException = new AtomicReference<Exception>();

        var channel = new TestTransportChannel(
            ActionListener.wrap(resp -> fail("should not succeed when serialization fails"), sentException::set)
        );

        ActionListener<FailingTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            channel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        listener.onResponse(new FailingTestResponse());

        assertThat(sentException.get(), notNullValue());
        assertThat(sentException.get(), instanceOf(IOException.class));
        assertThat(sentException.get().getMessage(), equalTo("simulated serialization failure"));
    }

    public void testNetworkPathOnFailureForwardsFailure() {
        var sentException = new AtomicReference<Exception>();

        var channel = new TestTransportChannel(ActionListener.wrap(resp -> fail("should not succeed"), sentException::set));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            channel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        RuntimeException e = new RuntimeException("upstream failure");
        listener.onFailure(e);

        assertThat(sentException.get(), notNullValue());
        assertThat(sentException.get(), sameInstance(e));
    }

    public void testDirectPathForwardsOriginalResponse() {
        var sentResponse = new AtomicReference<TransportResponse>();

        var channel = new TestDirectResponseChannel(ActionListener.wrap(sentResponse::set, e -> fail("unexpected failure: " + e)));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            channel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        var original = new SimpleTestResponse("direct-test");
        listener.onResponse(original);

        assertSame(original, sentResponse.get());
        assertThat(sentResponse.get(), not(instanceOf(BytesTransportResponse.class)));
    }

    public void testDirectPathOnFailureForwardsFailure() {
        var sentException = new AtomicReference<Exception>();

        var channel = new TestDirectResponseChannel(ActionListener.wrap(resp -> fail("should not succeed"), sentException::set));

        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            channel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        RuntimeException e = new RuntimeException("upstream failure");
        listener.onFailure(e);

        assertThat(sentException.get(), notNullValue());
        assertThat(sentException.get(), sameInstance(e));
    }

    public void testTaskTransportChannelUnwrapsToDirectPath() {
        var sentResponse = new AtomicReference<TransportResponse>();

        var directChannel = new TestDirectResponseChannel(ActionListener.wrap(sentResponse::set, e -> fail("unexpected failure: " + e)));
        var taskChannel = TestTransportChannels.newTaskTransportChannel(directChannel, () -> {});

        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            taskChannel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        var original = new SimpleTestResponse("task-wrapped-test");
        listener.onResponse(original);

        assertSame(original, sentResponse.get());
        assertThat(sentResponse.get(), not(instanceOf(BytesTransportResponse.class)));
        sentResponse.get().decRef();
    }

    public void testTaskTransportChannelUnwrapsToNetworkPath() {
        var sentResponse = new AtomicReference<TransportResponse>();

        var networkChannel = new TestTransportChannel(ActionListener.wrap(resp -> {
            resp.mustIncRef();
            sentResponse.set(resp);
        }, e -> fail("unexpected failure: " + e)));
        var taskChannel = TestTransportChannels.newTaskTransportChannel(networkChannel, () -> {});

        ActionListener<SimpleTestResponse> listener = SearchTransportService.channelListener(
            transportService,
            taskChannel,
            newLimitedBreaker(ByteSizeValue.ofMb(100))
        );

        listener.onResponse(new SimpleTestResponse("task-network-test"));

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

}
