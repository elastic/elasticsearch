/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeHttpBodyStream;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base test class for OTLP REST actions. Tests the common request/response handling
 * defined in {@link AbstractOTLPRestAction}, parameterized by signal-specific behavior
 * (action construction and response parsing).
 */
public abstract class AbstractOTLPRestActionTests extends ESTestCase {

    protected ThreadPool threadPool;
    protected NoOpNodeClient client;
    private IndexingPressure indexingPressure;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexingPressure = new IndexingPressure(Settings.EMPTY);
        threadPool = createThreadPool();
        client = new NoOpNodeClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    /** Construct the signal-specific REST action under test. */
    protected abstract AbstractOTLPRestAction createAction(IndexingPressure pressure, long maxRequestSizeBytes);

    /** The expected transport action type that the REST action dispatches to. */
    protected abstract ActionType<OTLPActionResponse> actionType();

    /** Create a successful {@link OTLPActionResponse} for the signal. */
    protected abstract OTLPActionResponse createSuccessResponse();

    /** Verify that the response bytes represent the expected empty/default response for the signal. */
    protected abstract void assertEmptyResponse(byte[] responseBytes) throws InvalidProtocolBufferException;

    /** The REST route path, e.g. {@code "/_otlp/v1/metrics"}. */
    protected abstract String routePath();

    // --- shared tests ---

    @SuppressWarnings("unchecked")
    public void testSuccessfulRequest() {
        var expectedResponse = createSuccessResponse();
        client = new NoOpNodeClient(threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request req,
                ActionListener<Response> listener
            ) {
                assertThat(actionType, equalTo(actionType()));
                listener.onResponse((Response) expectedResponse);
            }
        };
        try (var response = execute(1024, 64)) {
            assertThat(response.status(), equalTo(RestStatus.OK));
            assertThat(response.contentType(), equalTo(AbstractOTLPRestAction.CONTENT_TYPE_PROTOBUF));
            assertThat(response.content(), equalTo(expectedResponse.getResponse()));
        }
    }

    public void testEmptyBodyReturnsSuccess() throws Exception {
        try (var response = execute(1024, 0)) {
            assertThat(response.status(), equalTo(RestStatus.OK));
            assertThat(response.contentType(), equalTo(AbstractOTLPRestAction.CONTENT_TYPE_PROTOBUF));
            assertEmptyResponse(response.content().array());
        }
    }

    @SuppressWarnings("unchecked")
    public void testTransportActionFailure() {
        client = new NoOpNodeClient(threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request req,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new ElasticsearchStatusException("ingest failed", RestStatus.INTERNAL_SERVER_ERROR));
            }
        };
        try (var response = execute(1024, 64)) {
            assertThat(response.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    public void testOversizedBodyReturns413() {
        try (var response = execute(100, 101)) {
            assertThat(response.status(), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
            assertThat(response.contentType(), equalTo(AbstractOTLPRestAction.CONTENT_TYPE_PROTOBUF));
            assertThat(response.content().length(), equalTo(0));
        }
    }

    public void testIndexingPressureRejectionReturns429() {
        long limitBytes = 1024;
        var tightPressure = new IndexingPressure(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), ByteSizeValue.ofBytes(limitBytes)).build()
        );
        try (var response = execute(tightPressure, limitBytes + 1, 64)) {
            assertThat(response.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(response.contentType(), equalTo(AbstractOTLPRestAction.CONTENT_TYPE_PROTOBUF));
            assertThat(response.content().length(), equalTo(0));
        }
    }

    // --- shared test infrastructure ---

    protected RestResponse execute(long maxSize, int bodySize) {
        return execute(indexingPressure, maxSize, bodySize);
    }

    protected RestResponse execute(IndexingPressure pressure, long maxSize, int bodySize) {
        var stream = new FakeHttpBodyStream();
        var action = createAction(pressure, maxSize);
        var httpRequest = new FakeRestRequest.FakeHttpRequest(
            RestRequest.Method.POST,
            routePath(),
            Map.of("Content-Type", List.of("application/x-protobuf")),
            stream
        );
        var request = RestRequest.request(parserConfig(), httpRequest, new FakeRestRequest.FakeHttpChannel(null));
        var channel = new FakeRestChannel(request, true, 1);
        try {
            var consumer = (BaseRestHandler.RequestBodyChunkConsumer) action.prepareRequest(request, client);
            stream.setHandler(new HttpBody.ChunkHandler() {
                @Override
                public void onNext(ReleasableBytesReference chunk, boolean last) {
                    consumer.handleChunk(channel, chunk, last);
                }

                @Override
                public void close() {
                    consumer.streamClose();
                }
            });
            try (consumer) {
                consumer.accept(channel);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        if (channel.capturedResponse() == null) {
            stream.sendNext(randomReleasableBytesReference(bodySize), true);
        }
        RestResponse response = channel.capturedResponse();
        assertNotNull(response);
        return response;
    }
}
