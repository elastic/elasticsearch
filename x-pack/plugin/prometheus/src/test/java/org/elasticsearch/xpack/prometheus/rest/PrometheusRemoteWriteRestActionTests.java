/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PrometheusRemoteWriteRestActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private NoOpNodeClient client;
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

    @SuppressWarnings("unchecked")
    public void testSuccessfulWrite() {
        client = new NoOpNodeClient(threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request req,
                ActionListener<Response> listener
            ) {
                assertThat(actionType, equalTo(PrometheusRemoteWriteTransportAction.TYPE));
                var remoteWriteRequest = (PrometheusRemoteWriteTransportAction.RemoteWriteRequest) req;
                remoteWriteRequest.close();
                listener.onResponse((Response) new PrometheusRemoteWriteTransportAction.RemoteWriteResponse());
            }
        };
        try (var response = executeRemoteWrite(1024, 64)) {
            assertThat(response.status(), equalTo(RestStatus.NO_CONTENT));
        }
    }

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
        try (var response = executeRemoteWrite(1024, 64)) {
            assertThat(response.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    public void testOversizedBodyReturnsPlainText413() {
        try (var response = executeRemoteWrite(100, 101)) {
            assertThat(response.status(), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
            assertThat(response.contentType(), equalTo(RestResponse.TEXT_CONTENT_TYPE));
            assertThat(response.content().utf8ToString(), containsString("request body too large"));
        }
    }

    private RestResponse executeRemoteWrite(int maxSize, int bodySize) {
        var stream = new FakeHttpBodyStream();
        var action = new PrometheusRemoteWriteRestAction(indexingPressure, maxSize);
        var httpRequest = new FakeRestRequest.FakeHttpRequest(
            RestRequest.Method.POST,
            "/_prometheus/api/v1/write",
            Map.of("Content-Type", List.of("application/x-protobuf")),
            stream
        );
        var request = RestRequest.request(parserConfig(), httpRequest, new FakeRestRequest.FakeHttpChannel(null));
        var channel = new FakeRestChannel(request, true, 1);
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
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        stream.sendNext(randomReleasableBytesReference(bodySize), true);
        RestResponse response = channel.capturedResponse();
        assertNotNull(response);
        return response;
    }
}
