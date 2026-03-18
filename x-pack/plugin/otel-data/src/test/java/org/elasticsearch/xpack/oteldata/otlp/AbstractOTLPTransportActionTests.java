/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Base test class for OTLP transport actions. Tests the common request/response handling
 * defined in {@link AbstractOTLPTransportAction}, parameterized by signal-specific behavior
 * (request construction and response parsing).
 */
public abstract class AbstractOTLPTransportActionTests extends ESTestCase {

    protected Client client;
    private AbstractOTLPTransportAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        when(client.prepareBulk()).thenAnswer(invocation -> new BulkRequestBuilder(client));
        action = createAction();
    }

    /** Construct the signal-specific transport action under test. */
    protected abstract AbstractOTLPTransportAction createAction();

    /** Create an {@link OTLPActionRequest} containing at least one data point / log record. */
    protected abstract OTLPActionRequest createRequestWithData();

    /** Create an {@link OTLPActionRequest} with no data points / log records. */
    protected abstract OTLPActionRequest createEmptyRequest();

    /** Parse whether the protobuf response has a partial success field set. */
    protected abstract boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException;

    /** Parse the rejected count from the partial success in the protobuf response. */
    protected abstract long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException;

    /** Parse the error message from the partial success in the protobuf response. */
    protected abstract String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException;

    /** The data stream type, e.g. {@code "metrics"} or {@code "logs"}. Used to construct index names in bulk responses. */
    protected abstract String dataStreamType();

    // --- shared tests ---

    public void testSuccess() throws Exception {
        OTLPActionResponse response = executeRequest(createRequestWithData());

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        assertThat(parseHasPartialSuccess(response.getResponse().array()), equalTo(false));
    }

    public void testSuccessEmptyRequest() throws Exception {
        OTLPActionResponse response = executeRequest(createEmptyRequest());

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        assertThat(parseHasPartialSuccess(response.getResponse().array()), equalTo(false));
    }

    public void test429() throws Exception {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            bulkItemFailure(dataStreamType() + "-generic.otel-default", RestStatus.TOO_MANY_REQUESTS, "too many requests"),
            successResponse() };
        OTLPActionResponse response = executeRequest(createRequestWithData(), new BulkResponse(bulkItemResponses, 0));

        assertThat(response.getStatus(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(
            parseRejectedCount(response.getResponse().array()),
            equalTo(Arrays.stream(bulkItemResponses).filter(BulkItemResponse::isFailed).count())
        );
        assertThat(parseErrorMessage(response.getResponse().array()), containsString("too many requests"));
    }

    public void testPartialSuccess() throws Exception {
        OTLPActionResponse response = executeRequest(
            createRequestWithData(),
            new BulkResponse(
                new BulkItemResponse[] {
                    bulkItemFailure(dataStreamType() + "-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 1"),
                    bulkItemFailure(dataStreamType() + "-generic.otel-default", RestStatus.BAD_REQUEST, "bad request 2"),
                    bulkItemFailure(dataStreamType() + "-other.otel-default", RestStatus.BAD_REQUEST, "bad request 3"),
                    bulkItemFailure(
                        dataStreamType() + "-generic.otel-default",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        "internal server error"
                    ) },
                0
            )
        );

        assertThat(response.getStatus(), equalTo(RestStatus.OK));
        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(1L));
        // the error message contains only one message per unique index and error status
        String errorMessage = parseErrorMessage(responseBytes);
        assertThat(errorMessage, containsString("bad request 1"));
        assertThat(errorMessage, not(containsString("bad request  2")));
        assertThat(errorMessage, containsString("bad request 3"));
        assertThat(errorMessage, containsString("internal server error"));
    }

    public void testBulkError() throws Exception {
        assertExceptionStatus(new IllegalArgumentException("bazinga"), RestStatus.BAD_REQUEST);
        assertExceptionStatus(new IllegalStateException("bazinga"), RestStatus.INTERNAL_SERVER_ERROR);
    }

    private void assertExceptionStatus(Exception exception, RestStatus restStatus) throws InvalidProtocolBufferException {
        if (randomBoolean()) {
            doThrow(exception).when(client).execute(any(), any(), any());
        }
        OTLPActionResponse response = executeRequest(createRequestWithData(), exception);

        assertThat(response.getStatus(), equalTo(restStatus));
        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(1L));
        assertThat(parseErrorMessage(responseBytes), equalTo(exception.getMessage()));
    }

    // --- shared test infrastructure ---

    protected OTLPActionResponse executeRequest(OTLPActionRequest request) {
        return executeRequest(request, listener -> listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 0)));
    }

    protected OTLPActionResponse executeRequest(OTLPActionRequest request, BulkResponse bulkResponse) {
        return executeRequest(request, listener -> listener.onResponse(bulkResponse));
    }

    protected OTLPActionResponse executeRequest(OTLPActionRequest request, Exception bulkFailure) {
        return executeRequest(request, listener -> listener.onFailure(bulkFailure));
    }

    protected OTLPActionResponse executeRequest(OTLPActionRequest request, Consumer<ActionListener<BulkResponse>> bulkResponseConsumer) {
        ArgumentCaptor<ActionListener<BulkResponse>> bulkResponseListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), any(), bulkResponseListener.capture());

        ActionListener<OTLPActionResponse> responseListener = mock();
        action.doExecute(null, request, responseListener);
        if (bulkResponseListener.getAllValues().isEmpty() == false) {
            bulkResponseConsumer.accept(bulkResponseListener.getValue());
        }

        ArgumentCaptor<OTLPActionResponse> response = ArgumentCaptor.forClass(OTLPActionResponse.class);
        verify(responseListener).onResponse(response.capture());
        return response.getValue();
    }

    protected static BulkItemResponse successResponse() {
        return BulkItemResponse.success(-1, DocWriteRequest.OpType.CREATE, mock(DocWriteResponse.class));
    }

    protected static BulkItemResponse bulkItemFailure(String index, RestStatus restStatus, String failureMessage) {
        return BulkItemResponse.failure(
            -1,
            DocWriteRequest.OpType.CREATE,
            new BulkItemResponse.Failure(index, "id", new RuntimeException(failureMessage), restStatus)
        );
    }
}
