/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.mockito.Mockito.mock;

public class OTLPTracesTransportActionTests extends AbstractOTLPTransportActionTests {

    @Override
    protected AbstractOTLPTransportAction createAction() {
        return new OTLPTracesTransportAction(mock(TransportService.class), mock(ActionFilters.class), mock(ThreadPool.class), client);
    }

    @Override
    protected OTLPActionRequest createRequestWithData() {
        return new OTLPActionRequest(
            new BytesArray(OtlpTraceUtils.createTracesRequest(List.of(OtlpTraceUtils.createSpan("test-span"))).toByteArray())
        );
    }

    @Override
    protected OTLPActionRequest createEmptyRequest() {
        return new OTLPActionRequest(new BytesArray(OtlpTraceUtils.createTracesRequest(List.of()).toByteArray()));
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportTraceServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportTraceServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedSpans();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportTraceServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "traces";
    }
}
