/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.SeverityNumber;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.mockito.Mockito.mock;

public class OTLPLogsTransportActionTests extends AbstractOTLPTransportActionTests {

    @Override
    protected AbstractOTLPTransportAction createAction() {
        return new OTLPLogsTransportAction(mock(TransportService.class), mock(ActionFilters.class), mock(ThreadPool.class), client);
    }

    @Override
    protected OTLPActionRequest createRequestWithData() {
        return new OTLPActionRequest(
            new BytesArray(
                OtlpLogUtils.createLogsRequest(
                    List.of(OtlpLogUtils.createLogRecord("Hello world", SeverityNumber.SEVERITY_NUMBER_INFO, "INFO"))
                ).toByteArray()
            )
        );
    }

    @Override
    protected OTLPActionRequest createEmptyRequest() {
        return new OTLPActionRequest(new BytesArray(OtlpLogUtils.createLogsRequest(List.of()).toByteArray()));
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportLogsServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportLogsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedLogRecords();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportLogsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "logs";
    }
}
