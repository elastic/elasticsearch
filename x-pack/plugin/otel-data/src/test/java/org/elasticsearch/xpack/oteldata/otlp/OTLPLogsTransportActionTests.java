/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.SeverityNumber;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
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

    public void testPrepareBulkRequestUsesDocumentIdAttribute() throws Exception {
        IndexRequest indexRequest = prepareIndexRequestWithAttributes(
            List.of(OtlpUtils.keyValue(DocumentMetadata.DOCUMENT_ID_ATTRIBUTE, "log-doc-id"))
        );

        assertThat(indexRequest.id(), equalTo("log-doc-id"));
        @SuppressWarnings("unchecked")
        Map<String, Object> attributes = (Map<String, Object>) indexRequest.sourceAsMap().get("attributes");
        assertThat(attributes.get(DocumentMetadata.DOCUMENT_ID_ATTRIBUTE), nullValue());
    }

    public void testPrepareBulkRequestLeavesDocumentIdUnsetWhenAttributeEmpty() throws Exception {
        IndexRequest indexRequest = prepareIndexRequestWithAttributes(
            List.of(OtlpUtils.keyValue(DocumentMetadata.DOCUMENT_ID_ATTRIBUTE, ""))
        );

        assertThat(indexRequest.id(), nullValue());
    }

    public void testPrepareBulkRequestUsesIngestPipelineAttribute() throws Exception {
        IndexRequest indexRequest = prepareIndexRequestWithAttributes(
            List.of(OtlpUtils.keyValue(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE, "logs-pipeline"))
        );

        assertThat(indexRequest.getPipeline(), equalTo("logs-pipeline"));
        @SuppressWarnings("unchecked")
        Map<String, Object> attributes = (Map<String, Object>) indexRequest.sourceAsMap().get("attributes");
        assertThat(attributes.get(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE), nullValue());
    }

    public void testPrepareBulkRequestLeavesPipelineUnsetWhenAttributeEmpty() throws Exception {
        IndexRequest indexRequest = prepareIndexRequestWithAttributes(
            List.of(OtlpUtils.keyValue(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE, ""))
        );

        assertThat(indexRequest.getPipeline(), nullValue());
    }

    private IndexRequest prepareIndexRequestWithAttributes(List<KeyValue> attributes) throws Exception {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        createAction().prepareBulkRequest(
            new OTLPActionRequest(
                new BytesArray(
                    OtlpLogUtils.createLogsRequest(
                        List.of(OtlpLogUtils.createLogRecord("Hello world", SeverityNumber.SEVERITY_NUMBER_INFO, "INFO", attributes))
                    ).toByteArray()
                )
            ),
            bulkRequestBuilder
        );
        return (IndexRequest) bulkRequestBuilder.request().requests().get(0);
    }
}
