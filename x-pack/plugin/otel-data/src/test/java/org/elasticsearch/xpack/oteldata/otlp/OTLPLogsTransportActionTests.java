/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.SeverityNumber;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class OTLPLogsTransportActionTests extends AbstractOTLPTransportActionTests {

    @Override
    protected AbstractOTLPTransportAction createAction() {
        return new OTLPLogsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            Settings.EMPTY
        );
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

    public void testAttributeFanoutReturns413() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        OTLPLogsTransportAction action = new OTLPLogsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            settings
        );

        // ~1 KiB resource attribute × 15 log records ≈ 15 KiB, exceeds the 10 KiB limit
        String largeValue = "x".repeat(1024);
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            logRecords.add(OtlpLogUtils.createLogRecord("body", SeverityNumber.SEVERITY_NUMBER_INFO, "INFO"));
        }
        OTLPActionRequest request = new OTLPActionRequest(
            new BytesArray(
                OtlpLogUtils.createLogsRequest(List.of(OtlpUtils.keyValue("resource.large", largeValue)), logRecords).toByteArray()
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        assertThat(ExceptionsHelper.status(exception.getValue()), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(exception.getValue().getMessage(), containsString("attribute data written across all documents would exceed limit"));
        verify(client, never()).execute(any(), any(), any());
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
