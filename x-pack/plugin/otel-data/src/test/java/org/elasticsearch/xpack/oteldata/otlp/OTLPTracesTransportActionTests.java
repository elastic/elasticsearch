/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class OTLPTracesTransportActionTests extends AbstractOTLPTransportActionTests {

    @Override
    protected AbstractOTLPTransportAction createAction() {
        return new OTLPTracesTransportAction(
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

    public void testPrepareBulkRequestUsesEncodingScopeRouting() throws Exception {
        InstrumentationScope scope = InstrumentationScope.newBuilder()
            .setName("github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension")
            .addAttributes(OtlpUtils.keyValue("encoding.format", "aws.cloudtrail"))
            .build();

        String indexName = prepareIndexName(
            ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(
                    OtlpTraceUtils.createResourceSpans(
                        List.of(OtlpUtils.keyValue("service.name", "test-service")),
                        List.of(ScopeSpans.newBuilder().setScope(scope).addSpans(OtlpTraceUtils.createSpan("test-span")).build())
                    )
                )
                .build()
        );

        assertThat(indexName, equalTo("traces-aws.cloudtrail.otel-default"));
    }

    private String prepareIndexName(ExportTraceServiceRequest request) throws Exception {
        OTLPTracesTransportAction tracesAction = (OTLPTracesTransportAction) createAction();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        tracesAction.prepareBulkRequest(new OTLPActionRequest(new BytesArray(request.toByteArray())), bulkRequestBuilder);
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        return indexRequest.index();
    }

    public void testPrepareBulkRequestUsesDocumentIdAttribute() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        createAction().prepareBulkRequest(
            new OTLPActionRequest(
                new BytesArray(
                    OtlpTraceUtils.createTracesRequest(
                        List.of(
                            OtlpTraceUtils.createSpan("test-span", List.of(OtlpUtils.keyValue("elasticsearch.document_id", "trace-doc-id")))
                        )
                    ).toByteArray()
                )
            ),
            bulkRequestBuilder
        );

        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.id(), equalTo("trace-doc-id"));
    }

    public void testPrepareBulkRequestLeavesDocumentIdUnsetWhenAttributeMissing() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        createAction().prepareBulkRequest(createRequestWithData(), bulkRequestBuilder);

        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.id(), nullValue());
    }

    public void testPrepareBulkRequestAddsSpanEventDocuments() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        createAction().prepareBulkRequest(
            new OTLPActionRequest(
                new BytesArray(
                    OtlpTraceUtils.createTracesRequest(
                        List.of(
                            OtlpTraceUtils.createSpan(
                                "test-span",
                                List.of(),
                                List.of(
                                    OtlpTraceUtils.createEvent(
                                        "exception",
                                        2_000_000_000L,
                                        List.of(OtlpUtils.keyValue("event.attr.foo", "event.attr.bar"))
                                    )
                                )
                            )
                        )
                    ).toByteArray()
                )
            ),
            bulkRequestBuilder
        );

        var bulkRequest = bulkRequestBuilder.request();
        assertThat(bulkRequest.requests(), hasSize(2));

        IndexRequest spanRequest = (IndexRequest) bulkRequest.requests().get(0);
        IndexRequest eventRequest = (IndexRequest) bulkRequest.requests().get(1);

        assertThat(spanRequest.index(), equalTo("traces-generic.otel-default"));
        assertThat(eventRequest.index(), equalTo("logs-generic.otel-default"));
        assertThat(eventRequest.sourceAsMap().get("event_name"), equalTo("exception"));
        @SuppressWarnings("unchecked")
        Map<String, Object> attributes = (Map<String, Object>) eventRequest.sourceAsMap().get("attributes");
        assertThat(attributes.get("event.attr.foo"), equalTo("event.attr.bar"));
        assertThat(attributes.get("event.name"), equalTo("exception"));
    }

    public void testAttributeFanoutReturns413() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        OTLPTracesTransportAction action = new OTLPTracesTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            settings
        );

        // ~1 KiB resource attribute × 15 spans ≈ 15 KiB, exceeds the 10 KiB limit
        String largeValue = "x".repeat(1024);
        List<Span> spans = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            spans.add(OtlpTraceUtils.createSpan("span-" + i));
        }
        OTLPActionRequest request = new OTLPActionRequest(
            new BytesArray(
                OtlpTraceUtils.createTracesRequest(List.of(OtlpUtils.keyValue("resource.large", largeValue)), spans).toByteArray()
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

    public void testPrepareBulkRequestUsesSpanEventDocumentIdAttribute() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        createAction().prepareBulkRequest(
            new OTLPActionRequest(
                new BytesArray(
                    OtlpTraceUtils.createTracesRequest(
                        List.of(
                            OtlpTraceUtils.createSpan(
                                "test-span",
                                List.of(),
                                List.of(
                                    OtlpTraceUtils.createEvent(
                                        "exception",
                                        2_000_000_000L,
                                        List.of(OtlpUtils.keyValue("elasticsearch.document_id", "span-event-doc-id"))
                                    )
                                )
                            )
                        )
                    ).toByteArray()
                )
            ),
            bulkRequestBuilder
        );

        var bulkRequest = bulkRequestBuilder.request();
        IndexRequest eventRequest = (IndexRequest) bulkRequest.requests().get(1);
        assertThat(eventRequest.id(), equalTo("span-event-doc-id"));
    }
}
