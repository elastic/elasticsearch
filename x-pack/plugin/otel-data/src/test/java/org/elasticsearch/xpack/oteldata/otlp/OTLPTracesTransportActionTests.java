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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
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
