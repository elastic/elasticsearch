/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import com.google.protobuf.MessageLite;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.SpanDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.SpanEventDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) traces requests.
 * This action processes the incoming traces data and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the traces.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPTracesTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/traces";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    public static final String TYPE_TRACES = "traces";
    private static final String DOCUMENT_ID_ATTRIBUTE = "elasticsearch.document_id";

    @Inject
    public OTLPTracesTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool, Client client) {
        super(NAME, transportService, actionFilters, threadPool, client);
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        var tracesServiceRequest = ExportTraceServiceRequest.parseFrom(request.getRequest().streamInput());
        SpanDocumentBuilder spanDocumentBuilder = new SpanDocumentBuilder(byteStringAccessor);
        SpanEventDocumentBuilder spanEventDocumentBuilder = new SpanEventDocumentBuilder(byteStringAccessor);
        List<ResourceSpans> resourceSpansList = tracesServiceRequest.getResourceSpansList();
        for (int i = 0, resourceSpansListSize = resourceSpansList.size(); i < resourceSpansListSize; i++) {
            ResourceSpans resourceSpans = resourceSpansList.get(i);
            Resource resource = resourceSpans.getResource();
            List<ScopeSpans> scopeSpansList = resourceSpans.getScopeSpansList();
            for (int j = 0, scopeSpansListSize = scopeSpansList.size(); j < scopeSpansListSize; j++) {
                ScopeSpans scopeSpans = scopeSpansList.get(j);
                InstrumentationScope scope = scopeSpans.getScope();
                String scopeRoutingDataset = TargetIndex.extractScopeRoutingDataset(scope);
                List<Span> spansList = scopeSpans.getSpansList();
                for (int k = 0, spansListSize = spansList.size(); k < spansListSize; k++) {
                    Span span = spansList.get(k);
                    TargetIndex index = TargetIndex.evaluate(
                        TYPE_TRACES,
                        span.getAttributesList(),
                        scopeRoutingDataset,
                        scope.getAttributesList(),
                        resource.getAttributesList()
                    );
                    try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                        spanDocumentBuilder.buildSpanDocument(
                            xContentBuilder,
                            resource,
                            resourceSpans.getSchemaUrlBytes(),
                            scope,
                            scopeSpans.getSchemaUrlBytes(),
                            index,
                            span
                        );
                        IndexRequest indexRequest = new IndexRequest(index.index());
                        String documentId = extractDocumentId(span.getAttributesList());
                        if (documentId.isEmpty() == false) {
                            indexRequest.id(documentId);
                        }
                        bulkRequestBuilder.add(
                            indexRequest.opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(xContentBuilder)
                        );
                    }
                    List<Span.Event> eventsList = span.getEventsList();
                    for (int l = 0, eventsListSize = eventsList.size(); l < eventsListSize; l++) {
                        Span.Event event = eventsList.get(l);
                        TargetIndex eventIndex = TargetIndex.evaluate(
                            OTLPLogsTransportAction.TYPE_LOGS,
                            event.getAttributesList(),
                            scopeRoutingDataset,
                            scope.getAttributesList(),
                            resource.getAttributesList()
                        );
                        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                            spanEventDocumentBuilder.buildSpanEventDocument(
                                xContentBuilder,
                                resource,
                                resourceSpans.getSchemaUrlBytes(),
                                scope,
                                scopeSpans.getSchemaUrlBytes(),
                                eventIndex,
                                span,
                                event
                            );
                            IndexRequest eventRequest = new IndexRequest(eventIndex.index());
                            String eventDocumentId = extractDocumentId(event.getAttributesList());
                            if (eventDocumentId.isEmpty() == false) {
                                eventRequest.id(eventDocumentId);
                            }
                            bulkRequestBuilder.add(
                                eventRequest.opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(xContentBuilder)
                            );
                        }
                    }
                }
            }
        }
        return ProcessingContext.withTotalDataPoints(bulkRequestBuilder.numberOfActions());
    }

    @Override
    MessageLite responseWithRejectedDataPoints(int rejectedDataPoints, String message) {
        ExportTracePartialSuccess partialSuccess = ExportTracePartialSuccess.newBuilder()
            .setRejectedSpans(rejectedDataPoints)
            .setErrorMessage(message)
            .build();
        return ExportTraceServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }

    private static String extractDocumentId(List<KeyValue> attributes) {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            if (DOCUMENT_ID_ATTRIBUTE.equals(attribute.getKey())) {
                return attribute.getValue().getStringValue();
            }
        }
        return "";
    }
}
