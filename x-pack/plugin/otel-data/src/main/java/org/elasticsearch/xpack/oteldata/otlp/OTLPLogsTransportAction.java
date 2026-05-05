/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.MessageLite;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.LogDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) logs requests.
 * This action processes the incoming logs data and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the logs.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPLogsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/logs";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public OTLPLogsTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool, Client client) {
        super(NAME, transportService, actionFilters, threadPool, client);
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        var logsServiceRequest = ExportLogsServiceRequest.parseFrom(request.getRequest().streamInput());
        LogDocumentBuilder logDocumentBuilder = new LogDocumentBuilder(byteStringAccessor);
        List<ResourceLogs> resourceLogsList = logsServiceRequest.getResourceLogsList();
        MappingMode requestMappingMode = request.getRequestMappingMode();
        LogsProcessingContext context = new LogsProcessingContext();
        for (int i = 0, resourceLogsListSize = resourceLogsList.size(); i < resourceLogsListSize; i++) {
            ResourceLogs resourceLogs = resourceLogsList.get(i);
            Resource resource = resourceLogs.getResource();
            List<ScopeLogs> scopeLogsList = resourceLogs.getScopeLogsList();
            for (int j = 0, scopeLogsListSize = scopeLogsList.size(); j < scopeLogsListSize; j++) {
                ScopeLogs scopeLogs = scopeLogsList.get(j);
                InstrumentationScope scope = scopeLogs.getScope();
                List<LogRecord> logRecordsList = scopeLogs.getLogRecordsList();
                MappingMode mode = requestMappingMode;
                String scopeMappingMode = scopeMappingMode(scope);
                if (scopeMappingMode != null) {
                    mode = MappingMode.parse(scopeMappingMode);
                }
                String scopeRoutingDataset = TargetIndex.extractScopeRoutingDataset(scope);
                for (int k = 0, logRecordsListSize = logRecordsList.size(); k < logRecordsListSize; k++) {
                    LogRecord logRecord = logRecordsList.get(k);
                    context.incrementTotalLogRecords();
                    if (mode == MappingMode.BODYMAP && logRecord.getBody().hasKvlistValue() == false) {
                        context.rejectInvalidBodyType(logRecord);
                        continue;
                    }
                    TargetIndex index = TargetIndex.evaluate(
                        TargetIndex.TYPE_LOGS,
                        mode,
                        logRecord.getAttributesList(),
                        scopeRoutingDataset,
                        scope.getAttributesList(),
                        resource.getAttributesList()
                    );
                    try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                        if (mode == MappingMode.BODYMAP) {
                            logDocumentBuilder.buildBodyMapLogDocument(xContentBuilder, logRecord);
                        } else {
                            logDocumentBuilder.buildLogDocument(
                                xContentBuilder,
                                resource,
                                resourceLogs.getSchemaUrlBytes(),
                                scope,
                                scopeLogs.getSchemaUrlBytes(),
                                index,
                                logRecord
                            );
                        }
                        IndexRequest indexRequest = new IndexRequest(index.index());
                        String documentId = DocumentMetadata.documentId(logRecord.getAttributesList());
                        if (Strings.hasLength(documentId)) {
                            indexRequest.id(documentId);
                        }
                        String ingestPipeline = DocumentMetadata.ingestPipeline(logRecord.getAttributesList());
                        if (Strings.hasLength(ingestPipeline)) {
                            indexRequest.setPipeline(ingestPipeline);
                        }
                        bulkRequestBuilder.add(
                            indexRequest.opType(DocWriteRequest.OpType.CREATE).setRequireDataStream(true).source(xContentBuilder)
                        );
                    }
                }
            }
        }
        return context;
    }

    private static @Nullable String scopeMappingMode(InstrumentationScope scope) {
        for (int i = 0, size = scope.getAttributesCount(); i < size; i++) {
            KeyValue attribute = scope.getAttributes(i);
            if (MappingMode.SCOPE_ATTRIBUTE.equals(attribute.getKey())) {
                return attribute.getValue().getStringValue();
            }
        }
        return null;
    }

    private static class LogsProcessingContext implements ProcessingContext {

        private int totalLogRecords;
        private int ignoredLogRecords;
        private final Set<String> ignoredLogRecordMessages = new HashSet<>();

        private void incrementTotalLogRecords() {
            totalLogRecords++;
        }

        private void incrementTotalLogRecords(int logRecords) {
            totalLogRecords += logRecords;
        }

        private void rejectInvalidBodyType(LogRecord logRecord) {
            rejectLogRecord("Invalid log record body type for 'bodymap' mapping mode: " + logRecord.getBody().getValueCase());
        }

        private void rejectLogRecord(String message) {
            ignoredLogRecords++;
            addIgnoredLogRecordMessage(message);
        }

        private void addIgnoredLogRecordMessage(String message) {
            ignoredLogRecordMessages.add(message);
        }

        @Override
        public int totalItems() {
            return totalLogRecords;
        }

        @Override
        public int getIgnoredItems() {
            return ignoredLogRecords;
        }

        @Override
        public String getIgnoredItemsMessage(int limit) {
            if (ignoredLogRecordMessages.isEmpty()) {
                return "";
            }
            StringBuilder message = new StringBuilder();
            message.append("Ignored ").append(ignoredLogRecords).append(" log records due to the following reasons:\n");
            int count = 0;
            for (String ignoredLogRecordMessage : ignoredLogRecordMessages) {
                message.append(" - ").append(ignoredLogRecordMessage).append('\n');
                count++;
                if (count >= limit) {
                    break;
                }
            }
            if (count < ignoredLogRecordMessages.size()) {
                message.append(" - ... and more\n");
            }
            return message.toString();
        }
    }

    @Override
    MessageLite responseWithRejectedItems(int rejectedItems, String message) {
        ExportLogsPartialSuccess partialSuccess = ExportLogsPartialSuccess.newBuilder()
            .setRejectedLogRecords(rejectedItems)
            .setErrorMessage(message)
            .build();
        return ExportLogsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }
}
