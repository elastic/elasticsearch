/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.oteldata.otlp.AbstractOTLPTransportAction.ProcessingContext;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    public void testBodyMapMappingModeUsesBodyAsDocument() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder()
            .setTimeUnixNano(1_000_000_000L)
            .setSeverityText("ignored")
            .setBody(
                AnyValue.newBuilder()
                    .setKvlistValue(
                        KeyValueList.newBuilder()
                            .addValues(keyValue("@timestamp", "2024-03-12T20:00:41.123456789Z"))
                            .addValues(keyValue("id", 1L))
                            .addValues(keyValue("key", "value"))
                            .addValues(keyValue("key.a", "a"))
                    )
                    .build()
            )
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(createBodyMapRequest(List.of(logRecord)), bulkRequestBuilder);

        assertThat(context.totalDataPoints(), equalTo(1));
        assertThat(context.getIgnoredDataPoints(), equalTo(0));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        Map<String, Object> source = indexRequest.sourceAsMap();
        assertThat(source.get("@timestamp"), equalTo("2024-03-12T20:00:41.123456789Z"));
        assertThat(((Number) source.get("id")).longValue(), equalTo(1L));
        assertThat(source.get("key"), equalTo("value"));
        assertThat(source.get("key.a"), equalTo("a"));
        assertThat(source.get("body"), equalTo(null));
        assertThat(source.get("severity_text"), equalTo(null));
    }

    public void testBodyMapMappingModeFromRequestUsesBodyAsDocument() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", 42L))).build())
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(logRecord), null, MappingMode.BODYMAP),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(1));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(((Number) indexRequest.sourceAsMap().get("a")).longValue(), equalTo(42L));
    }

    public void testScopeMappingModeTakesPrecedenceOverRequestMappingMode() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("not a bodymap").build()).build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(logRecord), "otel", MappingMode.BODYMAP),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(1));
        assertThat(context.getIgnoredDataPoints(), equalTo(0));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.sourceAsMap().get("body"), equalTo(Map.of("text", "not a bodymap")));
    }

    public void testUnknownScopeMappingModeSkipsScopeAndIsReported() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord invalidScopeLogRecord = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("skip me").build()).build();
        LogRecord validScopeLogRecord = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("index me").build()).build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createLogsRequest(
                MappingMode.OTEL,
                createScopeLogs(List.of(invalidScopeLogRecord), "ecs"),
                createScopeLogs(List.of(validScopeLogRecord), "otel")
            ),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(2));
        assertThat(context.getIgnoredDataPoints(), equalTo(1));
        assertThat(context.getIgnoredDataPointsMessage(10), containsString("Unsupported mapping mode [ecs]"));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.index(), equalTo("logs-generic.otel-default"));
        assertThat(indexRequest.sourceAsMap().get("body"), equalTo(Map.of("text", "index me")));
    }

    public void testUnknownScopeMappingModeReturnsPartialSuccessForMixedRequest() throws Exception {
        LogRecord invalidScopeLogRecord1 = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("skip me").build()).build();
        LogRecord invalidScopeLogRecord2 = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setStringValue("skip me too").build())
            .build();
        LogRecord validScopeLogRecord = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("index me").build()).build();
        OTLPActionRequest request = createLogsRequest(
            MappingMode.OTEL,
            createScopeLogs(List.of(invalidScopeLogRecord1, invalidScopeLogRecord2), "ecs"),
            createScopeLogs(List.of(validScopeLogRecord), "otel")
        );

        OTLPActionResponse response = executeRequest(request, new BulkResponse(new BulkItemResponse[] { successResponse() }, 0));

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseHasPartialSuccess(responseBytes), equalTo(true));
        assertThat(parseRejectedCount(responseBytes), equalTo(2L));
        assertThat(parseErrorMessage(responseBytes), containsString("Unsupported mapping mode [ecs]"));
    }

    public void testBodyMapModeDoesNotRouteFromBodyDataStreamType() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder()
            .setBody(
                AnyValue.newBuilder()
                    .setKvlistValue(
                        KeyValueList.newBuilder().addValues(keyValue("data_stream.type", "metrics")).addValues(keyValue("a", 42L))
                    )
                    .build()
            )
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(logRecord), null, MappingMode.BODYMAP),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(1));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.index(), equalTo("logs-generic-default"));
    }

    public void testBodyMapModeAllowsDataStreamTypeOverrideFromAttributesOnly() throws Exception {
        LogRecord logRecord = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", 42L))).build())
            .addAttributes(keyValue("data_stream.type", "metrics"))
            .build();

        assertBodyMapModeRoutesDataStreamTypeOverride(logRecord);
    }

    private void assertBodyMapModeRoutesDataStreamTypeOverride(LogRecord logRecord) throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(logRecord), null, MappingMode.BODYMAP),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(1));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.index(), equalTo("metrics-generic-default"));
    }

    public void testBodyMapModeRejectsInvalidDataStreamType() {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", 42L))).build())
            .addAttributes(keyValue("data_stream.type", "traces"))
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> logsAction.prepareBulkRequest(createBodyMapRequest(List.of(logRecord), null, MappingMode.BODYMAP), bulkRequestBuilder)
        );
        assertThat(e.getMessage(), containsString("data_stream.type cannot be other than logs or metrics"));
    }

    public void testBodyMapModeRoutesWithoutOtelSuffix() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord logRecord = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", 42L))).build())
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(logRecord), null, MappingMode.BODYMAP),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(indexRequest.index(), equalTo("logs-generic-default"));
    }

    public void testBodyMapMappingModeIgnoresInvalidBodyTypes() throws Exception {
        OTLPLogsTransportAction logsAction = (OTLPLogsTransportAction) createAction();
        LogRecord invalidLogRecord1 = LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue("not a map").build()).build();
        LogRecord invalidLogRecord2 = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setStringValue("also not a map").build())
            .build();
        LogRecord validLogRecord = LogRecord.newBuilder()
            .setBody(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addValues(keyValue("a", 42L))).build())
            .build();
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        ProcessingContext context = logsAction.prepareBulkRequest(
            createBodyMapRequest(List.of(invalidLogRecord1, invalidLogRecord2, validLogRecord)),
            bulkRequestBuilder
        );

        assertThat(context.totalDataPoints(), equalTo(3));
        assertThat(context.getIgnoredDataPoints(), equalTo(2));
        String ignoredDataPointsMessage = context.getIgnoredDataPointsMessage(10);
        String invalidBodyTypeMessage = "Invalid log record body type for 'bodymap' mapping mode";
        assertThat(ignoredDataPointsMessage, containsString(invalidBodyTypeMessage));
        assertThat(
            ignoredDataPointsMessage.indexOf(invalidBodyTypeMessage),
            equalTo(ignoredDataPointsMessage.lastIndexOf(invalidBodyTypeMessage))
        );
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(1));
        IndexRequest indexRequest = (IndexRequest) bulkRequestBuilder.request().requests().get(0);
        assertThat(((Number) indexRequest.sourceAsMap().get("a")).longValue(), equalTo(42L));
    }

    private static OTLPActionRequest createBodyMapRequest(List<LogRecord> logRecords) {
        return createBodyMapRequest(logRecords, "bodymap", MappingMode.OTEL);
    }

    private static OTLPActionRequest createBodyMapRequest(
        List<LogRecord> logRecords,
        String scopeMappingMode,
        MappingMode requestMappingMode
    ) {
        return createLogsRequest(requestMappingMode, createScopeLogs(logRecords, scopeMappingMode));
    }

    private static OTLPActionRequest createLogsRequest(MappingMode requestMappingMode, ScopeLogs... scopeLogs) {
        return new OTLPActionRequest(
            new BytesArray(
                ExportLogsServiceRequest.newBuilder()
                    .addResourceLogs(ResourceLogs.newBuilder().addAllScopeLogs(List.of(scopeLogs)))
                    .build()
                    .toByteArray()
            ),
            requestMappingMode
        );
    }

    private static ScopeLogs createScopeLogs(List<LogRecord> logRecords, String scopeMappingMode) {
        InstrumentationScope.Builder scope = InstrumentationScope.newBuilder().setName("test");
        if (scopeMappingMode != null) {
            scope.addAttributes(keyValue("elastic.mapping.mode", scopeMappingMode));
        }
        return ScopeLogs.newBuilder().setScope(scope).addAllLogRecords(logRecords).build();
    }
}
