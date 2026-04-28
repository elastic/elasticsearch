/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.List;

public class OtlpLogUtils {

    private static Resource createResource(List<KeyValue> attributes) {
        return Resource.newBuilder().addAllAttributes(attributes).build();
    }

    private static InstrumentationScope createScope(String name, String version) {
        return InstrumentationScope.newBuilder().setName(name).setVersion(version).build();
    }

    public static LogRecord createLogRecord(String body, SeverityNumber severityNumber, String severityText) {
        return createLogRecord(body, severityNumber, severityText, List.of());
    }

    public static LogRecord createLogRecord(String body, SeverityNumber severityNumber, String severityText, List<KeyValue> attributes) {
        return LogRecord.newBuilder()
            .setTimeUnixNano(System.nanoTime())
            .setObservedTimeUnixNano(System.nanoTime())
            .setSeverityNumber(severityNumber)
            .setSeverityText(severityText)
            .setBody(AnyValue.newBuilder().setStringValue(body).build())
            .addAllAttributes(attributes)
            .build();
    }

    public static ResourceLogs createResourceLogs(List<KeyValue> resourceAttributes, List<ScopeLogs> scopeLogs) {
        return ResourceLogs.newBuilder().setResource(createResource(resourceAttributes)).addAllScopeLogs(scopeLogs).build();
    }

    public static ScopeLogs createScopeLogs(String name, String version, List<LogRecord> logRecords) {
        return ScopeLogs.newBuilder().setScope(createScope(name, version)).addAllLogRecords(logRecords).build();
    }

    public static ExportLogsServiceRequest createLogsRequest(List<LogRecord> logRecords) {
        return createLogsRequest(List.of(OtlpUtils.keyValue("service.name", "test-service")), logRecords);
    }

    public static ExportLogsServiceRequest createLogsRequest(List<KeyValue> resourceAttributes, List<LogRecord> logRecords) {
        return ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(createResourceLogs(resourceAttributes, List.of(createScopeLogs("test", "1.0.0", logRecords))))
            .build();
    }
}
