/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Parses OTLP protobuf log records into the protocol-neutral {@link ReceivedTelemetry.ReceivedLog}.
 * Used by both the HTTP {@code /v1/logs} route and the gRPC {@code LogsServiceImpl} in
 * {@link RecordingApmServer}.
 */
public final class OtlpLogsParser {

    private OtlpLogsParser() {}

    public static List<ReceivedTelemetry> parse(InputStream input) throws IOException {
        ExportLogsServiceRequest request = ExportLogsServiceRequest.parseFrom(input);
        List<ReceivedTelemetry> result = new ArrayList<>();
        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                for (LogRecord record : scopeLogs.getLogRecordsList()) {
                    result.add(toReceivedLog(record));
                }
            }
        }
        return result;
    }

    static ReceivedTelemetry.ReceivedLog toReceivedLog(LogRecord record) {
        Map<String, Object> attributes = new HashMap<>();
        for (KeyValue kv : record.getAttributesList()) {
            Object value = unwrap(kv.getValue());
            if (value != null) {
                attributes.put(kv.getKey(), value);
            }
        }
        Optional<String> traceId = record.getTraceId().isEmpty()
            ? Optional.empty()
            : Optional.of(HexFormat.of().formatHex(record.getTraceId().toByteArray()));
        return new ReceivedTelemetry.ReceivedLog(
            record.getTimeUnixNano(),
            record.getSeverityNumberValue(),
            record.getSeverityText(),
            record.getBody().getStringValue(),
            attributes,
            traceId
        );
    }

    private static Object unwrap(AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> value.getIntValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case BOOL_VALUE -> value.getBoolValue();
            default -> null;
        };
    }
}
