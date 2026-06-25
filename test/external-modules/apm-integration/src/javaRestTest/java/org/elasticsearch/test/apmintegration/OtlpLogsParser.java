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
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * Parses OTLP protobuf log records into the protocol-neutral {@link ReceivedTelemetry.ReceivedLog}.
 * Used by the gRPC {@code LogsServiceImpl} in {@link RecordingApmServer}.
 */
public final class OtlpLogsParser extends OtlpParser {

    private OtlpLogsParser() {}

    static List<ReceivedTelemetry> parse(ExportLogsServiceRequest request) {
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
        Optional<String> traceId = record.getTraceId().isEmpty()
            ? Optional.empty()
            : Optional.of(HexFormat.of().formatHex(record.getTraceId().toByteArray()));
        return new ReceivedTelemetry.ReceivedLog(
            record.getTimeUnixNano(),
            record.getSeverityNumberValue(),
            record.getSeverityText(),
            record.getBody().getStringValue(),
            extractRawAttributes(record.getAttributesList()),
            traceId
        );
    }
}
