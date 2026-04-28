/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;

/**
 * This class constructs an Elasticsearch document representation of an OTel log record.
 */
public class LogDocumentBuilder extends OTelDocumentBuilder {

    public LogDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    public void buildLogDocument(
        XContentBuilder builder,
        Resource resource,
        ByteString resourceSchemaUrl,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        TargetIndex targetIndex,
        LogRecord logRecord
    ) throws IOException {
        builder.startObject();
        long observedTimestamp = logRecord.getObservedTimeUnixNano();
        long docTimestamp = logRecord.getTimeUnixNano();
        if (docTimestamp == 0) {
            docTimestamp = observedTimestamp;
        }
        addEpochMillisNanosField(builder, "@timestamp", docTimestamp);
        addEpochMillisNanosField(builder, "observed_timestamp", observedTimestamp);
        if (logRecord.getSeverityNumber() != SeverityNumber.SEVERITY_NUMBER_UNSPECIFIED) {
            builder.field("severity_number", logRecord.getSeverityNumber().getNumber());
        }
        addFieldIfNotEmpty(builder, "severity_text", logRecord.getSeverityTextBytes());
        ByteString eventName = logRecord.getEventNameBytes();
        if (eventName.isEmpty() == false) {
            addFieldIfNotEmpty(builder, "event_name", eventName);
        } else {
            for (KeyValue attribute : logRecord.getAttributesList()) {
                if ("event.name".equals(attribute.getKey())) {
                    addFieldIfNotEmpty(builder, "event_name", attribute.getValue().getStringValueBytes());
                    break;
                }
            }
        }
        addSpanId(builder, logRecord.getSpanId().toByteArray());
        addTraceId(builder, logRecord.getTraceId().toByteArray());
        buildResource(resource, resourceSchemaUrl, builder);
        buildDataStream(builder, targetIndex);
        buildScope(builder, scope, scopeSchemaUrl);
        buildAttributes(builder, logRecord.getAttributesList(), logRecord.getDroppedAttributesCount());
        buildBody(builder, logRecord);
        builder.endObject();
    }

    private void buildBody(XContentBuilder builder, LogRecord logRecord) throws IOException {
        AnyValue body = logRecord.getBody();
        AnyValue.ValueCase valueCase = body.getValueCase();
        if (valueCase == AnyValue.ValueCase.VALUE_NOT_SET) {
            return;
        }
        if (valueCase == AnyValue.ValueCase.ARRAY_VALUE && body.getArrayValue().getValuesCount() == 0) {
            return;
        }
        builder.startObject("body");
        switch (valueCase) {
            case ARRAY_VALUE: {
                boolean allMaps = true;
                for (var v : body.getArrayValue().getValuesList()) {
                    if (v.hasKvlistValue() == false) {
                        allMaps = false;
                        break;
                    }
                }
                if (allMaps) {
                    buildStructuredBody(builder, body);
                } else {
                    // The flattened field type only accepts objects or arrays of objects
                    // If this is an array of primitive values, for example, wrap the array in a 'value' object
                    builder.field("structured");
                    builder.startObject();
                    builder.field("value");
                    buildAnyValue(builder, body);
                    builder.endObject();
                }
                break;
            }
            case KVLIST_VALUE:
                buildStructuredBody(builder, body);
                break;
            default:
                buildTextBody(builder, body);
                break;
        }
        builder.endObject();
    }

    private void buildTextBody(XContentBuilder builder, AnyValue value) throws IOException {
        builder.field("text");
        buildAnyValue(builder, value);
    }

    private void buildStructuredBody(XContentBuilder builder, AnyValue body) throws IOException {
        builder.field("structured");
        buildAnyValue(builder, body);
    }
}
