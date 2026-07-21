/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.message.StringMapMessage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.datatree.DataArray;
import org.elasticsearch.datatree.DataBoolean;
import org.elasticsearch.datatree.DataDecimal;
import org.elasticsearch.datatree.DataDouble;
import org.elasticsearch.datatree.DataInteger;
import org.elasticsearch.datatree.DataLong;
import org.elasticsearch.datatree.DataNull;
import org.elasticsearch.datatree.DataObject;
import org.elasticsearch.datatree.DataString;
import org.elasticsearch.datatree.DataValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.audit.AuditLogCustomizer;
import org.elasticsearch.xpack.core.security.audit.AuditLogMessageConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Renders an audit {@link DataObject} as the flat, {@code String}-valued {@link StringMapMessage} historically produced by
 * {@link LoggingAuditTrail}.
 * <p>
 * This is the default, byte-for-byte back-compatible rendering that {@link LoggingAuditTrail} applies whenever an
 * {@link AuditLogCustomizer} does not supply its own converter (see {@link AuditLogCustomizer#messageConverter()}): every field is
 * emitted under its name with its string value, matching the flat map the audit trail has always logged. Fields whose value is
 * {@link DataNull} are dropped.
 * <p>
 * This flattening is the single terminal point at which the structured entry is collapsed for the pattern layout: scalar fields
 * become their string form, while a field holding a nested {@link DataObject} or {@link DataArray} (for example a security config
 * change body) is serialized to a compact JSON string, preserving field order. The audit trail's pattern layout substitutes those
 * pre-rendered JSON strings verbatim.
 */
public final class StringMapMessageConverter implements AuditLogMessageConverter {

    /**
     * Shared, stateless instance of the default converter.
     */
    public static final StringMapMessageConverter INSTANCE = new StringMapMessageConverter();

    private StringMapMessageConverter() {}

    /**
     * Converts the given entry into a flat, {@code String}-valued {@link StringMapMessage}.
     * <p>
     * {@link DataNull} fields are dropped rather than emitted as null-valued entries.
     *
     * @param entry the audit entry to render
     * @return the log4j message
     * @throws UncheckedIOException if a nested object or array field cannot be serialized to JSON
     */
    @Override
    public StringMapMessage convert(DataObject entry) {
        final StringMapMessage message = new StringMapMessage(entry.view().size());
        entry.forEach((name, value) -> {
            if (value != DataNull.INSTANCE) {
                message.with(name, flatString(name, value));
            }
        });
        return message;
    }

    private static String flatString(String name, DataValue value) {
        return switch (value) {
            case DataNull ignored -> null;
            case DataString dataString -> dataString.value();
            case DataBoolean dataBoolean -> Boolean.toString(dataBoolean.value());
            case DataLong dataLong -> Long.toString(dataLong.value());
            case DataDouble dataDouble -> Double.toString(dataDouble.value());
            case DataInteger dataInteger -> dataInteger.value().toString();
            case DataDecimal dataDecimal -> dataDecimal.value().toString();
            case DataObject object -> toJson(name, object);
            case DataArray array -> isAllStrings(array) ? stringArrayToJson(array) : toJson(name, array);
        };
    }

    private static boolean isAllStrings(DataArray array) {
        for (DataValue element : array) {
            if (element instanceof DataString == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Encodes an array of {@link DataString}s as a compact JSON array, quoting and escaping each element directly into a
     * {@link StringBuilder}. This mirrors the audit trail's long-standing array rendering and avoids the per-field
     * {@link XContentBuilder} allocation (and its self-reference bookkeeping) that the general {@link #toJson} path incurs.
     */
    private static String stringArrayToJson(DataArray array) {
        final StringBuilder builder = new StringBuilder();
        final JsonStringEncoder encoder = JsonStringEncoder.getInstance();
        builder.append('[');
        for (DataValue element : array) {
            if (builder.length() > 1) {
                builder.append(',');
            }
            builder.append('"');
            encoder.quoteAsString(((DataString) element).value(), builder);
            builder.append('"');
        }
        builder.append(']');
        return builder.toString();
    }

    /**
     * Serializes a nested value to a compact JSON string, preserving field and element order. The {@link DataValue} tree is
     * written straight into the {@link XContentBuilder}, avoiding an intermediate generic Java tree and the builder's cyclic
     * reference checks (the model is acyclic by construction).
     */
    private static String toJson(String name, DataValue value) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            writeValue(builder, value);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to render audit field [" + name + "] as JSON", e);
        }
    }

    private static void writeValue(XContentBuilder builder, DataValue value) throws IOException {
        switch (value) {
            case DataNull ignored -> builder.nullValue();
            case DataString dataString -> builder.value(dataString.value());
            case DataBoolean dataBoolean -> builder.value(dataBoolean.value());
            case DataLong dataLong -> builder.value(dataLong.value());
            case DataDouble dataDouble -> builder.value(dataDouble.value());
            case DataInteger dataInteger -> builder.value(dataInteger.value());
            case DataDecimal dataDecimal -> builder.value(dataDecimal.value());
            case DataObject object -> {
                builder.startObject();
                for (Map.Entry<String, DataValue> field : object.view().entrySet()) {
                    builder.field(field.getKey());
                    writeValue(builder, field.getValue());
                }
                builder.endObject();
            }
            case DataArray array -> {
                builder.startArray();
                for (DataValue element : array) {
                    writeValue(builder, element);
                }
                builder.endArray();
            }
        }
    }
}
