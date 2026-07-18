/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.message.StringMapMessage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.audit.AuditLogCustomizer;
import org.elasticsearch.xpack.core.security.audit.AuditLogMessageConverter;
import org.elasticsearch.xpack.core.security.audit.data.DataArray;
import org.elasticsearch.xpack.core.security.audit.data.DataBoolean;
import org.elasticsearch.xpack.core.security.audit.data.DataDecimal;
import org.elasticsearch.xpack.core.security.audit.data.DataDouble;
import org.elasticsearch.xpack.core.security.audit.data.DataInteger;
import org.elasticsearch.xpack.core.security.audit.data.DataLong;
import org.elasticsearch.xpack.core.security.audit.data.DataNull;
import org.elasticsearch.xpack.core.security.audit.data.DataObject;
import org.elasticsearch.xpack.core.security.audit.data.DataString;
import org.elasticsearch.xpack.core.security.audit.data.DataValue;
import org.elasticsearch.xpack.core.security.audit.data.DataValues;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Renders an audit {@link DataObject} as the flat, {@code String}-valued {@link StringMapMessage} historically produced by
 * {@link LoggingAuditTrail}.
 * <p>
 * This is the default, byte-for-byte back-compatible rendering that {@link LoggingAuditTrail} applies whenever an
 * {@link AuditLogCustomizer} does not supply its own converter (see {@link AuditLogCustomizer#messageConverter()}): every field is
 * emitted under its name with its string value, matching the flat map the audit trail has always logged. The message is built from
 * a complete map (rather than incremental {@code put} calls) so that fields whose value is {@link DataNull} are preserved as
 * {@code null} entries, matching how the audit trail seeds its common fields.
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
     *
     * @param entry the audit entry to render
     * @return the log4j message
     * @throws UncheckedIOException if a nested object or array field cannot be serialized to JSON
     */
    @Override
    public StringMapMessage convert(DataObject entry) {
        final Map<String, String> data = new HashMap<>(entry.view().size());
        entry.forEach((name, value) -> data.put(name, flatString(name, value)));
        return new StringMapMessage(data);
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
            case DataArray array -> toJson(name, array);
        };
    }

    /**
     * Serializes a nested value to a compact JSON string, preserving field and element order.
     */
    private static String toJson(String name, DataValue value) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.value(DataValues.toJava(value));
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to render audit field [" + name + "] as JSON", e);
        }
    }
}
