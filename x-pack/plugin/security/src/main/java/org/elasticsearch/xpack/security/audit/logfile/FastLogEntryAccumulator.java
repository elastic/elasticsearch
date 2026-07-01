/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.ACTION_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.API_KEY_ID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.API_KEY_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CHANGE_CONFIG_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CLUSTER_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CLUSTER_UUID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CREATE_CONFIG_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.CROSS_CLUSTER_ACCESS_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.DELETE_CONFIG_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EVENT_ACTION_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.EVENT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.HOST_ADDRESS_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.HOST_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.INDICES_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.INVALIDATE_API_KEYS_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.NODE_ID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.NODE_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.OPAQUE_ID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.ORIGIN_ADDRESS_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_DOMAIN_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_REALM_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_AS_DOMAIN_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_AS_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_AS_REALM_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_BY_DOMAIN_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_BY_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_RUN_BY_REALM_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PUT_CONFIG_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REALM_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_BODY_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_ID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.REQUEST_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.RULE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.SERVICE_TOKEN_NAME_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.SERVICE_TOKEN_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.TRACE_ID_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.TRANSPORT_PROFILE_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.URL_PATH_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.URL_QUERY_FIELD_NAME;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.X_FORWARDED_FOR_FIELD_NAME;

/**
 * A write-once accumulator for a single audit log entry that doubles as the log4j2 {@link Message} handed to the audit logger.
 *
 * <p>Acts as a drop in replacement for the {@link org.apache.logging.log4j.message.StringMapMessage} previously used in
 * {@link LoggingAuditTrail}. The log4j class stores its fields in a sorted-by-key structure, which means that every field added during
 * construction pays for a binary search to find its insertion point, and the backing array was grown/copied as fields were added.
 * Profiling revealed this to be significant overhead for virtually no return: each field is written exactly once and never queried by key
 * except when the log message is finally constructed. The introduction of this O(1) insertion data structure improves performance
 * considerably.
 *
 * <p>Here, we leverage the fact that audit logging fields are fixed and known ahead of time. This structure is captured as a static
 * {@link #AUDIT_FORMAT}: an ordered array of {@link LogField}s plus a name&rarr;slot index. Each {@link #with(String, Object)} is then a
 * single map lookup and an array store — O(1), no sorting and no resizing — and field values are held <em>as references</em> rather than
 * being eagerly rendered to JSON. Delaying serialization allows for future performance gains when paired with an async logger, i.e. it
 * allows for all JSON encoding (now relocated within {@link #formatTo}) to be done off the transport thread. The serialization code was
 * previously present in {@link LoggingAuditTrail} and tightly coupled with the transport thread.
 */
final class FastLogEntryAccumulator implements Message, StringBuilderFormattable {

    private static final Object[] EMPTY_PARAMS = new Object[0];

    /**
     * How a field's value is rendered into the JSON line.
     */
    enum FieldType {
        /** A scalar value rendered as a JSON-escaped, double-quoted string. */
        STRING,
        /** An {@code Object[]} reference rendered as a JSON array of double-quoted, JSON-escaped strings (nulls skipped). */
        STRING_ARRAY,
        /** A value that is already a well-formed JSON fragment (object or array) and is emitted verbatim. */
        RAW
    }

    /**
     * A single field of the audit line: its JSON key and how its value is encoded.
     * {@code prefix} is the {@code , "name":} separator that precedes every field value in the JSON output; used during serialization.
     * Precomputing it for performance reasons.
     */
    record LogField(String name, FieldType type, String prefix) {
        LogField(String name, FieldType type) {
            this(name, type, ", \"" + name + "\":");
        }
    }

    /**
     * The ordered set of fields that may appear in an audit line. The order matches the order in which the fields appear in the
     * previous {@code appender.audit_rolling.layout.pattern}, so the rendered line is byte-for-byte compatible with the older layout.
     */
    record LogFormat(LogField[] fields, Map<String, Integer> indexByName) {
        static LogFormat of(LogField... fields) {
            final Map<String, Integer> indexByName = Maps.newHashMapWithExpectedSize(fields.length);
            for (int i = 0; i < fields.length; i++) {
                indexByName.put(fields[i].name(), i);
            }
            return new LogFormat(fields, indexByName);
        }

        int indexOf(String name) {
            final Integer index = indexByName.get(name);
            return index == null ? -1 : index;
        }
    }

    private static final LogFormat AUDIT_FORMAT = LogFormat.of(
        new LogField(CLUSTER_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(CLUSTER_UUID_FIELD_NAME, FieldType.STRING),
        new LogField(NODE_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(NODE_ID_FIELD_NAME, FieldType.STRING),
        new LogField(HOST_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(HOST_ADDRESS_FIELD_NAME, FieldType.STRING),
        new LogField(EVENT_TYPE_FIELD_NAME, FieldType.STRING),
        new LogField(EVENT_ACTION_FIELD_NAME, FieldType.STRING),
        new LogField(AUTHENTICATION_TYPE_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_BY_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_AS_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_REALM_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_DOMAIN_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_BY_REALM_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_BY_DOMAIN_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_AS_REALM_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_RUN_AS_DOMAIN_FIELD_NAME, FieldType.STRING),
        new LogField(PRINCIPAL_ROLES_FIELD_NAME, FieldType.STRING_ARRAY),
        new LogField(API_KEY_ID_FIELD_NAME, FieldType.STRING),
        new LogField(API_KEY_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(SERVICE_TOKEN_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(SERVICE_TOKEN_TYPE_FIELD_NAME, FieldType.STRING),
        new LogField(CROSS_CLUSTER_ACCESS_FIELD_NAME, FieldType.RAW),
        new LogField(ORIGIN_TYPE_FIELD_NAME, FieldType.STRING),
        new LogField(ORIGIN_ADDRESS_FIELD_NAME, FieldType.STRING),
        new LogField(REALM_FIELD_NAME, FieldType.STRING),
        // "realm_domain" appears in the layout pattern but is never populated by the audit trail (there is no field-name constant for
        // it); it is kept here so the in-code format stays a faithful mirror of the pattern.
        new LogField("realm_domain", FieldType.STRING),
        new LogField(URL_PATH_FIELD_NAME, FieldType.STRING),
        new LogField(URL_QUERY_FIELD_NAME, FieldType.STRING),
        new LogField(REQUEST_METHOD_FIELD_NAME, FieldType.STRING),
        new LogField(REQUEST_BODY_FIELD_NAME, FieldType.STRING),
        new LogField(REQUEST_ID_FIELD_NAME, FieldType.STRING),
        new LogField(ACTION_FIELD_NAME, FieldType.STRING),
        new LogField(REQUEST_NAME_FIELD_NAME, FieldType.STRING),
        new LogField(INDICES_FIELD_NAME, FieldType.STRING_ARRAY),
        new LogField(OPAQUE_ID_FIELD_NAME, FieldType.STRING),
        new LogField(TRACE_ID_FIELD_NAME, FieldType.STRING),
        new LogField(X_FORWARDED_FOR_FIELD_NAME, FieldType.STRING),
        new LogField(TRANSPORT_PROFILE_FIELD_NAME, FieldType.STRING),
        new LogField(RULE_FIELD_NAME, FieldType.STRING),
        new LogField(PUT_CONFIG_FIELD_NAME, FieldType.RAW),
        new LogField(DELETE_CONFIG_FIELD_NAME, FieldType.RAW),
        new LogField(CHANGE_CONFIG_FIELD_NAME, FieldType.RAW),
        new LogField(CREATE_CONFIG_FIELD_NAME, FieldType.RAW),
        new LogField(INVALIDATE_API_KEYS_FIELD_NAME, FieldType.RAW)
    );

    private final LogFormat format;
    private final Object[] values;
    private volatile String rendered;

    /**
     * Creates an audit entry seeded with the always-present common fields (node/cluster identity, default origin). Common fields with
     * a {@code null} value (used to mark a field as deliberately absent) are skipped.
     */
    FastLogEntryAccumulator(Map<String, String> commonFields) {
        this(AUDIT_FORMAT, commonFields);
    }

    FastLogEntryAccumulator(LogFormat format, Map<String, String> commonFields) {
        this.format = format;
        this.values = new Object[format.fields().length];
        for (Map.Entry<String, String> commonField : commonFields.entrySet()) {
            with(commonField.getKey(), commonField.getValue());
        }
    }

    FastLogEntryAccumulator with(String name, Object value) {
        if (value == null) {
            return this;
        }
        final int index = format.indexOf(name);
        if (index >= 0) {
            values[index] = value;
        }
        return this;
    }

    Object get(String name) {
        final int index = format.indexOf(name);
        return index < 0 ? null : values[index];
    }

    void remove(String name) {
        final int index = format.indexOf(name);
        if (index >= 0) {
            values[index] = null;
        }
    }

    /**
     * Returns the set fields as a key-sorted map. Used to serialize a nested audit object (the {@code cross_cluster_access}
     * authentication) as a JSON object; the previous implementation derived this nested object from the sorted backing map of a
     * {@link org.apache.logging.log4j.message.StringMapMessage}, so the sorted order is preserved here for compatibility.
     */
    SortedMap<String, Object> getData() {
        final SortedMap<String, Object> data = new TreeMap<>();
        for (int i = 0; i < format.fields().length; i++) {
            if (values[i] != null) {
                data.put(format.fields()[i].name(), values[i]);
            }
        }
        return data;
    }

    @Override
    public void formatTo(StringBuilder buffer) {
        String rendered = this.rendered;
        if (rendered == null) {
            final StringBuilder sb = new StringBuilder(1024); // derived via profiling to minimize resizing for a typical accessGranted.
            final JsonStringEncoder jsonStringEncoder = JsonStringEncoder.getInstance();
            final LogField[] fields = format.fields();
            for (int i = 0; i < fields.length; i++) {
                final Object value = values[i];
                if (value == null) {
                    continue;
                }
                final LogField field = fields[i];
                switch (field.type()) {
                    case STRING -> {
                        final String string = value.toString();
                        // an empty value was suppressed by %varsNotEmpty in the old layout, so skip it here too
                        if (string.isEmpty()) {
                            continue;
                        }
                        sb.append(field.prefix());
                        sb.append('"');
                        if (isAsciiSafe(string)) {
                            sb.append(string);
                        } else {
                            jsonStringEncoder.quoteAsString(string, sb);
                        }
                        sb.append('"');
                    }
                    case STRING_ARRAY -> {
                        sb.append(field.prefix());
                        appendQuotedJsonArray(sb, (Object[]) value, jsonStringEncoder);
                    }
                    case RAW -> {
                        sb.append(field.prefix());
                        sb.append(value);
                    }
                }
            }
            rendered = sb.toString();
            this.rendered = rendered;
        }
        buffer.append(rendered);
    }

    private static void appendQuotedJsonArray(StringBuilder sb, Object[] values, JsonStringEncoder jsonStringEncoder) {
        sb.append('[');
        final int start = sb.length();
        for (final Object value : values) {
            if (value != null) {
                if (sb.length() > start) {
                    sb.append(',');
                }
                final String string = value.toString();
                sb.append('"');
                if (isAsciiSafe(string)) {
                    sb.append(string);
                } else {
                    jsonStringEncoder.quoteAsString(string, sb);
                }
                sb.append('"');
            }
        }
        sb.append(']');
    }

    /**
     * Returns {@code true} if every character in {@code s} can be written into a JSON string as-is, with no escaping needed.
     * Specifically: all chars are in [0x20, 0x7e] and neither {@code "} nor {@code \}.
     */
    private static boolean isAsciiSafe(String s) {
        for (int i = 0, len = s.length(); i < len; i++) {
            final char c = s.charAt(i);
            if (c < 0x20 || c == '"' || c == '\\' || c > 0x7e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getFormattedMessage() {
        if (rendered != null) {
            return rendered;
        }
        final StringBuilder sb = new StringBuilder(1024);
        formatTo(sb);
        return sb.toString();
    }

    @Override
    public String getFormat() {
        return "";
    }

    @Override
    public Object[] getParameters() {
        return EMPTY_PARAMS;
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }
}
