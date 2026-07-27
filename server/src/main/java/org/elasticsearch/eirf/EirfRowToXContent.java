/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.KeyValueReader;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;

/**
 * Converts a {@link SourceRow} to XContent output via {@link XContentBuilder}.
 */
public final class EirfRowToXContent {

    private EirfRowToXContent() {}

    /**
     * Writes a single row as a nested JSON object to the given builder.
     * The builder should not have startObject called yet - this method handles it.
     */
    public static void writeRow(SourceRow row, SourceSchema schema, XContentBuilder builder) throws IOException {
        // Walk the schema tree depth-first so children of the same non-leaf are emitted contiguously, even when
        // heterogeneous documents caused their leaves to be interleaved in schema leaf order.
        EirfRowXContentParser.SchemaNode root = EirfRowXContentParser.buildSchemaTree(schema);
        writeRowFromSchema(row, root, builder);
    }

    public static void writeRowFromSchema(SourceRow row, EirfRowXContentParser.SchemaNode schemaTree, XContentBuilder builder)
        throws IOException {
        builder.startObject();
        writeChildren(schemaTree, row, builder);
        builder.endObject();
    }

    private static void writeChildren(EirfRowXContentParser.SchemaNode node, SourceRow row, XContentBuilder builder) throws IOException {
        for (EirfRowXContentParser.SchemaNode child : node.children()) {
            if (child.isLeaf()) {
                int leafIdx = child.leafColumnIndex();
                if (row.isAbsent(leafIdx)) {
                    continue;
                }
                writeLeafValue(row, leafIdx, row.getTypeByte(leafIdx), child.name(), builder);
            } else if (isNotEmpty(child, row)) {
                builder.field(child.name());
                builder.startObject();
                writeChildren(child, row, builder);
                builder.endObject();
            }
        }
    }

    private static boolean isNotEmpty(EirfRowXContentParser.SchemaNode node, SourceRow row) {
        for (EirfRowXContentParser.SchemaNode child : node.children()) {
            if (child.isLeaf()) {
                int leafIdx = child.leafColumnIndex();
                if (row.isAbsent(leafIdx) == false) {
                    return true;
                }
            } else if (isNotEmpty(child, row)) {
                return true;
            }
        }
        return false;
    }

    private static void writeLeafValue(SourceRow row, int leafIdx, byte type, String leafName, XContentBuilder builder) throws IOException {
        switch (type) {
            case SourceValueType.INT -> builder.field(leafName, row.getIntValue(leafIdx));
            // Emit as double so the textual form preserves enough precision for downstream double re-parsers
            // (e.g. scaled_float); the value still round-trips bit-for-bit because (double)(float)val == val
            // holds for any value stored as FLOAT.
            case SourceValueType.FLOAT -> builder.field(leafName, (double) row.getFloatValue(leafIdx));
            case SourceValueType.LONG -> builder.field(leafName, row.getLongValue(leafIdx));
            case SourceValueType.DOUBLE -> builder.field(leafName, row.getDoubleValue(leafIdx));
            case SourceValueType.STRING -> {
                builder.field(leafName);
                row.getStringValue(leafIdx).toXContent(builder, null);
            }
            case SourceValueType.NULL -> builder.nullField(leafName);
            case SourceValueType.TRUE -> builder.field(leafName, true);
            case SourceValueType.FALSE -> builder.field(leafName, false);
            case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> {
                builder.field(leafName);
                writeArray(row.getArrayValue(leafIdx), builder);
            }
            case SourceValueType.KEY_VALUE -> {
                builder.field(leafName);
                writeKeyValue(row.getKeyValue(leafIdx), builder);
            }
            case SourceValueType.BINARY -> {
                BytesRef binary = row.getBinaryValue(leafIdx);
                builder.field(leafName).value(binary.bytes, binary.offset, binary.length);
            }
            default -> throw new IllegalArgumentException("unsupported type [" + type + "]");
        }
    }

    static void writeArray(ArrayReader reader, XContentBuilder builder) throws IOException {
        builder.startArray();
        while (reader.next()) {
            writeElementValue(reader, builder);
        }
        builder.endArray();
    }

    private static void writeElementValue(ArrayReader array, XContentBuilder builder) throws IOException {
        switch (array.type()) {
            case SourceValueType.INT -> builder.value(array.intValue());
            case SourceValueType.FLOAT -> builder.value((double) array.floatValue());
            case SourceValueType.LONG -> builder.value(array.longValue());
            case SourceValueType.DOUBLE -> builder.value(array.doubleValue());
            case SourceValueType.STRING -> {
                XContentString.UTF8Bytes bytes = array.textValue().bytes();
                builder.utf8Value(bytes.bytes(), bytes.offset(), bytes.length());
            }
            case SourceValueType.TRUE -> builder.value(true);
            case SourceValueType.FALSE -> builder.value(false);
            case SourceValueType.NULL -> builder.nullValue();
            case SourceValueType.KEY_VALUE -> writeKeyValue(array.nestedKeyValue(), builder);
            case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> writeArray(array.nestedArray(), builder);
            default -> throw new IllegalArgumentException("unsupported type [" + array.type() + "]");
        }
    }

    static void writeKeyValue(KeyValueReader kv, XContentBuilder builder) throws IOException {
        builder.startObject();
        while (kv.next()) {
            builder.field(kv.key());
            writeKvValue(kv, builder);
        }
        builder.endObject();
    }

    private static void writeKvValue(KeyValueReader kv, XContentBuilder builder) throws IOException {
        switch (kv.type()) {
            case SourceValueType.INT -> builder.value(kv.intValue());
            case SourceValueType.FLOAT -> builder.value((double) kv.floatValue());
            case SourceValueType.LONG -> builder.value(kv.longValue());
            case SourceValueType.DOUBLE -> builder.value(kv.doubleValue());
            case SourceValueType.STRING -> builder.value(kv.stringValue());
            case SourceValueType.TRUE -> builder.value(true);
            case SourceValueType.FALSE -> builder.value(false);
            case SourceValueType.NULL -> builder.nullValue();
            case SourceValueType.KEY_VALUE -> writeKeyValue(kv.nestedKeyValue(), builder);
            case SourceValueType.UNION_ARRAY, SourceValueType.FIXED_ARRAY -> writeArray(kv.nestedArray(), builder);
            default -> throw new IllegalArgumentException("unsupported type [" + kv.type() + "]");
        }
    }
}
