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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Converts an EIRF row to XContent output via {@link XContentBuilder}.
 */
public final class EirfRowToXContent {

    private EirfRowToXContent() {}

    /**
     * Writes a single row as a nested JSON object to the given builder.
     * The builder should not have startObject called yet - this method handles it.
     */
    public static void writeRow(EirfRowReader row, EirfSchema schema, XContentBuilder builder) throws IOException {
        // Walk the schema tree depth-first so children of the same non-leaf are emitted contiguously, even when
        // heterogeneous documents caused their leaves to be interleaved in schema leaf order.
        EirfRowXContentParser.SchemaNode root = EirfRowXContentParser.buildSchemaTree(schema);
        builder.startObject();
        writeChildren(root, row, builder);
        builder.endObject();
    }

    private static void writeChildren(EirfRowXContentParser.SchemaNode node, EirfRowReader row, XContentBuilder builder)
        throws IOException {
        for (EirfRowXContentParser.SchemaNode child : node.children()) {
            if (child.isLeaf()) {
                int leafIdx = child.leafColumnIndex();
                if (leafIdx >= row.columnCount() || row.isNull(leafIdx)) {
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

    private static boolean isNotEmpty(EirfRowXContentParser.SchemaNode node, EirfRowReader row) {
        for (EirfRowXContentParser.SchemaNode child : node.children()) {
            if (child.isLeaf()) {
                int leafIdx = child.leafColumnIndex();
                if (leafIdx < row.columnCount() && row.isNull(leafIdx) == false) {
                    return true;
                }
            } else if (isNotEmpty(child, row)) {
                return true;
            }
        }
        return false;
    }

    private static void writeLeafValue(EirfRowReader row, int leafIdx, byte type, String leafName, XContentBuilder builder)
        throws IOException {
        switch (type) {
            case EirfType.INT -> builder.field(leafName, row.getIntValue(leafIdx));
            case EirfType.FLOAT -> builder.field(leafName, row.getFloatValue(leafIdx));
            case EirfType.LONG -> builder.field(leafName, row.getLongValue(leafIdx));
            case EirfType.DOUBLE -> builder.field(leafName, row.getDoubleValue(leafIdx));
            case EirfType.STRING -> {
                builder.field(leafName);
                row.getStringValue(leafIdx).toXContent(builder, null);
            }
            case EirfType.TRUE -> builder.field(leafName, true);
            case EirfType.FALSE -> builder.field(leafName, false);
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                builder.field(leafName);
                writeArray(row.getArrayValue(leafIdx), builder);
            }
            case EirfType.KEY_VALUE -> {
                builder.field(leafName);
                writeKeyValue(row.getKeyValue(leafIdx), builder);
            }
            case EirfType.BINARY -> {
                BytesRef binary = row.getBinaryValue(leafIdx);
                builder.field(leafName).value(binary.bytes, binary.offset, binary.length);
            }
            default -> throw new IllegalArgumentException("unsupported type [" + type + "]");
        }
    }

    static void writeArray(EirfArrayReader reader, XContentBuilder builder) throws IOException {
        builder.startArray();
        while (reader.next()) {
            writeElementValue(reader, builder);
        }
        builder.endArray();
    }

    private static void writeElementValue(EirfArrayReader array, XContentBuilder builder) throws IOException {
        switch (array.type()) {
            case EirfType.INT -> builder.value(array.intValue());
            case EirfType.FLOAT -> builder.value(array.floatValue());
            case EirfType.LONG -> builder.value(array.longValue());
            case EirfType.DOUBLE -> builder.value(array.doubleValue());
            case EirfType.STRING -> builder.value(array.stringValue());
            case EirfType.TRUE -> builder.value(true);
            case EirfType.FALSE -> builder.value(false);
            case EirfType.NULL -> builder.nullValue();
            case EirfType.KEY_VALUE -> writeKeyValue(array.nestedKeyValue(), builder);
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> writeArray(array.nestedArray(), builder);
            default -> throw new IllegalArgumentException("unsupported type [" + array.type() + "]");
        }
    }

    static void writeKeyValue(EirfKeyValueReader kv, XContentBuilder builder) throws IOException {
        builder.startObject();
        while (kv.next()) {
            builder.field(kv.key());
            writeKvValue(kv, builder);
        }
        builder.endObject();
    }

    private static void writeKvValue(EirfKeyValueReader kv, XContentBuilder builder) throws IOException {
        switch (kv.type()) {
            case EirfType.INT -> builder.value(kv.intValue());
            case EirfType.FLOAT -> builder.value(kv.floatValue());
            case EirfType.LONG -> builder.value(kv.longValue());
            case EirfType.DOUBLE -> builder.value(kv.doubleValue());
            case EirfType.STRING -> builder.value(kv.stringValue());
            case EirfType.TRUE -> builder.value(true);
            case EirfType.FALSE -> builder.value(false);
            case EirfType.NULL -> builder.nullValue();
            case EirfType.KEY_VALUE -> writeKeyValue(kv.nestedKeyValue(), builder);
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> writeArray(kv.nestedArray(), builder);
            default -> throw new IllegalArgumentException("unsupported type [" + kv.type() + "]");
        }
    }
}
