/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes a list of {@link IndexRequest}s into a binary columnar {@link DocumentBatch}.
 * This encoder is mapping-unaware — it operates purely on JSON structure:
 * <ul>
 *   <li>JSON objects are flattened to dot-path columns</li>
 *   <li>JSON scalars become typed column values</li>
 *   <li>JSON arrays are stored as raw XContent bytes (BINARY columns)</li>
 * </ul>
 */
public class DocumentBatchEncoder {

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private DocumentBatchEncoder() {}

    /**
     * Encode a list of IndexRequests into a binary columnar DocumentBatch.
     *
     * @param requests     the index requests to encode
     * @return a DocumentBatch backed by a contiguous byte array
     */
    public static DocumentBatch encode(List<IndexRequest> requests) throws IOException {
        int docCount = requests.size();

        // Phase 1: Extract all field values from each document
        // Map from field path to list of per-doc values (null if absent)
        Map<String, ColumnBuilder> columns = new LinkedHashMap<>();

        for (int docIdx = 0; docIdx < docCount; docIdx++) {
            IndexRequest request = requests.get(docIdx);
            BytesReference source = request.source();
            XContentType xContentType = request.getContentType();
            if (xContentType == null) {
                xContentType = XContentType.JSON;
            }

            try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, source.streamInput())) {
                parser.nextToken(); // START_OBJECT
                flattenObject(parser, "", docIdx, docCount, columns, xContentType);
            }

            // Ensure all columns known so far have an entry for this doc
            for (ColumnBuilder col : columns.values()) {
                col.ensureSize(docIdx + 1);
            }
        }

        // Phase 2: Serialize to binary format
        return serialize(requests, columns, docCount);
    }


    private static void flattenObject(
        XContentParser parser,
        String prefix,
        int docIdx,
        int docCount,
        Map<String, ColumnBuilder> columns,
        XContentType xContentType
    ) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            token = parser.nextToken(); // value token

            switch (token) {
                case START_OBJECT -> {
                    // Recurse into nested object, flatten to dot-paths
                    flattenObject(parser, fieldPath, docIdx, docCount, columns, xContentType);
                }
                case START_ARRAY -> {
                    // Store entire array as raw XContent bytes
                    BytesReference rawBytes = captureRawXContent(parser, xContentType);
                    ColumnBuilder col = columns.computeIfAbsent(fieldPath, k -> new ColumnBuilder(docCount));
                    col.setBinary(docIdx, rawBytes);
                }
                case VALUE_STRING -> {
                    ColumnBuilder col = columns.computeIfAbsent(fieldPath, k -> new ColumnBuilder(docCount));
                    col.setString(docIdx, parser.text());
                }
                case VALUE_NUMBER -> {
                    ColumnBuilder col = columns.computeIfAbsent(fieldPath, k -> new ColumnBuilder(docCount));
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT -> col.setInt(docIdx, parser.intValue());
                        case LONG -> col.setLong(docIdx, parser.longValue());
                        case FLOAT -> col.setFloat(docIdx, parser.floatValue());
                        case DOUBLE -> col.setDouble(docIdx, parser.doubleValue());
                        default ->
                            // BIG_INTEGER, BIG_DECIMAL -> store as string
                            col.setString(docIdx, parser.text());
                    }
                }
                case VALUE_BOOLEAN -> {
                    ColumnBuilder col = columns.computeIfAbsent(fieldPath, k -> new ColumnBuilder(docCount));
                    col.setBoolean(docIdx, parser.booleanValue());
                }
                case VALUE_NULL -> {
                    ColumnBuilder col = columns.computeIfAbsent(fieldPath, k -> new ColumnBuilder(docCount));
                    col.setNull(docIdx);
                }
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
        }
    }

    /**
     * Capture the current array (including nested arrays/objects) as raw XContent bytes.
     * Parser is positioned at START_ARRAY and will be positioned after END_ARRAY on return.
     */
    private static BytesReference captureRawXContent(XContentParser parser, XContentType xContentType) throws IOException {
        // Use a builder to serialize the array
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static DocumentBatch serialize(
        List<IndexRequest> requests,
        Map<String, ColumnBuilder> columns,
        int docCount
    ) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4096);

        // Header: magic(4) + version(4) + docCount(4) + columnCount(4) = 16 bytes
        writeInt(out, DocumentBatch.MAGIC);
        writeInt(out, DocumentBatch.VERSION);
        writeInt(out, docCount);
        writeInt(out, columns.size());

        // Per-document metadata
        for (int i = 0; i < docCount; i++) {
            IndexRequest req = requests.get(i);

            // id
            String id = req.id();
            if (id != null) {
                byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
                writeInt(out, idBytes.length);
                out.write(idBytes);
            } else {
                writeInt(out, 0);
            }

            // routing
            String routing = req.routing();
            if (routing != null) {
                byte[] routingBytes = routing.getBytes(StandardCharsets.UTF_8);
                writeInt(out, routingBytes.length);
                out.write(routingBytes);
            } else {
                writeInt(out, 0);
            }

            // XContentType
            XContentType xct = req.getContentType();
            out.write(xct != null ? xct.ordinal() : XContentType.JSON.ordinal());

            // autoGeneratedTimestamp
            writeLong(out, req.getAutoGeneratedTimestamp());

            // isRetry
            out.write(req.isRetry() ? 1 : 0);
        }

        // Serialize column data into a separate buffer to compute offsets
        List<String> fieldPaths = new ArrayList<>(columns.keySet());
        List<ColumnBuilder> builders = new ArrayList<>(columns.values());
        byte[][] columnDataArrays = new byte[columns.size()][];

        for (int j = 0; j < builders.size(); j++) {
            columnDataArrays[j] = builders.get(j).serialize(docCount);
        }

        // Column directory
        int colDataOffset = 0;
        for (int j = 0; j < fieldPaths.size(); j++) {
            String path = fieldPaths.get(j);
            byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
            writeInt(out, pathBytes.length);
            out.write(pathBytes);

            out.write(builders.get(j).resolvedType().id());

            writeInt(out, colDataOffset);
            writeInt(out, columnDataArrays[j].length);

            colDataOffset += columnDataArrays[j].length;
        }

        // Column data
        for (byte[] colData : columnDataArrays) {
            out.write(colData);
        }

        return new DocumentBatch(out.toByteArray());
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        byte[] buf = new byte[4];
        INT_HANDLE.set(buf, 0, value);
        out.write(buf, 0, 4);
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        byte[] buf = new byte[8];
        LONG_HANDLE.set(buf, 0, value);
        out.write(buf, 0, 8);
    }

    /**
     * Accumulates values for a single column during encoding. Tracks the current column type
     * and handles type widening when different documents have different types for the same field.
     */
    static class ColumnBuilder {

        private static final VarHandle INT_H = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle LONG_H = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        private ColumnType type;
        private final boolean[] present;
        // Storage for values by type. Only one is active at a time (may change on widening).
        private int[] intValues;
        private long[] longValues;
        private String[] stringValues;
        private boolean[] booleanValues;
        private BytesReference[] binaryValues;

        ColumnBuilder(int docCount) {
            this.present = new boolean[docCount];
            this.type = null; // will be set on first value
        }

        void ensureSize(int size) {
            // present array is pre-allocated; nothing to do for docs that don't have this field
        }

        void setInt(int docIdx, int value) {
            if (type == null) {
                type = ColumnType.INT;
                intValues = new int[present.length];
            } else if (type == ColumnType.LONG) {
                // already widened to LONG
                longValues[docIdx] = value;
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.STRING) {
                stringValues[docIdx] = Integer.toString(value);
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.BINARY) {
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(
                    Integer.toString(value).getBytes(StandardCharsets.UTF_8)
                );
                present[docIdx] = true;
                return;
            } else if (type != ColumnType.INT) {
                widenTo(ColumnType.INT);
            }
            intValues[docIdx] = value;
            present[docIdx] = true;
        }

        void setLong(int docIdx, long value) {
            if (type == null) {
                type = ColumnType.LONG;
                longValues = new long[present.length];
            } else if (type == ColumnType.INT) {
                // Widen INT -> LONG
                widenIntToLong();
            } else if (type == ColumnType.STRING) {
                stringValues[docIdx] = Long.toString(value);
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.BINARY) {
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(Long.toString(value).getBytes(StandardCharsets.UTF_8));
                present[docIdx] = true;
                return;
            } else if (type != ColumnType.LONG) {
                widenTo(ColumnType.LONG);
            }
            longValues[docIdx] = value;
            present[docIdx] = true;
        }

        void setFloat(int docIdx, float value) {
            // Store float as INT (raw bits)
            if (type == null) {
                type = ColumnType.INT;
                intValues = new int[present.length];
            } else if (type == ColumnType.LONG) {
                longValues[docIdx] = Double.doubleToRawLongBits(value);
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.STRING) {
                stringValues[docIdx] = Float.toString(value);
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.BINARY) {
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(
                    Float.toString(value).getBytes(StandardCharsets.UTF_8)
                );
                present[docIdx] = true;
                return;
            } else if (type != ColumnType.INT) {
                widenTo(ColumnType.INT);
            }
            intValues[docIdx] = Float.floatToRawIntBits(value);
            present[docIdx] = true;
        }

        void setDouble(int docIdx, double value) {
            // Store double as LONG (raw bits)
            if (type == null) {
                type = ColumnType.LONG;
                longValues = new long[present.length];
            } else if (type == ColumnType.INT) {
                widenIntToLong();
            } else if (type == ColumnType.STRING) {
                stringValues[docIdx] = Double.toString(value);
                present[docIdx] = true;
                return;
            } else if (type == ColumnType.BINARY) {
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(
                    Double.toString(value).getBytes(StandardCharsets.UTF_8)
                );
                present[docIdx] = true;
                return;
            } else if (type != ColumnType.LONG) {
                widenTo(ColumnType.LONG);
            }
            longValues[docIdx] = Double.doubleToRawLongBits(value);
            present[docIdx] = true;
        }

        void setString(int docIdx, String value) {
            if (type == null) {
                type = ColumnType.STRING;
                stringValues = new String[present.length];
            } else if (type != ColumnType.STRING && type != ColumnType.BINARY) {
                widenToString();
            }
            if (type == ColumnType.BINARY) {
                // Already widened to BINARY, store as binary
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(value.getBytes(StandardCharsets.UTF_8));
                present[docIdx] = true;
                return;
            }
            stringValues[docIdx] = value;
            present[docIdx] = true;
        }

        void setBoolean(int docIdx, boolean value) {
            if (type == null) {
                type = ColumnType.BOOLEAN;
                booleanValues = new boolean[present.length];
            } else if (type == ColumnType.BINARY) {
                binaryValues[docIdx] = new org.elasticsearch.common.bytes.BytesArray(
                    Boolean.toString(value).getBytes(StandardCharsets.UTF_8)
                );
                present[docIdx] = true;
                return;
            } else if (type != ColumnType.BOOLEAN) {
                widenToString();
                stringValues[docIdx] = Boolean.toString(value);
                present[docIdx] = true;
                return;
            }
            booleanValues[docIdx] = value;
            present[docIdx] = true;
        }

        void setBinary(int docIdx, BytesReference value) {
            if (type == null) {
                type = ColumnType.BINARY;
                binaryValues = new BytesReference[present.length];
            } else if (type != ColumnType.BINARY) {
                widenToBinary();
            }
            binaryValues[docIdx] = value;
            present[docIdx] = true;
        }

        void setNull(int docIdx) {
            // Just mark as present (the value itself is "null")
            // Actually, null means the field exists but has null value.
            // We track this via present[docIdx] = true and the value storage being empty/zero.
            // For now, null values are simply not marked as present, matching the "absent" case.
            // This is because JSON null and field-absent are equivalent for most mappers.
        }

        ColumnType resolvedType() {
            return type != null ? type : ColumnType.NULL;
        }

        /**
         * Serialize this column's data to a byte array.
         */
        byte[] serialize(int docCount) {
            ColumnType resolved = resolvedType();
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            switch (resolved) {
                case INT -> {
                    // Null bitmask + fixed-size values
                    writeNullBitmask(out, docCount);
                    for (int i = 0; i < docCount; i++) {
                        byte[] buf = new byte[4];
                        INT_H.set(buf, 0, present[i] ? intValues[i] : 0);
                        out.write(buf, 0, 4);
                    }
                }
                case LONG -> {
                    writeNullBitmask(out, docCount);
                    for (int i = 0; i < docCount; i++) {
                        byte[] buf = new byte[8];
                        LONG_H.set(buf, 0, present[i] ? longValues[i] : 0L);
                        out.write(buf, 0, 8);
                    }
                }
                case BOOLEAN -> {
                    writeNullBitmask(out, docCount);
                    for (int i = 0; i < docCount; i++) {
                        out.write(present[i] && booleanValues[i] ? 1 : 0);
                    }
                }
                case STRING -> {
                    // Variable length: [present(1)][len(4)][bytes] or [0x00]
                    for (int i = 0; i < docCount; i++) {
                        if (present[i] && stringValues[i] != null) {
                            out.write(1);
                            byte[] strBytes = stringValues[i].getBytes(StandardCharsets.UTF_8);
                            byte[] lenBuf = new byte[4];
                            INT_H.set(lenBuf, 0, strBytes.length);
                            out.write(lenBuf, 0, 4);
                            out.write(strBytes, 0, strBytes.length);
                        } else {
                            out.write(0);
                        }
                    }
                }
                case BINARY -> {
                    // Variable length: [present(1)][len(4)][bytes] or [0x00]
                    for (int i = 0; i < docCount; i++) {
                        if (present[i] && binaryValues[i] != null) {
                            out.write(1);
                            byte[] binBytes = BytesReference.toBytes(binaryValues[i]);
                            byte[] lenBuf = new byte[4];
                            INT_H.set(lenBuf, 0, binBytes.length);
                            out.write(lenBuf, 0, 4);
                            out.write(binBytes, 0, binBytes.length);
                        } else {
                            out.write(0);
                        }
                    }
                }
                case NULL -> {
                    // No data needed
                }
            }

            return out.toByteArray();
        }

        private void writeNullBitmask(ByteArrayOutputStream out, int docCount) {
            int bitmaskSize = (docCount + 7) / 8;
            byte[] bitmask = new byte[bitmaskSize];
            for (int i = 0; i < docCount; i++) {
                if (present[i]) {
                    bitmask[i / 8] |= (byte) (1 << (i % 8));
                }
            }
            out.write(bitmask, 0, bitmaskSize);
        }

        private void widenIntToLong() {
            longValues = new long[present.length];
            if (intValues != null) {
                for (int i = 0; i < present.length; i++) {
                    if (present[i]) {
                        longValues[i] = intValues[i];
                    }
                }
            }
            intValues = null;
            type = ColumnType.LONG;
        }

        private void widenToString() {
            stringValues = new String[present.length];
            if (intValues != null) {
                for (int i = 0; i < present.length; i++) {
                    if (present[i]) {
                        stringValues[i] = Integer.toString(intValues[i]);
                    }
                }
                intValues = null;
            } else if (longValues != null) {
                for (int i = 0; i < present.length; i++) {
                    if (present[i]) {
                        stringValues[i] = Long.toString(longValues[i]);
                    }
                }
                longValues = null;
            } else if (booleanValues != null) {
                for (int i = 0; i < present.length; i++) {
                    if (present[i]) {
                        stringValues[i] = Boolean.toString(booleanValues[i]);
                    }
                }
                booleanValues = null;
            }
            type = ColumnType.STRING;
        }

        private void widenToBinary() {
            binaryValues = new BytesReference[present.length];
            if (stringValues != null) {
                for (int i = 0; i < present.length; i++) {
                    if (present[i] && stringValues[i] != null) {
                        binaryValues[i] = new org.elasticsearch.common.bytes.BytesArray(stringValues[i].getBytes(StandardCharsets.UTF_8));
                    }
                }
                stringValues = null;
            }
            // For other types, convert to their string representation first
            type = ColumnType.BINARY;
        }

        private void widenTo(ColumnType target) {
            ColumnType widened = resolvedType().widenWith(target);
            switch (widened) {
                case LONG -> widenIntToLong();
                case STRING -> widenToString();
                case BINARY -> widenToBinary();
                default -> throw new IllegalStateException("Cannot widen " + type + " to " + target);
            }
        }
    }
}
