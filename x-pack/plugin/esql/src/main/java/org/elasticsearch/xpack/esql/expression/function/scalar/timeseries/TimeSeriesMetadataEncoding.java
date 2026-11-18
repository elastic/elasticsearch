/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The bytes of a typed label container, {@link DataType#TIME_SERIES_METADATA_DATA_TYPE}: the labels of a series in name order, each
 * its name and its values, every length a variable-length int.
 * <pre>
 * label := vint(name length) name vint(value count) (vint(value length) value)+
 * </pre>
 * Names are unique and non-empty, values non-empty and kept in source order. Names compare as unsigned bytes, which is code point
 * order. Equal label sets get equal bytes, so a label container keys groups and joins as it is, and a page holds it dictionary
 * encoded: each distinct label set once, and an ordinal per row (see {@link TimeSeriesMetadataConverter}). Every label container
 * function streams over these bytes without parsing them: a read stops at its label, and a rewrite copies the labels it keeps and
 * writes a set label at its position, which keeps the result in name order. Only {@link #toJson} turns a label container into the
 * JSON object users see.
 */
public final class TimeSeriesMetadataEncoding {
    /** The label container of a series without labels. */
    public static final BytesRef EMPTY = new BytesRef();

    private TimeSeriesMetadataEncoding() {}

    /** Writes a label: {@code name} holding {@code values}, neither of them empty. */
    static void writeLabel(BreakingBytesRefBuilder out, BytesRef name, List<BytesRef> values) {
        writeBytes(out, name);
        writeVInt(out, values.size());
        for (BytesRef value : values) {
            writeBytes(out, value);
        }
    }

    private static void writeBytes(BreakingBytesRefBuilder out, BytesRef bytes) {
        writeVInt(out, bytes.length);
        out.append(bytes);
    }

    private static void writeVInt(BreakingBytesRefBuilder out, int value) {
        while ((value & ~0x7F) != 0) {
            out.append((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.append((byte) value);
    }

    /**
     * Walks the labels of one label container in name order without copying them. The name and the values it hands out are views
     * of the label container, valid until it moves on.
     */
    static final class Reader {
        private final BytesRef name = new BytesRef();
        private byte[] bytes;
        private int position;
        private int end;
        private int labelStart;
        private int valueCount;
        private int valuesStart;

        Reader reset(BytesRef encoded) {
            bytes = encoded.bytes;
            position = encoded.offset;
            end = encoded.offset + encoded.length;
            return this;
        }

        /** Moves to the next label: false past the last one. */
        boolean next() {
            if (position >= end) {
                return false;
            }
            labelStart = position;
            int nameLength = readVInt();
            if (nameLength <= 0) {
                throw invalid();
            }
            name.bytes = bytes;
            name.offset = position;
            name.length = nameLength;
            skip(nameLength);
            valueCount = readVInt();
            if (valueCount <= 0) {
                throw invalid();
            }
            valuesStart = position;
            for (int i = 0; i < valueCount; i++) {
                skip(readVInt());
            }
            return true;
        }

        BytesRef name() {
            return name;
        }

        int valueCount() {
            return valueCount;
        }

        /** The values of the label, in order, each a view written to its own {@link BytesRef}. */
        List<BytesRef> values() {
            List<BytesRef> values = new ArrayList<>(valueCount);
            int labelEnd = position;
            position = valuesStart;
            for (int i = 0; i < valueCount; i++) {
                int length = readVInt();
                values.add(new BytesRef(bytes, position, length));
                position += length;
            }
            position = labelEnd;
            return values;
        }

        /** Appends the values of the label: one as a value, several as one position. */
        void appendValues(BytesRefBlock.Builder result) {
            int labelEnd = position;
            position = valuesStart;
            BytesRef value = new BytesRef();
            value.bytes = bytes;
            if (valueCount > 1) {
                result.beginPositionEntry();
            }
            for (int i = 0; i < valueCount; i++) {
                value.length = readVInt();
                value.offset = position;
                position += value.length;
                result.appendBytesRef(value);
            }
            if (valueCount > 1) {
                result.endPositionEntry();
            }
            position = labelEnd;
        }

        /** Copies the label, its name and values, as it is. */
        void copyLabel(BreakingBytesRefBuilder out) {
            out.append(bytes, labelStart, position - labelStart);
        }

        private int readVInt() {
            int value = 0;
            for (int shift = 0; shift < 32; shift += 7) {
                if (position >= end) {
                    throw invalid();
                }
                byte b = bytes[position++];
                value |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
            }
            throw invalid();
        }

        private void skip(int length) {
            if (length < 0 || length > end - position) {
                throw invalid();
            }
            position += length;
        }

        private static IllegalArgumentException invalid() {
            return new IllegalArgumentException("Invalid label container");
        }
    }

    /**
     * The label container of the JSON object {@code source} of a series' dimensions, as the source holds them, written to
     * {@code out}: labels named as PromQL names them, each holding its non-empty values in source order.
     * <p>
     * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
     * container. Prometheus has no empty label, so data written by it never holds such a pair.
     */
    static BytesRef fromSource(BytesRef source, BreakingBytesRefBuilder out) {
        out.clear();
        try {
            for (var label : readSource(source).entrySet()) {
                writeLabel(out, label.getKey(), label.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid label container", e);
        }
        return out.bytesRefView();
    }

    private static SortedMap<BytesRef, List<BytesRef>> readSource(BytesRef source) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                source.bytes,
                source.offset,
                source.length
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(parser.getTokenLocation(), "time-series source must be an object");
            }
            var labels = new TreeMap<BytesRef, List<BytesRef>>();
            readSourceObject(parser, "", labels);
            if (parser.nextToken() != null) {
                throw new XContentParseException(parser.getTokenLocation(), "unexpected content after time-series source");
            }
            return labels;
        }
    }

    private static void readSourceObject(XContentParser parser, String prefix, Map<BytesRef, List<BytesRef>> labels) throws IOException {
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            String field = prefix + parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                readSourceObject(parser, field + ".", labels);
                continue;
            }
            var values = new ArrayList<BytesRef>();
            if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    readSourceValue(parser, token, field, values);
                }
            } else {
                readSourceValue(parser, token, field, values);
            }
            String label = PromqlLabels.labelName(field);
            if (label.isEmpty()) {
                throw new XContentParseException(parser.getTokenLocation(), "time-series label names must be non-empty");
            }
            if (labels.containsKey(new BytesRef(label))) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "multiple dimension fields resolve to time-series label [" + label + "]"
                );
            }
            if (values.isEmpty() == false) {
                labels.put(new BytesRef(label), values);
            }
        }
    }

    /** Scalars keep their source text; null and empty values are left out. */
    private static void readSourceValue(XContentParser parser, XContentParser.Token token, String field, List<BytesRef> values)
        throws IOException {
        if (token.isValue()) {
            String value = parser.text();
            if (value.isEmpty() == false) {
                values.add(new BytesRef(value));
            }
        } else if (token != XContentParser.Token.VALUE_NULL) {
            throw new XContentParseException(parser.getTokenLocation(), "time-series dimension [" + field + "] must contain values");
        }
    }

    /** {@link #fromSource(BytesRef, BreakingBytesRefBuilder)} outside of a driver: for literals and test values. */
    public static BytesRef fromSource(BytesRef source) {
        try (BreakingBytesRefBuilder out = new BreakingBytesRefBuilder(new NoopCircuitBreaker("tsmd"), "tsmd")) {
            return BytesRef.deepCopyOf(fromSource(source, out));
        }
    }

    /** The label container holding {@code labels}, a label without values left out: for literals and test values. */
    public static BytesRef encode(Map<String, List<String>> labels) {
        var sorted = new TreeMap<BytesRef, List<BytesRef>>();
        labels.forEach((name, values) -> {
            if (values.isEmpty() == false) {
                sorted.put(new BytesRef(name), values.stream().map(BytesRef::new).toList());
            }
        });
        try (BreakingBytesRefBuilder out = new BreakingBytesRefBuilder(new NoopCircuitBreaker("tsmd"), "tsmd")) {
            for (var label : sorted.entrySet()) {
                writeLabel(out, label.getKey(), label.getValue());
            }
            return BytesRef.deepCopyOf(out.bytesRefView());
        }
    }

    /** Writes {@code encoded} as the JSON object users see: labels in name order, one value as a string, several as an array. */
    static void toJson(BytesRef encoded, XContentBuilder builder) throws IOException {
        builder.startObject();
        Reader reader = new Reader().reset(encoded);
        while (reader.next()) {
            builder.field(reader.name().utf8ToString());
            List<BytesRef> values = reader.values();
            if (values.size() > 1) {
                builder.startArray();
            }
            for (BytesRef value : values) {
                builder.utf8Value(value.bytes, value.offset, value.length);
            }
            if (values.size() > 1) {
                builder.endArray();
            }
        }
        builder.endObject();
    }

    /** {@code encoded} as the JSON object users see, see {@link #toJson(BytesRef, XContentBuilder)}. */
    public static BytesRef toJson(BytesRef encoded) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            toJson(encoded, builder);
            return BytesReference.bytes(builder).toBytesRef();
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid label container", e);
        }
    }
}
