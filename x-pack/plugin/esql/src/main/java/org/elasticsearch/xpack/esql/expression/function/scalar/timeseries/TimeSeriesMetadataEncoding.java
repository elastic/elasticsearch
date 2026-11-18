/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The bytes of a typed label container, {@link DataType#TIME_SERIES_METADATA_DATA_TYPE}: the labels of a series in name order,
 * behind a table of where each starts.
 * <pre>
 * container := ""                                             no labels
 *            | vint(count) byte(width) offset{count - 1} label{count}
 * offset    := width bytes, little endian: where a label after the first starts, counted from where the first starts
 * label     := vint(name length) name (vint(value length) value)+
 * </pre>
 * {@code width} is the fewest bytes, 1, 2 or 4, that hold the last offset. Names are unique and non-empty, values non-empty;
 * a label holds its values up to where the next one starts. Names compare as unsigned bytes, which is code point order.
 * <p>
 * Equal label sets get equal bytes, so a label container keys groups and joins as it is, and a page holds it dictionary
 * encoded: each distinct label set once, and an ordinal per row. A label is found by a binary search over the table, and a
 * rewrite copies the labels it keeps as they are, so no label container function parses one. Only {@link #toJson} turns a label
 * container into the JSON object users see.
 */
public final class TimeSeriesMetadataEncoding {
    /** The label container of a series without labels. */
    public static final BytesRef EMPTY = new BytesRef();

    private TimeSeriesMetadataEncoding() {}

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid label container");
    }

    private static void writeVInt(BreakingBytesRefBuilder out, int value) {
        while ((value & ~0x7F) != 0) {
            out.append((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.append((byte) value);
    }

    /**
     * Reads one label container: a label by its position in name order, or found by name. The names and values it hands out are
     * views of the label container.
     */
    static final class Reader {
        private byte[] bytes;
        private int count;
        private int width;
        /** Where the table of offsets starts. */
        private int offsets;
        /** Where the first label starts. */
        private int labels;
        private int end;
        /** Where the vint read last ends. */
        private int position;

        Reader reset(BytesRef container) {
            bytes = container.bytes;
            end = container.offset + container.length;
            position = container.offset;
            if (container.length == 0) {
                count = 0;
                return this;
            }
            count = readVInt();
            if (count <= 0 || position >= end) {
                throw invalid();
            }
            width = bytes[position++];
            if (width != 1 && width != 2 && width != 4) {
                throw invalid();
            }
            offsets = position;
            labels = offsets + (count - 1) * width;
            if (labels >= end || labels < offsets) {
                throw invalid();
            }
            // each label is checked as it is read, so a lookup reads only the labels its search visits
            return this;
        }

        /** The number of labels. */
        int count() {
            return count;
        }

        /** Where label {@code i} starts. */
        private int start(int i) {
            if (i == 0) {
                return labels;
            }
            int at = offsets + (i - 1) * width;
            int offset = bytes[at] & 0xFF;
            for (int b = 1; b < width; b++) {
                offset |= (bytes[at + b] & 0xFF) << (8 * b);
            }
            if (offset <= 0 || offset >= end - labels) {
                throw invalid();
            }
            return labels + offset;
        }

        /** Where label {@code i} ends. */
        private int end(int i) {
            return i + 1 < count ? start(i + 1) : end;
        }

        /** The name of label {@code i}, a view written to {@code name}. */
        BytesRef name(int i, BytesRef name) {
            position = start(i);
            int length = readVInt();
            if (length <= 0 || length > end(i) - position) {
                throw invalid();
            }
            name.bytes = bytes;
            name.offset = position;
            name.length = length;
            return name;
        }

        /** The position of the label named {@code name}, or {@code -(insertion point) - 1} when there is none. */
        int find(BytesRef name) {
            BytesRef scratch = new BytesRef();
            int low = 0;
            int high = count - 1;
            while (low <= high) {
                int middle = (low + high) >>> 1;
                int order = name(middle, scratch).compareTo(name);
                if (order < 0) {
                    low = middle + 1;
                } else if (order > 0) {
                    high = middle - 1;
                } else {
                    return middle;
                }
            }
            return -(low + 1);
        }

        /** Where the values of label {@code i} start. */
        private int values(int i) {
            position = start(i);
            int length = readVInt();
            return position + length;
        }

        /** The number of values of label {@code i}. */
        int valueCount(int i) {
            int labelEnd = end(i);
            position = values(i);
            int values = 0;
            while (position < labelEnd) {
                skip(readVInt(), labelEnd);
                values++;
            }
            if (values == 0) {
                throw invalid();
            }
            return values;
        }

        /** The values of label {@code i}, in order, each a view written to its own {@link BytesRef}. */
        List<BytesRef> values(int i, List<BytesRef> values) {
            int labelEnd = end(i);
            position = values(i);
            while (position < labelEnd) {
                int length = readVInt();
                values.add(new BytesRef(bytes, position, length));
                skip(length, labelEnd);
            }
            return values;
        }

        /** Appends the values of label {@code i}: one as a value, several as one position. */
        void appendValues(int i, BytesRefBlock.Builder result) {
            int labelEnd = end(i);
            position = values(i);
            BytesRef value = new BytesRef();
            value.bytes = bytes;
            value.length = readVInt();
            value.offset = position;
            skip(value.length, labelEnd);
            if (position == labelEnd) {
                result.appendBytesRef(value);
                return;
            }
            result.beginPositionEntry();
            result.appendBytesRef(value);
            while (position < labelEnd) {
                value.length = readVInt();
                value.offset = position;
                skip(value.length, labelEnd);
                result.appendBytesRef(value);
            }
            result.endPositionEntry();
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

        private void skip(int length, int limit) {
            if (length <= 0 || length > limit - position) {
                throw invalid();
            }
            position += length;
        }
    }

    /**
     * Writes label containers: the labels, in name order, then {@link #build} the container of them. A label that gets no value is
     * left out. Its buffers reserve memory, so it is closed.
     */
    static final class Writer implements Releasable {
        private final BreakingBytesRefBuilder labels;
        private final BreakingBytesRefBuilder container;
        private int[] starts = new int[8];
        private int count;
        /** Where the label being written starts, and whether it got a value yet. */
        private int labelStart = -1;
        private boolean valued;

        Writer(CircuitBreaker breaker) {
            BreakingBytesRefBuilder labels = new BreakingBytesRefBuilder(breaker, "ts_metadata_labels");
            BreakingBytesRefBuilder container = null;
            try {
                container = new BreakingBytesRefBuilder(breaker, "ts_metadata");
            } finally {
                if (container == null) {
                    Releasables.closeExpectNoException(labels);
                }
            }
            this.labels = labels;
            this.container = container;
        }

        void clear() {
            labels.clear();
            count = 0;
            labelStart = -1;
        }

        /** Starts a label named {@code name}, which comes after every label written so far. */
        void beginLabel(BytesRef name) {
            assert labelStart < 0 : "a label is being written";
            labelStart = labels.length();
            valued = false;
            writeVInt(labels, name.length);
            labels.append(name);
        }

        /** Writes a value of the label begun last; an empty one is left out. */
        void value(BytesRef value) {
            if (value.length > 0) {
                writeVInt(labels, value.length);
                labels.append(value);
                valued = true;
            }
        }

        /** Ends the label begun last: whether it got a value, as one that got none is left out. */
        boolean endLabel() {
            if (valued) {
                added(labelStart);
            } else {
                labels.setLength(labelStart);
            }
            labelStart = -1;
            return valued;
        }

        /** Writes {@code name} holding {@code values}, the empty ones left out. */
        void label(BytesRef name, List<BytesRef> values) {
            beginLabel(name);
            for (BytesRef value : values) {
                value(value);
            }
            endLabel();
        }

        /** Copies label {@code i} of {@code reader} as it is. */
        void copy(Reader reader, int i) {
            assert labelStart < 0 : "a label is being written";
            int start = reader.start(i);
            added(labels.length());
            labels.append(reader.bytes, start, reader.end(i) - start);
        }

        private void added(int start) {
            if (count == starts.length) {
                starts = Arrays.copyOf(starts, count * 2);
            }
            starts[count++] = start;
        }

        /** The label container of the labels written since {@link #clear}, a view valid until the next one. */
        BytesRef build() {
            assert labelStart < 0 : "a label is being written";
            container.clear();
            if (count == 0) {
                return container.bytesRefView();
            }
            int last = starts[count - 1];
            int width = last <= 0xFF ? 1 : last <= 0xFFFF ? 2 : 4;
            writeVInt(container, count);
            container.append((byte) width);
            for (int i = 1; i < count; i++) {
                for (int b = 0; b < width; b++) {
                    container.append((byte) (starts[i] >>> (8 * b)));
                }
            }
            container.append(labels.bytes(), 0, labels.length());
            return container.bytesRefView();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(labels, container);
        }
    }

    /**
     * The label container of the JSON object {@code source} of a series' dimensions, as the keyword {@code _timeseries} holds them,
     * written by {@code out}: labels named as PromQL names them, each holding its non-empty values in source order.
     * <p>
     * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
     * container. Prometheus has no empty label, so data written by it never holds such a pair.
     */
    static BytesRef fromSource(BytesRef source, Writer out) {
        out.clear();
        try {
            for (var label : readSource(source).entrySet()) {
                out.label(label.getKey(), label.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid label container", e);
        }
        return out.build();
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

    /** {@link #fromSource(BytesRef, Writer)} outside of a driver: for literals and test values. */
    public static BytesRef fromSource(BytesRef source) {
        try (Writer out = new Writer(new NoopCircuitBreaker("tsmd"))) {
            return BytesRef.deepCopyOf(fromSource(source, out));
        }
    }

    /** The label container holding {@code labels}, a label without values left out: for literals and test values. */
    public static BytesRef encode(Map<String, List<String>> labels) {
        var sorted = new TreeMap<BytesRef, List<BytesRef>>();
        labels.forEach((name, values) -> sorted.put(new BytesRef(name), values.stream().map(BytesRef::new).toList()));
        try (Writer out = new Writer(new NoopCircuitBreaker("tsmd"))) {
            for (var label : sorted.entrySet()) {
                out.label(label.getKey(), label.getValue());
            }
            return BytesRef.deepCopyOf(out.build());
        }
    }

    /** Writes {@code encoded} as the JSON object users see: labels in name order, one value as a string, several as an array. */
    static void toJson(BytesRef encoded, XContentBuilder builder) throws IOException {
        builder.startObject();
        Reader reader = new Reader().reset(encoded);
        BytesRef name = new BytesRef();
        var values = new ArrayList<BytesRef>();
        for (int i = 0; i < reader.count(); i++) {
            builder.field(reader.name(i, name).utf8ToString());
            values.clear();
            reader.values(i, values);
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
