/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Evaluates reads and rewrites of labels in a label container. A typed label container is the canonical JSON object - labels
 * sorted by name - so every operation streams over it: a read stops at its label, and a rewrite copies the labels it keeps in
 * their order, writing a set label at its sorted position, which keeps the result canonical. A keyword {@code _timeseries} is
 * the JSON object of a series' dimensions as the source holds them, so it is first read into the canonical form
 * (see {@link #canonical}); a rewrite of it is canonical too. Label names are folded once.
 */
public final class TimeSeriesMetadataFunctionEvaluator implements ExpressionEvaluator {
    enum Operation {
        GET,
        DROP,
        KEEP,
        SET
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesMetadataFunctionEvaluator.class);

    private final Operation operation;
    private final ExpressionEvaluator labelContainer;
    /** Whether the label container is a keyword {@code _timeseries}, read into the canonical form first. */
    private final boolean keyword;
    private final List<String> names;
    /** The value {@link Operation#SET} writes, null for the other operations. */
    private final @Nullable ExpressionEvaluator value;
    private final DriverContext context;

    private TimeSeriesMetadataFunctionEvaluator(
        Operation operation,
        ExpressionEvaluator labelContainer,
        boolean keyword,
        List<String> names,
        @Nullable ExpressionEvaluator value,
        DriverContext context
    ) {
        this.operation = operation;
        this.labelContainer = labelContainer;
        this.keyword = keyword;
        this.names = names;
        this.value = value;
        this.context = context;
    }

    /** {@code arguments} are the label container then the label names, and for {@link Operation#SET} the value last. */
    static Factory factory(Operation operation, List<Expression> arguments, ToEvaluator toEvaluator) {
        int labels = operation == Operation.SET ? 1 : arguments.size() - 1;
        String[] names = new String[labels];
        for (int i = 0; i < names.length; i++) {
            names[i] = ((BytesRef) arguments.get(i + 1).fold(toEvaluator.foldCtx())).utf8ToString();
        }
        ExpressionEvaluator.Factory value = operation == Operation.SET ? toEvaluator.apply(arguments.getLast()) : null;
        boolean keyword = arguments.getFirst().dataType() == DataType.KEYWORD;
        return new Factory(operation, toEvaluator.apply(arguments.getFirst()), keyword, List.of(names), value);
    }

    @Override
    public Block eval(Page page) {
        try (Block block = labelContainer.eval(page); Block values = value == null ? null : value.eval(page)) {
            return eval(page.getPositionCount(), block, (BytesRefBlock) values);
        }
    }

    private Block eval(int positions, Block block, @Nullable BytesRefBlock values) {
        BytesRef scratch = new BytesRef();
        try (
            BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(positions);
            BreakingBytesRefBuilder buffer = new BreakingBytesRefBuilder(context.breaker(), "ts_metadata");
            BreakingBytesRefBuilder canonical = keyword ? new BreakingBytesRefBuilder(context.breaker(), "ts_metadata_source") : null
        ) {
            for (int p = 0; p < positions; p++) {
                BytesRef encoded = null;
                if (block.isNull(p) == false) {
                    if (block.getValueCount(p) != 1) {
                        throw new IllegalArgumentException("The label container must be single-valued");
                    }
                    encoded = ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(p), scratch);
                    if (canonical != null) {
                        encoded = canonical(encoded, canonical);
                    }
                }
                try {
                    switch (operation) {
                        case GET -> read(encoded, names.getFirst(), result);
                        case DROP -> result.appendBytesRef(copy(encoded, names, false, buffer));
                        case KEEP -> result.appendBytesRef(copy(encoded, names, true, buffer));
                        case SET -> result.appendBytesRef(set(encoded, names.getFirst(), written(values, p), buffer));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Invalid label container", e);
                }
            }
            return result.build();
        }
    }

    /** Appends the values of {@code name}, or null when the label is absent. */
    private static void read(@Nullable BytesRef encoded, String name, BytesRefBlock.Builder result) throws IOException {
        if (encoded != null) {
            try (XContentParser parser = parser(encoded)) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    boolean match = parser.currentName().equals(name);
                    XContentParser.Token token = parser.nextToken();
                    if (match == false) {
                        parser.skipChildren();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        result.beginPositionEntry();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            result.appendBytesRef(new BytesRef(parser.text()));
                        }
                        result.endPositionEntry();
                        return;
                    } else {
                        result.appendBytesRef(new BytesRef(parser.text()));
                        return;
                    }
                }
            }
        }
        result.appendNull();
    }

    /** Copies the labels in {@code names} when {@code keep}, every other label otherwise, in their order. */
    private static BytesRef copy(@Nullable BytesRef encoded, List<String> names, boolean keep, BreakingBytesRefBuilder buffer)
        throws IOException {
        buffer.clear();
        try (XContentBuilder builder = jsonBuilder(buffer)) {
            builder.startObject();
            if (encoded != null) {
                try (XContentParser parser = parser(encoded)) {
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String label = parser.currentName();
                        parser.nextToken();
                        if (names.contains(label) == keep) {
                            builder.field(label);
                            builder.copyCurrentStructure(parser);
                        } else {
                            parser.skipChildren();
                        }
                    }
                    end(parser);
                }
            }
            builder.endObject();
        }
        return buffer.bytesRefView();
    }

    /** The non-empty values at {@code position}: PromQL has no empty label, so an empty or missing value writes none. */
    private static List<String> written(BytesRefBlock values, int position) {
        if (values.isNull(position)) {
            return List.of();
        }
        BytesRef scratch = new BytesRef();
        int first = values.getFirstValueIndex(position);
        var written = new ArrayList<String>(values.getValueCount(position));
        for (int i = first; i < first + values.getValueCount(position); i++) {
            BytesRef value = values.getBytesRef(i, scratch);
            if (value.length > 0) {
                written.add(value.utf8ToString());
            }
        }
        return written;
    }

    /** Copies every label but {@code name}, writing {@code values} as {@code name} at its sorted position; none removes it. */
    private static BytesRef set(@Nullable BytesRef encoded, String name, List<String> values, BreakingBytesRefBuilder buffer)
        throws IOException {
        buffer.clear();
        try (XContentBuilder builder = jsonBuilder(buffer)) {
            builder.startObject();
            boolean written = values.isEmpty();
            if (encoded != null) {
                try (XContentParser parser = parser(encoded)) {
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String label = parser.currentName();
                        parser.nextToken();
                        if (written == false && label.compareTo(name) >= 0) {
                            write(builder, name, values);
                            written = true;
                        }
                        if (label.equals(name)) {
                            parser.skipChildren();
                        } else {
                            builder.field(label);
                            builder.copyCurrentStructure(parser);
                        }
                    }
                    end(parser);
                }
            }
            if (written == false) {
                write(builder, name, values);
            }
            builder.endObject();
        }
        return buffer.bytesRefView();
    }

    /** One value is written as a string, several as an array, as {@code TimeSeriesMetadataConverter} does. */
    private static void write(XContentBuilder builder, String name, List<String> values) throws IOException {
        if (values.size() == 1) {
            builder.field(name, values.getFirst());
        } else {
            builder.array(name, values.toArray(String[]::new));
        }
    }

    private static void end(XContentParser parser) throws IOException {
        if (parser.nextToken() != null) {
            throw new IllegalArgumentException("Unexpected content after the label container");
        }
    }

    /** Opens {@code encoded}, positioned on its object. */
    private static XContentParser parser(BytesRef encoded) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            XContentParserConfiguration.EMPTY,
            encoded.bytes,
            encoded.offset,
            encoded.length
        );
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            parser.close();
            throw new IllegalArgumentException("The label container must be an object");
        }
        return parser;
    }

    /**
     * The label container of the JSON object {@code source} of a series' dimensions, written to {@code scratch}: labels named as
     * PromQL names them, sorted by name, each holding its non-empty values in source order, one value written as a string and
     * several as an array. Equal label sets get equal bytes, so the result can key joins and groups.
     * <p>
     * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
     * container. Prometheus has no empty label, so data written by it never holds such a pair.
     */
    static BytesRef canonical(BytesRef source, BreakingBytesRefBuilder scratch) {
        scratch.clear();
        try (XContentBuilder builder = jsonBuilder(scratch)) {
            builder.startObject();
            for (var label : readSource(source).entrySet()) {
                write(builder, label.getKey(), label.getValue());
            }
            builder.endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid label container", e);
        }
        return scratch.bytesRefView();
    }

    private static SortedMap<String, List<String>> readSource(BytesRef source) throws IOException {
        try (XContentParser parser = parser(source)) {
            var labels = new TreeMap<String, List<String>>();
            readSourceObject(parser, "", labels);
            end(parser);
            return labels;
        }
    }

    private static void readSourceObject(XContentParser parser, String prefix, Map<String, List<String>> labels) throws IOException {
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            String field = prefix + parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                readSourceObject(parser, field + ".", labels);
                continue;
            }
            var values = new ArrayList<String>();
            if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    readSourceValue(parser, token, field, values);
                }
            } else {
                readSourceValue(parser, token, field, values);
            }
            String label = PromqlLabels.labelName(field);
            if (label.isEmpty()) {
                throw new IllegalArgumentException("time-series label names must be non-empty");
            }
            if (labels.containsKey(label)) {
                throw new IllegalArgumentException("multiple dimension fields resolve to time-series label [" + label + "]");
            }
            if (values.isEmpty() == false) {
                labels.put(label, values);
            }
        }
    }

    /** Scalars keep their source text; null and empty values are left out. */
    private static void readSourceValue(XContentParser parser, XContentParser.Token token, String field, List<String> values)
        throws IOException {
        if (token.isValue()) {
            String value = parser.text();
            if (value.isEmpty() == false) {
                values.add(value);
            }
        } else if (token != XContentParser.Token.VALUE_NULL) {
            throw new IllegalArgumentException("time-series dimension [" + field + "] must contain values");
        }
    }

    /**
     * A JSON builder writing into breaker-accounted {@code buffer}; {@code TimeSeriesMetadataConverter} loads the canonical form
     * with it.
     */
    static XContentBuilder jsonBuilder(BreakingBytesRefBuilder buffer) throws IOException {
        return XContentFactory.jsonBuilder(new BytesRefBuilderOutputStream(buffer));
    }

    /** Adapts a {@link BreakingBytesRefBuilder} to an {@link OutputStream}, as {@code JsonString} does. */
    private static final class BytesRefBuilderOutputStream extends OutputStream {
        private final BreakingBytesRefBuilder builder;

        BytesRefBuilderOutputStream(BreakingBytesRefBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void write(int b) {
            builder.append((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            builder.append(b, off, len);
        }
    }

    @Override
    public long baseRamBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(names) + labelContainer.baseRamBytesUsed();
        if (value != null) {
            bytes += value.baseRamBytesUsed();
        }
        for (String name : names) {
            bytes += RamUsageEstimator.sizeOf(name);
        }
        return bytes;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(labelContainer, value);
    }

    @Override
    public String toString() {
        return describe(operation, keyword, names, labelContainer, value);
    }

    private static String describe(
        Operation operation,
        boolean keyword,
        List<String> names,
        Object labelContainer,
        @Nullable Object value
    ) {
        return "TimeSeriesMetadataFunctionEvaluator[operation="
            + operation
            + (keyword ? ", keyword" : "")
            + ", names="
            + names
            + ", labelContainer="
            + labelContainer
            + (value == null ? "" : ", value=" + value)
            + "]";
    }

    record Factory(
        Operation operation,
        ExpressionEvaluator.Factory labelContainer,
        boolean keyword,
        List<String> names,
        @Nullable ExpressionEvaluator.Factory value
    ) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesMetadataFunctionEvaluator(
                operation,
                labelContainer.get(context),
                keyword,
                names,
                value == null ? null : value.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return describe(operation, keyword, names, labelContainer, value);
        }
    }
}
