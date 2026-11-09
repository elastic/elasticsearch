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
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Evaluates reads and removals of labels in a typed label container. The label container is the canonical JSON object
 * {@link ToTimeSeries} writes, so both stream over it: a read stops at its label, and a removal copies the other labels
 * in their order, which keeps the result canonical. Labels are folded once.
 */
public final class TimeSeriesEvaluator implements ExpressionEvaluator {
    enum Operation {
        GET,
        UNSET
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesEvaluator.class);

    private final Operation operation;
    private final ExpressionEvaluator labelContainer;
    private final List<String> names;
    private final DriverContext context;

    private TimeSeriesEvaluator(Operation operation, ExpressionEvaluator labelContainer, List<String> names, DriverContext context) {
        this.operation = operation;
        this.labelContainer = labelContainer;
        this.names = names;
        this.context = context;
    }

    static Factory factory(Operation operation, List<Expression> arguments, ToEvaluator toEvaluator) {
        String[] names = new String[arguments.size() - 1];
        for (int i = 0; i < names.length; i++) {
            names[i] = ((BytesRef) arguments.get(i + 1).fold(toEvaluator.foldCtx())).utf8ToString();
        }
        return new Factory(operation, toEvaluator.apply(arguments.getFirst()), List.of(names));
    }

    @Override
    public Block eval(Page page) {
        try (Block block = labelContainer.eval(page)) {
            return eval(page.getPositionCount(), block);
        }
    }

    private Block eval(int positions, Block block) {
        BytesRef scratch = new BytesRef();
        try (
            BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(positions);
            BreakingBytesRefBuilder buffer = new BreakingBytesRefBuilder(context.breaker(), "ts_transform")
        ) {
            for (int p = 0; p < positions; p++) {
                BytesRef encoded = null;
                if (block.isNull(p) == false) {
                    if (block.getValueCount(p) != 1) {
                        throw new IllegalArgumentException("The label container must be single-valued");
                    }
                    encoded = ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(p), scratch);
                }
                try {
                    switch (operation) {
                        case GET -> read(encoded, names.getFirst(), result);
                        case UNSET -> result.appendBytesRef(remove(encoded, names, buffer));
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

    /** Copies every label but {@code names}, in their order, so a canonical label container stays canonical. */
    private static BytesRef remove(@Nullable BytesRef encoded, List<String> names, BreakingBytesRefBuilder buffer) throws IOException {
        buffer.clear();
        try (XContentBuilder builder = jsonBuilder(buffer)) {
            builder.startObject();
            if (encoded != null) {
                try (XContentParser parser = parser(encoded)) {
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        String label = parser.currentName();
                        parser.nextToken();
                        if (names.contains(label)) {
                            parser.skipChildren();
                        } else {
                            builder.field(label);
                            builder.copyCurrentStructure(parser);
                        }
                    }
                    if (parser.nextToken() != null) {
                        throw new IllegalArgumentException("Unexpected content after the label container");
                    }
                }
            }
            builder.endObject();
        }
        return buffer.bytesRefView();
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

    /** A JSON builder writing into breaker-accounted {@code buffer}; {@link ToTimeSeries} writes the canonical form with it. */
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
        for (String name : names) {
            bytes += RamUsageEstimator.sizeOf(name);
        }
        return bytes;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(labelContainer);
    }

    @Override
    public String toString() {
        return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", labelContainer=" + labelContainer + "]";
    }

    record Factory(Operation operation, ExpressionEvaluator.Factory labelContainer, List<String> names)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new TimeSeriesEvaluator(operation, labelContainer.get(context), names, context);
        }

        @Override
        public String toString() {
            return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", labelContainer=" + labelContainer + "]";
        }
    }
}
