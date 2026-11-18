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
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates reads and rewrites of labels in a label container. Every operation streams over the bytes of a typed label container
 * (see {@link TimeSeriesMetadataEncoding}): a read stops at its label, and a rewrite copies the labels it keeps, writing a set label
 * at its position in name order. A keyword {@code _timeseries} is the JSON object of a series' dimensions as the source holds them,
 * so it is read into a label container first, and a rewrite of it is written back as the JSON object of the result.
 * <p>
 * A label container block is dictionary encoded, each distinct label set once and an ordinal per row, so an operation that only
 * depends on the label container runs once per label set and keeps the ordinals: a series has a row per time bucket.
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
    /** Whether the label container is a keyword {@code _timeseries}, read into a typed label container first. */
    private final boolean keyword;
    private final List<String> names;
    private final List<BytesRef> nameBytes;
    /** The value {@link Operation#SET} writes, null for the other operations. */
    private final @Nullable ExpressionEvaluator value;
    private final DriverContext context;
    private final TimeSeriesMetadataEncoding.Reader reader = new TimeSeriesMetadataEncoding.Reader();

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
        this.nameBytes = names.stream().map(BytesRef::new).toList();
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
            BytesRefBlock written = (BytesRefBlock) values;
            OrdinalBytesRefBlock ordinals = block instanceof BytesRefBlock containers ? containers.asOrdinals() : null;
            if (ordinals != null
                && ordinals.isDense()
                && ordinals.mayHaveMultivaluedFields() == false
                && (written == null || sameEverywhere(written))) {
                Block result = operation == Operation.GET ? readDictionary(ordinals) : rewriteDictionary(ordinals, written);
                if (result != null) {
                    return result;
                }
            }
            return evalRows(page.getPositionCount(), block, written);
        }
    }

    /** Whether every position writes the same value, so a rewrite only depends on the label container. */
    private static boolean sameEverywhere(BytesRefBlock written) {
        BytesRefVector vector = written.asVector();
        return vector != null && vector.isConstant();
    }

    private Block evalRows(int positions, Block block, @Nullable BytesRefBlock values) {
        BytesRef scratch = new BytesRef();
        try (BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(positions); Buffers buffers = new Buffers()) {
            for (int p = 0; p < positions; p++) {
                BytesRef encoded = null;
                if (block.isNull(p) == false) {
                    if (block.getValueCount(p) != 1) {
                        throw new IllegalArgumentException("The label container must be single-valued");
                    }
                    encoded = buffers.container(((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(p), scratch));
                }
                if (operation == Operation.GET) {
                    read(encoded, result);
                } else {
                    result.appendBytesRef(buffers.output(rewrite(encoded, written(values, p), buffers.rewritten)));
                }
            }
            return result.build();
        }
    }

    /**
     * The label read off each label set once, keeping the ordinals: null where an entry holds several values, which a dictionary
     * of values cannot hold one per entry.
     */
    private @Nullable Block readDictionary(OrdinalBytesRefBlock ordinals) {
        BytesRefVector dictionary = ordinals.getDictionaryVector();
        IntBlock rows = ordinals.getOrdinalsBlock();
        int[] read = new int[dictionary.getPositionCount()];
        BytesRef scratch = new BytesRef();
        BytesRefVector values;
        try (
            Buffers buffers = new Buffers();
            BytesRefVector.Builder builder = context.blockFactory().newBytesRefVectorBuilder(dictionary.getPositionCount())
        ) {
            int next = 0;
            for (int d = 0; d < read.length; d++) {
                read[d] = -1;
                if (find(buffers.container(dictionary.getBytesRef(d, scratch)), nameBytes.getFirst())) {
                    if (reader.valueCount() > 1) {
                        return null;
                    }
                    builder.appendBytesRef(reader.values().getFirst());
                    read[d] = next++;
                }
            }
            values = builder.build();
        }
        IntBlock remapped = null;
        try (IntBlock.Builder builder = context.blockFactory().newIntBlockBuilder(rows.getPositionCount())) {
            for (int p = 0; p < rows.getPositionCount(); p++) {
                int ordinal = rows.isNull(p) ? -1 : read[rows.getInt(rows.getFirstValueIndex(p))];
                if (ordinal < 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt(ordinal);
                }
            }
            remapped = builder.build();
        } finally {
            if (remapped == null) {
                Releasables.closeExpectNoException(values);
            }
        }
        return dictionaryEncoded(remapped, values);
    }

    /**
     * Each label set rewritten once, keeping the ordinals. A missing label container is the empty one, so it rewrites to a value
     * too: an extra entry the null rows point at.
     */
    private Block rewriteDictionary(OrdinalBytesRefBlock ordinals, @Nullable BytesRefBlock values) {
        BytesRefVector dictionary = ordinals.getDictionaryVector();
        IntBlock rows = ordinals.getOrdinalsBlock();
        boolean nulls = rows.mayHaveNulls();
        List<BytesRef> written = values == null ? List.of() : written(values, 0);
        BytesRef scratch = new BytesRef();
        BytesRefVector rewritten;
        try (
            Buffers buffers = new Buffers();
            BytesRefVector.Builder builder = context.blockFactory()
                .newBytesRefVectorBuilder(dictionary.getPositionCount() + (nulls ? 1 : 0))
        ) {
            for (int d = 0; d < dictionary.getPositionCount(); d++) {
                BytesRef encoded = buffers.container(dictionary.getBytesRef(d, scratch));
                builder.appendBytesRef(buffers.output(rewrite(encoded, written, buffers.rewritten)));
            }
            if (nulls) {
                builder.appendBytesRef(buffers.output(rewrite(null, written, buffers.rewritten)));
            }
            rewritten = builder.build();
        }
        if (nulls == false) {
            rows.incRef();
            return dictionaryEncoded(rows, rewritten);
        }
        IntBlock remapped = null;
        try (IntBlock.Builder builder = context.blockFactory().newIntBlockBuilder(rows.getPositionCount())) {
            int empty = dictionary.getPositionCount();
            for (int p = 0; p < rows.getPositionCount(); p++) {
                builder.appendInt(rows.isNull(p) ? empty : rows.getInt(rows.getFirstValueIndex(p)));
            }
            remapped = builder.build();
        } finally {
            if (remapped == null) {
                Releasables.closeExpectNoException(rewritten);
            }
        }
        return dictionaryEncoded(remapped, rewritten);
    }

    /** The block of {@code ordinals} into {@code dictionary}, both released if it cannot be built. */
    private static OrdinalBytesRefBlock dictionaryEncoded(IntBlock ordinals, BytesRefVector dictionary) {
        OrdinalBytesRefBlock result = null;
        try {
            result = new OrdinalBytesRefBlock(ordinals, dictionary);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, dictionary);
            }
        }
    }

    /** Appends the values of the label read, or null when it is absent. */
    private void read(@Nullable BytesRef encoded, BytesRefBlock.Builder result) {
        if (find(encoded, nameBytes.getFirst())) {
            reader.appendValues(result);
        } else {
            result.appendNull();
        }
    }

    /** Whether {@code encoded} holds {@code name}, the reader then on it. */
    private boolean find(@Nullable BytesRef encoded, BytesRef name) {
        if (encoded == null) {
            return false;
        }
        reader.reset(encoded);
        while (reader.next()) {
            int order = reader.name().compareTo(name);
            if (order == 0) {
                return true;
            }
            if (order > 0) {
                return false;
            }
        }
        return false;
    }

    /** The rewrite of {@code encoded}, written to {@code out}; {@code written} only matters to {@link Operation#SET}. */
    private BytesRef rewrite(@Nullable BytesRef encoded, List<BytesRef> written, BreakingBytesRefBuilder out) {
        out.clear();
        BytesRef set = operation == Operation.SET ? nameBytes.getFirst() : null;
        boolean done = set == null || written.isEmpty();
        if (encoded != null) {
            reader.reset(encoded);
            while (reader.next()) {
                if (set != null) {
                    int order = reader.name().compareTo(set);
                    if (done == false && order >= 0) {
                        TimeSeriesMetadataEncoding.writeLabel(out, set, written);
                        done = true;
                    }
                    if (order != 0) {
                        reader.copyLabel(out);
                    }
                } else if (nameBytes.contains(reader.name()) == (operation == Operation.KEEP)) {
                    reader.copyLabel(out);
                }
            }
        }
        if (done == false) {
            TimeSeriesMetadataEncoding.writeLabel(out, set, written);
        }
        return out.bytesRefView();
    }

    /** The non-empty values at {@code position}: PromQL has no empty label, so an empty or missing value writes none. */
    private static List<BytesRef> written(@Nullable BytesRefBlock values, int position) {
        if (values == null || values.isNull(position)) {
            return List.of();
        }
        int first = values.getFirstValueIndex(position);
        var written = new ArrayList<BytesRef>(values.getValueCount(position));
        for (int i = first; i < first + values.getValueCount(position); i++) {
            BytesRef value = values.getBytesRef(i, new BytesRef());
            if (value.length > 0) {
                written.add(value);
            }
        }
        return written;
    }

    /**
     * The working memory of one evaluation: the rewritten label container, and for a keyword {@code _timeseries} the label
     * container read from it and the JSON object written back.
     */
    private final class Buffers implements AutoCloseable {
        private final BreakingBytesRefBuilder rewritten = new BreakingBytesRefBuilder(context.breaker(), "ts_metadata");
        private final @Nullable BreakingBytesRefBuilder source = keyword
            ? new BreakingBytesRefBuilder(context.breaker(), "ts_metadata_source")
            : null;
        private final @Nullable BreakingBytesRefBuilder json = keyword
            ? new BreakingBytesRefBuilder(context.breaker(), "ts_metadata_json")
            : null;

        /** The label container {@code stored} holds: a keyword {@code _timeseries} reads into one. */
        BytesRef container(BytesRef stored) {
            return source == null ? stored : TimeSeriesMetadataEncoding.fromSource(stored, source);
        }

        /** The rewritten label container as the operation returns it: a keyword {@code _timeseries} as its JSON object. */
        BytesRef output(BytesRef encoded) {
            if (json == null) {
                return encoded;
            }
            json.clear();
            try (XContentBuilder builder = jsonBuilder(json)) {
                TimeSeriesMetadataEncoding.toJson(encoded, builder);
            } catch (IOException e) {
                throw new UncheckedIOException("Invalid label container", e);
            }
            return json.bytesRefView();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(rewritten, source, json);
        }
    }

    /** A JSON builder writing into breaker-accounted {@code buffer}. */
    private static XContentBuilder jsonBuilder(BreakingBytesRefBuilder buffer) throws IOException {
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
            bytes += 2 * RamUsageEstimator.sizeOf(name);
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
