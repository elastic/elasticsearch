/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.LongHash;
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
 * Evaluates reads and rewrites of labels in a label container. Every operation works on the bytes of a typed label container
 * (see {@link TimeSeriesMetadataEncoding}): a read finds its label by a binary search, and a rewrite copies the labels it keeps as
 * they are, writing a set label at its position in name order. A keyword {@code _timeseries} is the JSON object of a series'
 * dimensions as the source holds them, so it is read into a label container first, and a rewrite of it is written back as the
 * JSON object of the result.
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
    /** The names, sorted and unique: {@link Operation#KEEP} copies them in name order, {@link Operation#DROP} walks them along. */
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
        this.nameBytes = names.stream().map(BytesRef::new).sorted().distinct().toList();
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
            // A label container block holds each label set of the page once, so any dictionary of one is worth rewriting.
            if (ordinals != null && ordinals.mayHaveMultivaluedFields() == false) {
                if (written == null || sameEverywhere(written)) {
                    Block result = operation == Operation.GET ? readDictionary(ordinals) : rewriteDictionary(ordinals, written);
                    if (result != null) {
                        return result;
                    }
                } else if (written.asOrdinals() instanceof OrdinalBytesRefBlock dictionaryValues
                    && dictionaryValues.mayHaveMultivaluedFields() == false) {
                        return rewritePairs(ordinals, dictionaryValues);
                    }
            }
            return evalRows(page.getPositionCount(), block, written);
        }
    }

    /**
     * Each distinct pair of a label set and the value written rewritten once, where the values are dictionary encoded too: a value
     * computed from the labels of the same series, as {@code label_replace} writes, is one value per label set, so a series is
     * rewritten once per page and the result stays dictionary encoded.
     */
    private Block rewritePairs(OrdinalBytesRefBlock containers, OrdinalBytesRefBlock values) {
        IntBlock rows = containers.getOrdinalsBlock();
        IntBlock valueRows = values.getOrdinalsBlock();
        BytesRefVector dictionary = containers.getDictionaryVector();
        BytesRefVector valueDictionary = values.getDictionaryVector();
        int positions = rows.getPositionCount();
        BytesRef scratch = new BytesRef();
        BytesRef valueScratch = new BytesRef();
        BytesRefVector rewritten;
        IntBlock ordinals = null;
        try (
            LongHash pairs = new LongHash(1, context.bigArrays());
            IntBlock.Builder pairOrdinals = context.blockFactory().newIntBlockBuilder(positions);
            BytesRefVector.Builder pairDictionary = context.blockFactory().newBytesRefVectorBuilder(positions);
            Buffers buffers = new Buffers()
        ) {
            for (int p = 0; p < positions; p++) {
                // a missing label container is the empty one, and a missing value writes none: each is a pair too
                int container = rows.isNull(p) ? -1 : rows.getInt(rows.getFirstValueIndex(p));
                int value = valueRows.isNull(p) ? -1 : valueRows.getInt(valueRows.getFirstValueIndex(p));
                long pair = pairs.add(((long) (container + 1) << 32) | (value + 1L));
                if (pair < 0) {
                    pairOrdinals.appendInt((int) (-1 - pair));
                    continue;
                }
                BytesRef encoded = container < 0 ? null : buffers.container(dictionary.getBytesRef(container, scratch));
                BytesRef written = value < 0 ? null : valueDictionary.getBytesRef(value, valueScratch);
                List<BytesRef> label = written == null || written.length == 0 ? List.of() : List.of(written);
                pairDictionary.appendBytesRef(buffers.output(rewrite(encoded, label, buffers.rewritten)));
                pairOrdinals.appendInt((int) pair);
            }
            rewritten = pairDictionary.build();
            try {
                ordinals = pairOrdinals.build();
            } finally {
                if (ordinals == null) {
                    Releasables.closeExpectNoException(rewritten);
                }
            }
        }
        return dictionaryEncoded(ordinals, rewritten);
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
            var value = new ArrayList<BytesRef>(1);
            for (int d = 0; d < read.length; d++) {
                read[d] = -1;
                int label = find(buffers.container(dictionary.getBytesRef(d, scratch)), nameBytes.getFirst());
                if (label >= 0) {
                    value.clear();
                    if (reader.values(label, value).size() > 1) {
                        return null;
                    }
                    builder.appendBytesRef(value.getFirst());
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
        int label = find(encoded, nameBytes.getFirst());
        if (label >= 0) {
            reader.appendValues(label, result);
        } else {
            result.appendNull();
        }
    }

    /** The position of {@code name} in {@code encoded}, the reader then on it, or a negative number when it is absent. */
    private int find(@Nullable BytesRef encoded, BytesRef name) {
        return encoded == null ? -1 : reader.reset(encoded).find(name);
    }

    /** The rewrite of {@code encoded}, written by {@code out}; {@code written} only matters to {@link Operation#SET}. */
    private BytesRef rewrite(@Nullable BytesRef encoded, List<BytesRef> written, TimeSeriesMetadataEncoding.Writer out) {
        out.clear();
        reader.reset(encoded == null ? TimeSeriesMetadataEncoding.EMPTY : encoded);
        switch (operation) {
            case SET -> {
                BytesRef set = nameBytes.getFirst();
                int at = reader.find(set);
                int insertion = at >= 0 ? at : -at - 1;
                for (int i = 0; i < insertion; i++) {
                    out.copy(reader, i);
                }
                out.label(set, written);
                for (int i = at >= 0 ? at + 1 : insertion; i < reader.count(); i++) {
                    out.copy(reader, i);
                }
            }
            case KEEP -> {
                for (BytesRef kept : nameBytes) {
                    int at = reader.find(kept);
                    if (at >= 0) {
                        out.copy(reader, at);
                    }
                }
            }
            case DROP -> {
                BytesRef name = new BytesRef();
                int dropped = 0;
                for (int i = 0; i < reader.count(); i++) {
                    reader.name(i, name);
                    while (dropped < nameBytes.size() && nameBytes.get(dropped).compareTo(name) < 0) {
                        dropped++;
                    }
                    if (dropped == nameBytes.size() || nameBytes.get(dropped).equals(name) == false) {
                        out.copy(reader, i);
                    }
                }
            }
            case GET -> throw new IllegalStateException("a read does not rewrite");
        }
        return out.build();
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
        private final TimeSeriesMetadataEncoding.Writer rewritten;
        @Nullable
        private final TimeSeriesMetadataEncoding.Writer source;
        private final @Nullable BreakingBytesRefBuilder json;

        /** Each buffer reserves memory, so the ones made before a reservation fails are released. */
        Buffers() {
            TimeSeriesMetadataEncoding.Writer rewritten = null;
            TimeSeriesMetadataEncoding.Writer source = null;
            BreakingBytesRefBuilder json = null;
            boolean made = false;
            try {
                rewritten = new TimeSeriesMetadataEncoding.Writer(context.breaker());
                if (keyword) {
                    source = new TimeSeriesMetadataEncoding.Writer(context.breaker());
                    json = new BreakingBytesRefBuilder(context.breaker(), "ts_metadata_json");
                }
                made = true;
            } finally {
                if (made == false) {
                    Releasables.closeExpectNoException(rewritten, source, json);
                }
            }
            this.rewritten = rewritten;
            this.source = source;
            this.json = json;
        }

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
