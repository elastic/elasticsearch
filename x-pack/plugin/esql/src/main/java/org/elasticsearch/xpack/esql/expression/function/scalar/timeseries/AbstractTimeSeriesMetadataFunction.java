/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Internal function that evaluates expressions on a metadata container: the keyword {@code _timeseries} JSON object of a series'
 * dimensions, which reads as the metadata it holds.
 */
public abstract class AbstractTimeSeriesMetadataFunction extends EsqlScalarFunction {
    protected AbstractTimeSeriesMetadataFunction(Source source, List<Expression> arguments) {
        super(source, arguments);
    }

    protected AbstractTimeSeriesMetadataFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
        writeArguments(out);
    }

    /** Writes the arguments that are not expressions, after the children; a subclass reads them back in the same order. */
    protected void writeArguments(StreamOutput out) throws IOException {}

    /** The dimension names this function reads or writes: every argument after the metadata container by default. */
    public List<Expression> keys() {
        return children().subList(1, children().size());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        // A null metadata container is the empty one.
        TypeResolution resolution = isType(
            children().getFirst(),
            type -> type == DataType.KEYWORD,
            sourceText(),
            ParamOrdinal.FIRST,
            "_timeseries"
        );
        for (Expression key : keys()) {
            resolution = resolution.and(constantKey(key, ParamOrdinal.fromIndex(children().indexOf(key))));
        }
        return resolution;
    }

    private TypeResolution constantKey(Expression name, ParamOrdinal ordinal) {
        TypeResolution resolution = isString(name, sourceText(), ordinal).and(isFoldable(name, sourceText(), ordinal));
        if (resolution.unresolved()) {
            return resolution;
        }
        return name.fold(FoldContext.small()) instanceof BytesRef bytes && bytes.length > 0
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution("dimension names in [" + sourceText() + "] must be non-empty, single-valued strings");
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public final boolean foldable() {
        return Expressions.foldable(children());
    }

    /** The name a key argument folds to; type resolution checks it is a non-empty string. */
    protected static String foldKey(Expression key, ToEvaluator toEvaluator) {
        return ((BytesRef) key.fold(toEvaluator.foldCtx())).utf8ToString();
    }

    /**
     * Evaluates a metadata function. A keyword {@code _timeseries} is the JSON object of a series' dimensions as the
     * synthetic source writes it: objects nest as the mapping does and keys are sorted in every object. A key is named by its
     * field path, see {@link #find}. Every function streams over the container: a read stops at its key, and a rewrite copies
     * the fields it keeps in their order, writing a set key at its sorted position, so it writes metadata in the
     * same form and equal key sets get equal bytes.
     * <p>
     * When the other arguments are constant on a page, the result depends on the metadata alone: constant metadata
     * is evaluated once, and a dictionary encoded one once per dictionary entry.
     */
    public abstract static class AbstractEvaluator implements ExpressionEvaluator {
        protected final DriverContext driverContext;
        private final ExpressionEvaluator metadata;
        /** The arguments after the metadata and the key names, evaluated per page. */
        private final ExpressionEvaluator[] arguments;

        protected AbstractEvaluator(DriverContext driverContext, ExpressionEvaluator metadata, ExpressionEvaluator... arguments) {
            this.driverContext = driverContext;
            this.metadata = metadata;
            this.arguments = arguments;
        }

        /**
         * Appends the result for {@code container}, the metadata of a row or null when the row has none, where
         * {@code arguments} hold the other arguments at {@code position}. {@code scratch} holds metadata being written.
         */
        protected abstract void eval(
            @Nullable BytesRef container,
            Block[] arguments,
            int position,
            BytesRefBlock.Builder result,
            BreakingBytesRefBuilder scratch
        ) throws IOException;

        /** The size of the evaluator itself, without its argument evaluators. */
        protected abstract long shallowRamBytesUsed();

        @Override
        public final Block eval(Page page) {
            int positions = page.getPositionCount();
            Block[] values = new Block[arguments.length];
            try (BytesRefBlock containers = (BytesRefBlock) metadata.eval(page)) {
                // The result depends on the metadata alone when every other argument is the same on every row.
                boolean constantArguments = true;
                for (int i = 0; i < arguments.length; i++) {
                    values[i] = arguments[i].eval(page);
                    Vector argument = values[i].asVector();
                    constantArguments &= values[i].areAllValuesNull() || (argument != null && argument.isConstant());
                }
                BytesRefVector vector = containers.asVector();
                if (vector != null && constantArguments) {
                    if (vector.isConstant()) {
                        // One metadata on the page: evaluate it once.
                        try (BytesRefBlock one = eval(1, containers, values, true)) {
                            BytesRefVector single = one.asVector();
                            if (single != null) {
                                BytesRef result = single.getBytesRef(0, new BytesRef());
                                return driverContext.blockFactory().newConstantBytesRefBlockWith(result, positions);
                            }
                            if (one.isNull(0)) {
                                return driverContext.blockFactory().newConstantNullBlock(positions);
                            }
                        }
                    }
                    /*
                     * Metadata loads once per series and page, so a time-series aggregate leaves it dictionary encoded:
                     * evaluate once per dictionary entry. When every entry gives one value, the result is a dictionary over the
                     * same ordinals; when one gives none or several, as a read of an absent key or of a key of several values
                     * does, the values per entry are copied out to the rows.
                     *
                     * TODO(sidosera): filters, hash joins and top-n copy a dictionary encoded block out to its rows, so a page
                     * above them holds no dictionary and the function runs once per row. Restore the ordinals there; metadata
                     * should only ever be dictionary encoded.
                     */
                    OrdinalBytesRefVector dictionary = vector.asOrdinals();
                    if (dictionary != null && dictionary.isDense()) {
                        BytesRefVector entries = dictionary.getDictionaryVector();
                        try (BytesRefBlock perEntry = eval(entries.getPositionCount(), entries.asBlock(), values, true)) {
                            IntVector ordinals = dictionary.getOrdinalsVector();
                            BytesRefVector single = perEntry.asVector();
                            if (single != null) {
                                ordinals.incRef();
                                single.incRef();
                                return new OrdinalBytesRefVector(ordinals, single).asBlock();
                            }
                            int[] rows = new int[positions];
                            ordinals.copyTo(0, rows, 0, positions);
                            return perEntry.filter(true, rows, 0, positions);
                        }
                    }
                }
                return eval(positions, containers, values, false);
            } finally {
                Releasables.closeExpectNoException(values);
            }
        }

        /** One result per position of {@code containers}; {@code constant} arguments are read at their first position. */
        private BytesRefBlock eval(int positions, BytesRefBlock containers, Block[] arguments, boolean constant) {
            BytesRef bytes = new BytesRef();
            try (
                BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positions);
                BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(driverContext.breaker(), "ts_metadata")
            ) {
                for (int p = 0; p < positions; p++) {
                    BytesRef container = null;
                    if (containers.isNull(p) == false) {
                        if (containers.getValueCount(p) != 1) {
                            throw new IllegalArgumentException("The metadata must be single-valued");
                        }
                        container = containers.getBytesRef(containers.getFirstValueIndex(p), bytes);
                    }
                    try {
                        eval(container, arguments, constant ? 0 : p, result, scratch);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Invalid metadata", e);
                    }
                }
                return result.build();
            }
        }

        @Override
        public final long baseRamBytesUsed() {
            long bytes = shallowRamBytesUsed() + metadata.baseRamBytesUsed();
            for (ExpressionEvaluator argument : arguments) {
                bytes += argument.baseRamBytesUsed();
            }
            return bytes;
        }

        @Override
        public final void close() {
            Releasables.closeExpectNoException(metadata, Releasables.wrap(arguments));
        }

        /**
         * Moves {@code parser}, in an object at {@code prefix}, to the value of the field that holds {@code key}, returning false
         * when no field does. A key is named by its field path as PromQL names it, see {@link PromqlLabels#labelName(String)}.
         */
        static boolean find(XContentParser parser, String prefix, String key) throws IOException {
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = prefix + parser.currentName();
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    if (find(parser, field + ".", key)) {
                        return true;
                    }
                } else if (PromqlLabels.labelName(field).equals(key)) {
                    return true;
                } else {
                    parser.skipChildren();
                }
            }
            return false;
        }

        /** Opens {@code labels}, positioned on its object. */
        static XContentParser parser(BytesRef container) throws IOException {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                container.bytes,
                container.offset,
                container.length
            );
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                parser.close();
                throw new IllegalArgumentException("The metadata must be an object");
            }
            return parser;
        }

        static void end(XContentParser parser) throws IOException {
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("Unexpected content after the metadata");
            }
        }

        /** A JSON builder writing into breaker-accounted {@code scratch}. */
        static XContentBuilder jsonBuilder(BreakingBytesRefBuilder scratch) throws IOException {
            return XContentFactory.jsonBuilder(new BytesRefBuilderOutputStream(scratch));
        }

        /**
         * Writes rewritten metadata. An object opens only once a key is written into it, so an object left with no
         * key is left out, as the source of a series holds no object without a dimension in it.
         */
        static final class MetadataWriter {
            private final XContentBuilder builder;
            /** The objects entered but not written yet, outermost first. */
            private final List<String> pending = new ArrayList<>();

            MetadataWriter(XContentBuilder builder) {
                this.builder = builder;
            }

            void startObject(String name) {
                pending.add(name);
            }

            void endObject() throws IOException {
                if (pending.isEmpty()) {
                    builder.endObject();
                } else {
                    pending.removeLast();
                }
            }

            /** Starts the field {@code name}, opening the objects it is in. */
            XContentBuilder field(String name) throws IOException {
                for (String object : pending) {
                    builder.startObject(object);
                }
                pending.clear();
                return builder.field(name);
            }

            /** Writes the field {@code name}: one value as a string, several as an array. */
            void field(String name, List<String> values) throws IOException {
                field(name);
                if (values.size() == 1) {
                    builder.value(values.getFirst());
                } else {
                    builder.startArray();
                    for (String value : values) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
            }
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
    }
}
