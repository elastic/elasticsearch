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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Internal function that evaluates expressions on a metadata container: the keyword {@code _timeseries} JSON object of a series'
 * dimensions, which reads as the label container it holds.
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
    public List<Expression> labels() {
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
        for (Expression label : labels()) {
            resolution = resolution.and(labelName(label, ParamOrdinal.fromIndex(children().indexOf(label))));
        }
        return resolution;
    }

    private TypeResolution labelName(Expression name, ParamOrdinal ordinal) {
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

    /** The name a label argument folds to; type resolution checks it is a non-empty string. */
    protected static String foldLabelName(Expression label, ToEvaluator toEvaluator) {
        return ((BytesRef) label.fold(toEvaluator.foldCtx())).utf8ToString();
    }

    /**
     * Evaluates a label container function. A keyword {@code _timeseries} is the JSON object of a series' dimensions as the source
     * holds them, so it is first read into the canonical form - labels sorted by name, see {@link #canonical} - and every function
     * streams over that: a read stops at its label, and a rewrite copies the labels it keeps in their order, writing a set label
     * at its sorted position, which keeps the result canonical.
     * <p>
     * When the other arguments are constant on a page, the result depends on the label containers alone: a constant label
     * container is evaluated once, and a dictionary encoded one once per dictionary entry.
     */
    public abstract static class AbstractEvaluator implements ExpressionEvaluator {
        protected final DriverContext driverContext;
        private final ExpressionEvaluator labelContainer;
        /** The arguments after the label container and the label names, evaluated per page. */
        private final ExpressionEvaluator[] arguments;

        protected AbstractEvaluator(DriverContext driverContext, ExpressionEvaluator labelContainer, ExpressionEvaluator... arguments) {
            this.driverContext = driverContext;
            this.labelContainer = labelContainer;
            this.arguments = arguments;
        }

        /**
         * Appends the result for {@code labels}, the label container of a row or null when the row has none, where
         * {@code arguments} hold the other arguments at {@code position}. {@code scratch} holds a label container being written.
         */
        protected abstract void eval(
            @Nullable BytesRef labels,
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
            try (BytesRefBlock containers = (BytesRefBlock) labelContainer.eval(page)) {
                // The result depends on the label containers alone when every other argument is the same on every row.
                boolean constantArguments = true;
                for (int i = 0; i < arguments.length; i++) {
                    values[i] = arguments[i].eval(page);
                    Vector argument = values[i].asVector();
                    constantArguments &= values[i].areAllValuesNull() || (argument != null && argument.isConstant());
                }
                BytesRefVector vector = containers.asVector();
                if (vector != null && constantArguments) {
                    if (vector.isConstant()) {
                        // One label container on the page: evaluate it once.
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
                     * A label container loads once per series and page, so a time-series aggregate leaves it dictionary encoded:
                     * evaluate once per dictionary entry. When every entry gives one value, the result is a dictionary over the
                     * same ordinals; when one gives none or several, as a read of an absent label or of a label of several values
                     * does, the values per entry are copied out to the rows.
                     *
                     * TODO(sidosera): filters, hash joins and top-n copy a dictionary encoded block out to its rows, so a page
                     * above them holds no dictionary and the function runs once per row. Restore the ordinals there; a label
                     * container should only ever be dictionary encoded.
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
                BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(driverContext.breaker(), "ts_metadata");
                BreakingBytesRefBuilder canonical = new BreakingBytesRefBuilder(driverContext.breaker(), "ts_metadata_source")
            ) {
                for (int p = 0; p < positions; p++) {
                    BytesRef labels = null;
                    if (containers.isNull(p) == false) {
                        if (containers.getValueCount(p) != 1) {
                            throw new IllegalArgumentException("The label container must be single-valued");
                        }
                        labels = canonical(containers.getBytesRef(containers.getFirstValueIndex(p), bytes), canonical);
                    }
                    try {
                        eval(labels, arguments, constant ? 0 : p, result, scratch);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Invalid label container", e);
                    }
                }
                return result.build();
            }
        }

        @Override
        public final long baseRamBytesUsed() {
            long bytes = shallowRamBytesUsed() + labelContainer.baseRamBytesUsed();
            for (ExpressionEvaluator argument : arguments) {
                bytes += argument.baseRamBytesUsed();
            }
            return bytes;
        }

        @Override
        public final void close() {
            Releasables.closeExpectNoException(labelContainer, Releasables.wrap(arguments));
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

        /** Opens {@code labels}, positioned on its object. */
        static XContentParser parser(BytesRef labels) throws IOException {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                labels.bytes,
                labels.offset,
                labels.length
            );
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                parser.close();
                throw new IllegalArgumentException("The label container must be an object");
            }
            return parser;
        }

        static void end(XContentParser parser) throws IOException {
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("Unexpected content after the label container");
            }
        }

        /** A JSON builder writing into breaker-accounted {@code scratch}. */
        static XContentBuilder jsonBuilder(BreakingBytesRefBuilder scratch) throws IOException {
            return XContentFactory.jsonBuilder(new BytesRefBuilderOutputStream(scratch));
        }

        /** One value is written as a string, several as an array. */
        static void write(XContentBuilder builder, String name, List<String> values) throws IOException {
            if (values.size() == 1) {
                builder.field(name, values.getFirst());
            } else {
                builder.array(name, values.toArray(String[]::new));
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
