/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;

/**
 * Builds a JSON object string from a list of alternating key/value pairs:
 * {@code JSON_STRING(key1, value1, key2, value2, ...)}.
 * <p>
 * Keys must be strings ({@code keyword}/{@code text}). Values may be numbers, booleans or one of the
 * common representable types; numbers and booleans are rendered as JSON primitives while every other
 * supported type is rendered as a JSON string. A multivalued value becomes a JSON array and a null
 * value becomes JSON {@code null}. A null key nulls the whole row.
 */
public class JsonString extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonString",
        JsonString::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(JsonString.class).nAry(JsonString::new).name("json_string");

    /**
     * The value types this function accepts. Numbers and booleans are rendered as JSON primitives, the
     * remaining types are rendered as JSON strings.
     */
    static final Set<DataType> ACCEPTED_VALUE_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.IP,
        DataType.VERSION,
        DataType.DATETIME
    );

    /** Human readable list of accepted value types, in a fixed order, for type-resolution error messages. */
    static final String VALUE_TYPES_MESSAGE = "boolean, integer, long, double, keyword, text, ip, version or date";

    public JsonString(Source source, List<Expression> keyValuePairs) {
        super(source, keyValuePairs);
    }

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.6+") },
        returnType = "keyword",
        briefSummary = "Builds a JSON object string from key/value pairs.",
        description = """
            Builds a JSON object string from one or more key/value pairs. Keys must be strings. Numbers
            and booleans are rendered as JSON primitives, all other values are rendered as JSON strings.
            A multi-valued value is rendered as a JSON array and a null value as JSON `null`.""",
        examples = @Example(file = "json_string", tag = "json_string")
    )
    public JsonString(
        Source source,
        @Param(name = "key", type = { "keyword", "text" }, description = "The key of the first key/value pair.") Expression first,
        @Param(
            name = "value",
            type = { "boolean", "integer", "long", "double", "keyword", "text", "ip", "version", "date" },
            description = "The value of the first pair, followed by any further alternating keys and values. "
                + "Provide arguments as alternating key/value pairs."
        ) List<Expression> rest
    ) {
        this(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private JsonString(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Nullability nullable() {
        // JSON_STRING can produce a non-null output when a value argument is null: {"key":null}
        return Nullability.UNKNOWN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        List<Expression> children = children();
        if (children.size() < 2 || children.size() % 2 != 0) {
            return new TypeResolution(
                format(
                    null,
                    "function [{}] expects an even number of arguments (at least one key/value pair) but got [{}]",
                    sourceText(),
                    children.size()
                )
            );
        }

        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            TypeResolution resolution = i % 2 == 0
                ? isString(child, sourceText(), ParamOrdinal.fromIndex(i))
                : isType(child, ACCEPTED_VALUE_TYPES::contains, sourceText(), ParamOrdinal.fromIndex(i), VALUE_TYPES_MESSAGE);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        int pairs = children().size() / 2;
        ExpressionEvaluator.Factory[] keys = new ExpressionEvaluator.Factory[pairs];
        ExpressionEvaluator.Factory[] values = new ExpressionEvaluator.Factory[pairs];
        DataType[] valueTypes = new DataType[pairs];
        for (int i = 0; i < pairs; i++) {
            keys[i] = toEvaluator.apply(children().get(i * 2));
            values[i] = toEvaluator.apply(children().get(i * 2 + 1));
            valueTypes[i] = children().get(i * 2 + 1).dataType();
        }
        return new JsonStringEvaluator.Factory(source(), keys, values, valueTypes);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new JsonString(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonString::new, children().get(0), children().subList(1, children().size()));
    }

    /**
     * This function uses a hand-written {@link ExpressionEvaluator} rather than the generated
     * {@code @Evaluator} machinery because the values are heterogeneously typed: the generated evaluators
     * operate on a homogeneous array of a single block type, which can't express "one block per value, each
     * of a possibly different type".
     */
    static final class JsonStringEvaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonStringEvaluator.class);

        /**
         * Adapts a {@link BreakingBytesRefBuilder} to an {@link OutputStream} so an {@link XContentBuilder} can stream
         * directly into breaker-accounted memory.
         */
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

        private final Source source;
        private final ExpressionEvaluator[] keys;
        private final ExpressionEvaluator[] values;
        private final DataType[] valueTypes;
        private final DriverContext driverContext;
        private Warnings warnings;

        JsonStringEvaluator(
            Source source,
            ExpressionEvaluator[] keys,
            ExpressionEvaluator[] values,
            DataType[] valueTypes,
            DriverContext driverContext
        ) {
            this.source = source;
            this.keys = keys;
            this.values = values;
            this.valueTypes = valueTypes;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            BytesRefBlock[] keyBlocks = new BytesRefBlock[keys.length];
            Block[] valueBlocks = new Block[values.length];
            try (var keysRelease = Releasables.wrap(keyBlocks); var valuesRelease = Releasables.wrap(valueBlocks)) {
                for (int i = 0; i < keys.length; i++) {
                    keyBlocks[i] = (BytesRefBlock) keys[i].eval(page);
                }
                for (int i = 0; i < values.length; i++) {
                    valueBlocks[i] = values[i].eval(page);
                }
                return eval(page.getPositionCount(), keyBlocks, valueBlocks);
            }
        }

        private Block eval(int positionCount, BytesRefBlock[] keyBlocks, Block[] valueBlocks) {
            BytesRef scratch = new BytesRef();
            try (
                BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount);
                BreakingBytesRefBuilder jsonBuffer = new BreakingBytesRefBuilder(driverContext.breaker(), "json_string")
            ) {
                position: for (int p = 0; p < positionCount; p++) {
                    for (int i = 0; i < keyBlocks.length; i++) {
                        if (keyBlocks[i].isNull(p)) {
                            // A null key can't be represented in a JSON object, so the whole row is null (no warning).
                            result.appendNull();
                            continue position;
                        }
                        if (keyBlocks[i].getValueCount(p) != 1) {
                            warnings().registerException(new IllegalArgumentException("json_string keys must be single-valued"));
                            result.appendNull();
                            continue position;
                        }
                    }
                    result.appendBytesRef(buildObject(p, keyBlocks, valueBlocks, scratch, jsonBuffer));
                }
                return result.build();
            }
        }

        private BytesRef buildObject(
            int position,
            BytesRefBlock[] keyBlocks,
            Block[] valueBlocks,
            BytesRef scratch,
            BreakingBytesRefBuilder jsonBuffer
        ) {
            jsonBuffer.clear();
            try (XContentBuilder json = XContentFactory.jsonBuilder(new BytesRefBuilderOutputStream(jsonBuffer))) {
                json.startObject();
                for (int blockIndex = 0; blockIndex < keyBlocks.length; blockIndex++) {
                    BytesRefBlock keyBlock = keyBlocks[blockIndex];
                    Block valueBlock = valueBlocks[blockIndex];

                    BytesRef key = keyBlock.getBytesRef(keyBlock.getFirstValueIndex(position), scratch);
                    json.field(key.utf8ToString());

                    if (valueBlock.isNull(position)) {
                        json.nullValue();
                    } else {
                        int count = valueBlock.getValueCount(position);
                        int first = valueBlock.getFirstValueIndex(position);
                        if (count > 1) {
                            json.startArray();
                        }
                        for (int i = first; i < first + count; i++) {
                            switch (valueTypes[blockIndex]) {
                                case BOOLEAN -> json.value(((BooleanBlock) valueBlock).getBoolean(i));
                                case INTEGER -> json.value(((IntBlock) valueBlock).getInt(i));
                                case LONG -> json.value(((LongBlock) valueBlock).getLong(i));
                                case DOUBLE -> json.value(((DoubleBlock) valueBlock).getDouble(i));
                                case KEYWORD, TEXT -> json.value(((BytesRefBlock) valueBlock).getBytesRef(i, scratch).utf8ToString());
                                case IP -> json.value(ipToString(((BytesRefBlock) valueBlock).getBytesRef(i, scratch)));
                                case VERSION -> json.value(versionToString(((BytesRefBlock) valueBlock).getBytesRef(i, scratch)));
                                case DATETIME -> json.value(
                                    DEFAULT_DATE_TIME_FORMATTER.withZone(ZoneOffset.UTC).formatMillis(((LongBlock) valueBlock).getLong(i))
                                );
                                default -> throw new IllegalStateException("unsupported value type [" + valueTypes[blockIndex] + "]");
                            }
                        }
                        if (count > 1) {
                            json.endArray();
                        }
                    }
                }
                json.endObject();
            } catch (IOException e) {
                // Should never happen.
                throw new UncheckedIOException(e);
            }
            return jsonBuffer.bytesRefView();
        }

        @Override
        public long baseRamBytesUsed() {
            long ram = BASE_RAM_BYTES_USED;
            for (ExpressionEvaluator e : keys) {
                ram += e.baseRamBytesUsed();
            }
            for (ExpressionEvaluator e : values) {
                ram += e.baseRamBytesUsed();
            }
            return ram;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(() -> Releasables.close(keys), () -> Releasables.close(values));
        }

        private Warnings warnings() {
            if (warnings == null) {
                this.warnings = driverContext.createWarnings(source);
            }
            return warnings;
        }

        @Override
        public String toString() {
            return "JsonStringEvaluator[keys=" + Arrays.toString(keys) + ", values=" + Arrays.toString(values) + "]";
        }

        static final class Factory implements ExpressionEvaluator.Factory {
            private final Source source;
            private final ExpressionEvaluator.Factory[] keys;
            private final ExpressionEvaluator.Factory[] values;
            private final DataType[] valueTypes;

            Factory(Source source, ExpressionEvaluator.Factory[] keys, ExpressionEvaluator.Factory[] values, DataType[] valueTypes) {
                this.source = source;
                this.keys = keys;
                this.values = values;
                this.valueTypes = valueTypes;
            }

            @Override
            public ExpressionEvaluator get(DriverContext context) {
                ExpressionEvaluator[] keyEvaluators = new ExpressionEvaluator[keys.length];
                ExpressionEvaluator[] valueEvaluators = new ExpressionEvaluator[values.length];
                try {
                    for (int i = 0; i < keys.length; i++) {
                        keyEvaluators[i] = keys[i].get(context);
                    }
                    for (int i = 0; i < values.length; i++) {
                        valueEvaluators[i] = values[i].get(context);
                    }
                    JsonStringEvaluator evaluator = new JsonStringEvaluator(source, keyEvaluators, valueEvaluators, valueTypes, context);
                    keyEvaluators = null;
                    valueEvaluators = null;
                    return evaluator;
                } finally {
                    Releasables.close(
                        keyEvaluators == null ? () -> {} : Releasables.wrap(keyEvaluators),
                        valueEvaluators == null ? () -> {} : Releasables.wrap(valueEvaluators)
                    );
                }
            }

            @Override
            public String toString() {
                return "JsonStringEvaluator[keys=" + Arrays.toString(keys) + ", values=" + Arrays.toString(values) + "]";
            }
        }
    }
}
