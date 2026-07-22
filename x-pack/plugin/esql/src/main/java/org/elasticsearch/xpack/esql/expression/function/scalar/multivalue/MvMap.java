/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.LambdaEvaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.LambdaAccepting;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Applies a lambda expression to every element of a multi-value field and returns the transformed
 * values as a multi-value of the lambda body's type. Elements for which the lambda produces null
 * are dropped; if nothing remains the result is null (today's ES|QL null-collapse convention).
 * Snapshot-only while the lambda feature is under development.
 */
public class MvMap extends EsqlScalarFunction implements LambdaAccepting {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvMap", MvMap::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvMap.class).binary(MvMap::new).name("map");

    private final Expression field;
    private final Expression lambda;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        preview = true,
        description = "Applies a transformation to every element of a multi-value field.",
        examples = { @Example(file = "lambda", tag = "map") }
    )
    public MvMap(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "flattened",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "A multi-value field."
        ) Expression field,
        @Param(
            name = "transform",
            type = { "lambda" },
            description = "A lambda transforming each element.",
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT)
        ) Expression lambda
    ) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = lambda;
    }

    private MvMap(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(lambda);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return field;
    }

    public Lambda lambda() {
        if (lambda instanceof Lambda l) {
            return l;
        }
        throw new IllegalStateException("expected Lambda, got " + lambda.getClass().getSimpleName());
    }

    @Override
    public List<Attribute> resolveLambdaParams(Lambda l, List<Attribute> upstreamAttrs) {
        if (field.resolved() == false || l.parameters().size() != 1) {
            return List.of();
        }
        Attribute param = l.parameters().getFirst();
        return List.of(new ReferenceAttribute(param.source(), param.name(), field.dataType()));
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (lambda instanceof Lambda l) {
            if (l.parameters().size() != 1) {
                return new TypeResolution(
                    "second argument of [" + sourceText() + "] must be a lambda with exactly one parameter, got " + l.parameters().size()
                );
            }
            if (l.body().dataType() == DataType.NULL) {
                return TypeResolution.TYPE_RESOLVED;
            }
            return isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram(l.body(), sourceText(), SECOND);
        }
        return TypeResolutions.isType(lambda, dt -> dt == DataType.LAMBDA, sourceText(), SECOND, "lambda");
    }

    @Override
    public DataType dataType() {
        return lambda instanceof Lambda l ? l.body().dataType().noText() : DataType.UNSUPPORTED;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        if (PlannerUtils.toElementType(dataType()) == ElementType.NULL) {
            // a lambda mapping everything to null maps the whole field to null
            return ConstantEvaluators.CONSTANT_NULL_FACTORY;
        }
        ExpressionEvaluator.Factory f = toEvaluator.apply(field);
        EvaluatorMapper.LambdaBody body = toEvaluator.lambdaBody(lambda());
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvMapBooleanEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case BYTES_REF -> new MvMapBytesRefEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case DOUBLE -> new MvMapDoubleEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case INT -> new MvMapIntEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case LONG -> new MvMapLongEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMap(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMap::new, field, lambda);
    }

    /**
     * Combine step for the {@link LambdaEvaluator} contract: appends all values the lambda produced
     * for the elements in {@code [start, end)} as one position — flattening multivalued lambda
     * results, dropping nulls, and collapsing to null when nothing remains.
     */
    @LambdaEvaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, BooleanBlock body, int start, int end) {
        int count = valueCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            int first = body.getFirstValueIndex(i);
            int valueEnd = first + body.getValueCount(i);
            for (int v = first; v < valueEnd; v++) {
                builder.appendBoolean(body.getBoolean(v));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, IntBlock body, int start, int end) {
        int count = valueCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            int first = body.getFirstValueIndex(i);
            int valueEnd = first + body.getValueCount(i);
            for (int v = first; v < valueEnd; v++) {
                builder.appendInt(body.getInt(v));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, LongBlock body, int start, int end) {
        int count = valueCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            int first = body.getFirstValueIndex(i);
            int valueEnd = first + body.getValueCount(i);
            for (int v = first; v < valueEnd; v++) {
                builder.appendLong(body.getLong(v));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, DoubleBlock body, int start, int end) {
        int count = valueCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            int first = body.getFirstValueIndex(i);
            int valueEnd = first + body.getValueCount(i);
            for (int v = first; v < valueEnd; v++) {
                builder.appendDouble(body.getDouble(v));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, BytesRefBlock body, int start, int end) {
        int count = valueCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            int first = body.getFirstValueIndex(i);
            int valueEnd = first + body.getValueCount(i);
            for (int v = first; v < valueEnd; v++) {
                builder.appendBytesRef(body.getBytesRef(v, scratch));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    private static int valueCount(Block body, int start, int end) {
        int count = 0;
        for (int i = start; i < end; i++) {
            count += body.getValueCount(i);
        }
        return count;
    }
}
