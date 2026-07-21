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
 * Keeps the elements of a multi-value field for which the given lambda predicate is {@code true}.
 * When no element matches, the result is null (today's ES|QL null-collapse convention).
 * Snapshot-only while the lambda feature is under development.
 */
public class MvFilter extends EsqlScalarFunction implements LambdaAccepting {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvFilter", MvFilter::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvFilter.class).binary(MvFilter::new).name("filter");

    private final Expression field;
    private final Expression lambda;

    @FunctionInfo(
        returnType = { "?" },
        preview = true,
        description = "Keeps the elements of a multi-value field that satisfy the given predicate."
    )
    public MvFilter(
        Source source,
        @Param(name = "field", type = { "?" }, description = "A multi-value field.") Expression field,
        @Param(name = "predicate", type = { "?" }, description = "A lambda predicate.") Expression lambda
    ) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = lambda;
    }

    private MvFilter(StreamInput in) throws IOException {
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
            return TypeResolutions.isType(l.body(), dt -> dt == DataType.BOOLEAN, sourceText(), SECOND, "boolean");
        }
        return TypeResolutions.isType(lambda, dt -> dt == DataType.LAMBDA, sourceText(), SECOND, "lambda");
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        if (PlannerUtils.toElementType(dataType()) == ElementType.NULL) {
            return ConstantEvaluators.CONSTANT_NULL_FACTORY;
        }
        ExpressionEvaluator.Factory f = toEvaluator.apply(field);
        EvaluatorMapper.LambdaBody body = toEvaluator.lambdaBody(lambda());
        return switch (PlannerUtils.toElementType(dataType())) {
            case BOOLEAN -> new MvFilterBooleanEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case BYTES_REF -> new MvFilterBytesRefEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case DOUBLE -> new MvFilterDoubleEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case INT -> new MvFilterIntEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            case LONG -> new MvFilterLongEvaluator.Factory(f, body.bodyFactory(), body.outerChannels());
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvFilter(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvFilter::new, field, lambda);
    }

    /**
     * Combine step for the {@link LambdaEvaluator} contract: appends the field values in
     * {@code [start, end)} whose lambda result matches (any {@code true} value counts, null counts
     * as no match) as one position, collapsing to null when nothing matches.
     */
    @LambdaEvaluator(extraName = "Boolean")
    static void process(BooleanBlock.Builder builder, BooleanBlock field, BooleanBlock body, int start, int end) {
        int count = matchCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                builder.appendBoolean(field.getBoolean(field.getFirstValueIndex(i)));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, IntBlock field, BooleanBlock body, int start, int end) {
        int count = matchCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                builder.appendInt(field.getInt(field.getFirstValueIndex(i)));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, LongBlock field, BooleanBlock body, int start, int end) {
        int count = matchCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                builder.appendLong(field.getLong(field.getFirstValueIndex(i)));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, DoubleBlock field, BooleanBlock body, int start, int end) {
        int count = matchCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                builder.appendDouble(field.getDouble(field.getFirstValueIndex(i)));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    @LambdaEvaluator(extraName = "BytesRef")
    static void process(BytesRefBlock.Builder builder, BytesRefBlock field, BooleanBlock body, int start, int end) {
        int count = matchCount(body, start, end);
        if (count == 0) {
            builder.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        if (count > 1) {
            builder.beginPositionEntry();
        }
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                builder.appendBytesRef(field.getBytesRef(field.getFirstValueIndex(i), scratch));
            }
        }
        if (count > 1) {
            builder.endPositionEntry();
        }
    }

    private static int matchCount(BooleanBlock body, int start, int end) {
        int count = 0;
        for (int i = start; i < end; i++) {
            if (matches(body, i)) {
                count++;
            }
        }
        return count;
    }

    /** True if any value the lambda produced for this row is {@code true}; a null row is no match. */
    private static boolean matches(BooleanBlock body, int row) {
        int first = body.getFirstValueIndex(row);
        int end = first + body.getValueCount(row);
        for (int v = first; v < end; v++) {
            if (body.getBoolean(v)) {
                return true;
            }
        }
        return false;
    }
}
