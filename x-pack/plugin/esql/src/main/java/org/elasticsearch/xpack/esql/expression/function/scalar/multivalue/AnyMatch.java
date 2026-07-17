/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isRepresentableExceptCountersDenseVectorAggregateMetricDoubleAndHistogram;

/**
 * Returns {@code true} if any element of a multi-value field satisfies the given lambda predicate.
 * This is a snapshot-only stub; the evaluator is not yet implemented.
 */
public class AnyMatch extends EsqlScalarFunction implements LambdaAccepting {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "AnyMatch", AnyMatch::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(AnyMatch.class).binary(AnyMatch::new).name("any_match");

    private final Expression field;
    private final Expression lambda;

    @FunctionInfo(
        returnType = { "boolean" },
        preview = true,
        description = "Returns true if any element of a multi-value field satisfies the given predicate."
    )
    public AnyMatch(
        Source source,
        @Param(name = "field", type = { "?" }, description = "A multi-value field.") Expression field,
        @Param(name = "predicate", type = { "?" }, description = "A lambda predicate.") Expression lambda
    ) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = lambda;
    }

    private AnyMatch(StreamInput in) throws IOException {
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
        return DataType.BOOLEAN;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("any_match evaluator not yet implemented");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new AnyMatch(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, AnyMatch::new, field, lambda);
    }
}
