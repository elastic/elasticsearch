/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Asin;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Slowdown function - for debug purposes only.
 * Syntax: SLOW(boolean, ms) - if boolean is true, the function will sleep for ms milliseconds.
 * The boolean is useful if you want to slow down processing on specific index or cluster only.
 */
public class Slow extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Slow", Slow::new);

    @FunctionInfo(
        returnType = { "boolean" },
        description = "Debug function to slow down processing. The function will sleep for the specified number of milliseconds.",
        examples = { @Example(file = "null", tag = "slow") }
    )
    public Slow(
        Source source,
        @Param(name = "ms", type = { "integer", "long", "time_duration", }, description = "For how long") Expression ms
    ) {
        super(source, ms);
    }

    private Slow(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Slow(source(), newChildren.getFirst());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isType(
            field(),
            t -> t == DataType.TIME_DURATION || t.isWholeNumber(),
            sourceText(),
            FIRST,
            "long",
            "integer",
            "time_duration"
        );
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Slow::new, field());
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Object fold() {
        return null;
    }

    private long msValue() {
        if (field().foldable() == false) {
            throw new IllegalArgumentException("function [" + sourceText() + "] has invalid argument [" + field().sourceText() + "]");
        }
        var ms = field().fold();
        if (ms instanceof Duration duration) {
            return duration.toMillis();
        }
        return ((Number) ms).longValue();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        return new SlowEvaluator.Factory(source(), msValue());
    }

    @Evaluator
    static boolean process(@Fixed long ms) {
        // Only activate in snapshot builds
        if (Build.current().isSnapshot()) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                return true;
            }
        }
        return true;
    }
}
