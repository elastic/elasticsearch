/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Named;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxBooleanEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxBytesRefEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxDoubleEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxIntEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMaxLongEvaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

/**
 * Returns the maximum value of multiple columns.
 */
public class Greatest extends ScalarFunction implements EvaluatorMapper, OptionalArgument {
    private DataType dataType;

    public Greatest(Source source, @Named("first") Expression first, @Named("rest") List<Expression> rest) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        for (int position = 0; position < children().size(); position++) {
            Expression child = children().get(position);
            if (dataType == null || dataType == NULL) {
                dataType = child.dataType();
                continue;
            }
            TypeResolution resolution = TypeResolutions.isType(
                child,
                t -> t == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                dataType.typeName()
            );
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Greatest(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Greatest::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var suppliers = children().stream().map(toEvaluator).toList();
        if (dataType == DataTypes.BOOLEAN) {
            return dvrCtx -> new GreatestBooleanEvaluator(
                suppliers.stream()
                    .map(es -> es.get(dvrCtx))
                    .map(ev -> new MvMaxBooleanEvaluator(ev, dvrCtx))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                dvrCtx
            );
        }
        if (dataType == DataTypes.DOUBLE) {
            return dvrCtx -> new GreatestDoubleEvaluator(
                suppliers.stream()
                    .map(es -> es.get(dvrCtx))
                    .map(ev -> new MvMaxDoubleEvaluator(ev, dvrCtx))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                dvrCtx
            );
        }
        if (dataType == DataTypes.INTEGER) {
            return dvrCtx -> new GreatestIntEvaluator(
                suppliers.stream()
                    .map(es -> es.get(dvrCtx))
                    .map(ev -> new MvMaxIntEvaluator(ev, dvrCtx))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                dvrCtx
            );
        }
        if (dataType == DataTypes.LONG) {
            return dvrCtx -> new GreatestLongEvaluator(
                suppliers.stream()
                    .map(es -> es.get(dvrCtx))
                    .map(ev -> new MvMaxLongEvaluator(ev, dvrCtx))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                dvrCtx
            );
        }
        if (dataType == DataTypes.KEYWORD
            || dataType == DataTypes.TEXT
            || dataType == DataTypes.IP
            || dataType == DataTypes.VERSION
            || dataType == DataTypes.UNSUPPORTED) {

            return dvrCtx -> new GreatestBytesRefEvaluator(
                suppliers.stream()
                    .map(es -> es.get(dvrCtx))
                    .map(ev -> new MvMaxBytesRefEvaluator(ev, dvrCtx))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new),
                dvrCtx
            );
        }
        throw EsqlIllegalArgumentException.illegalDataType(dataType);
    }

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean[] values) {
        for (boolean v : values) {
            if (v) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef")
    static BytesRef process(BytesRef[] values) {
        BytesRef max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = max.compareTo(values[i]) > 0 ? max : values[i];
        }
        return max;
    }

    @Evaluator(extraName = "Int")
    static int process(int[] values) {
        int max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @Evaluator(extraName = "Long")
    static long process(long[] values) {
        long max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @Evaluator(extraName = "Double")
    static double process(double[] values) {
        double max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    // TODO unsigned long
}
