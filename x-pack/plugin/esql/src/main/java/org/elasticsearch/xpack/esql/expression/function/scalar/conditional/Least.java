/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
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
 * Returns the minimum value of multiple columns.
 */
public class Least extends ScalarFunction implements EvaluatorMapper, OptionalArgument {
    private DataType dataType;

    public Least(
        Source source,
        @Param(name = "first", type = { "integer", "long", "double", "boolean", "keyword", "text", "ip", "version" }) Expression first,
        @Param(name = "rest", type = { "integer", "long", "double", "boolean", "keyword", "text", "ip", "version" }) List<Expression> rest
    ) {
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
        return new Least(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Least::new, children().get(0), children().subList(1, children().size()));
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
        ExpressionEvaluator.Factory[] factories = children().stream()
            .map(e -> toEvaluator.apply(new MvMin(e.source(), e)))
            .toArray(ExpressionEvaluator.Factory[]::new);
        if (dataType == DataTypes.BOOLEAN) {
            return new LeastBooleanEvaluator.Factory(factories);
        }
        if (dataType == DataTypes.DOUBLE) {
            return new LeastDoubleEvaluator.Factory(factories);
        }
        if (dataType == DataTypes.INTEGER) {
            return new LeastIntEvaluator.Factory(factories);
        }
        if (dataType == DataTypes.LONG) {
            return new LeastLongEvaluator.Factory(factories);
        }
        if (dataType == DataTypes.KEYWORD
            || dataType == DataTypes.TEXT
            || dataType == DataTypes.IP
            || dataType == DataTypes.VERSION
            || dataType == DataTypes.UNSUPPORTED) {

            return new LeastBytesRefEvaluator.Factory(factories);
        }
        throw EsqlIllegalArgumentException.illegalDataType(dataType);
    }

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean[] values) {
        for (boolean v : values) {
            if (v == false) {
                return false;
            }
        }
        return true;
    }

    @Evaluator(extraName = "BytesRef")
    static BytesRef process(BytesRef[] values) {
        BytesRef min = values[0];
        for (int i = 1; i < values.length; i++) {
            min = min.compareTo(values[i]) < 0 ? min : values[i];
        }
        return min;
    }

    @Evaluator(extraName = "Int")
    static int process(int[] values) {
        int min = values[0];
        for (int i = 1; i < values.length; i++) {
            min = Math.min(min, values[i]);
        }
        return min;
    }

    @Evaluator(extraName = "Long")
    static long process(long[] values) {
        long min = values[0];
        for (int i = 1; i < values.length; i++) {
            min = Math.min(min, values[i]);
        }
        return min;
    }

    @Evaluator(extraName = "Double")
    static double process(double[] values) {
        double min = values[0];
        for (int i = 1; i < values.length; i++) {
            min = Math.min(min, values[i]);
        }
        return min;
    }
}
