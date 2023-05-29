/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;

public class ToBoolean extends AbstractConvertFunction {

    private static final Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> EVALUATORS =
        Map.of(
            BOOLEAN,
            (fieldEval, source) -> fieldEval,
            KEYWORD,
            ToBooleanFromStringEvaluator::new,
            DOUBLE,
            ToBooleanFromDoubleEvaluator::new,
            LONG,
            ToBooleanFromLongEvaluator::new,
            INTEGER,
            ToBooleanFromIntEvaluator::new
        );

    public ToBoolean(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToBoolean(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToBoolean::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static boolean fromKeyword(BytesRef keyword) {
        return Boolean.parseBoolean(keyword.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static boolean fromDouble(double d) {
        return d != 0;
    }

    @ConvertEvaluator(extraName = "FromLong")
    static boolean fromLong(long l) {
        return l != 0;
    }

    @ConvertEvaluator(extraName = "FromInt")
    static boolean fromInt(int i) {
        return fromLong(i);
    }
}
