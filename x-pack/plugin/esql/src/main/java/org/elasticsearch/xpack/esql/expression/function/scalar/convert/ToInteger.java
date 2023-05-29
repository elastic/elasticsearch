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

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;

public class ToInteger extends AbstractConvertFunction {

    private static final Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> EVALUATORS =
        Map.of(
            INTEGER,
            (fieldEval, source) -> fieldEval,
            BOOLEAN,
            ToIntegerFromBooleanEvaluator::new,
            DATETIME,
            ToIntegerFromLongEvaluator::new,
            KEYWORD,
            ToIntegerFromStringEvaluator::new,
            DOUBLE,
            ToIntegerFromDoubleEvaluator::new,
            LONG,
            ToIntegerFromLongEvaluator::new
        );

    public ToInteger(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToInteger(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToInteger::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static int fromBoolean(boolean bool) {
        return bool ? 1 : 0;
    }

    @ConvertEvaluator(extraName = "FromString")
    static int fromKeyword(BytesRef in) {
        String asString = in.utf8ToString();
        try {
            return Integer.parseInt(asString);
        } catch (NumberFormatException nfe) {
            try {
                return fromDouble(Double.parseDouble(asString));
            } catch (Exception e) {
                throw nfe;
            }
        }
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static int fromDouble(double dbl) {
        return fromLong(safeDoubleToLong(dbl));
    }

    @ConvertEvaluator(extraName = "FromLong")
    static int fromLong(long lng) {
        return safeToInt(lng);
    }
}
