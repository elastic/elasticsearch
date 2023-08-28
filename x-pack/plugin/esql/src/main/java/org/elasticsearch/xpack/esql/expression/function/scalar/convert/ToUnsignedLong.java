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

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.ONE_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.ZERO_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;

public class ToUnsignedLong extends AbstractConvertFunction {

    private static final Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> EVALUATORS =
        Map.of(
            UNSIGNED_LONG,
            (fieldEval, source) -> fieldEval,
            DATETIME,
            ToUnsignedLongFromLongEvaluator::new,
            BOOLEAN,
            ToUnsignedLongFromBooleanEvaluator::new,
            KEYWORD,
            ToUnsignedLongFromStringEvaluator::new,
            DOUBLE,
            ToUnsignedLongFromDoubleEvaluator::new,
            LONG,
            ToUnsignedLongFromLongEvaluator::new,
            INTEGER,
            ToUnsignedLongFromIntEvaluator::new
        );

    public ToUnsignedLong(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return UNSIGNED_LONG;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToUnsignedLong(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToUnsignedLong::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static long fromBoolean(boolean bool) {
        return bool ? ONE_AS_UNSIGNED_LONG : ZERO_AS_UNSIGNED_LONG;
    }

    @ConvertEvaluator(extraName = "FromString")
    static long fromKeyword(BytesRef in) {
        String asString = in.utf8ToString();
        return asLongUnsigned(safeToUnsignedLong(asString));
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static long fromDouble(double dbl) {
        return asLongUnsigned(safeToUnsignedLong(dbl));
    }

    @ConvertEvaluator(extraName = "FromLong")
    static long fromLong(long lng) {
        return asLongUnsigned(safeToUnsignedLong(lng));
    }

    @ConvertEvaluator(extraName = "FromInt")
    static long fromInt(int i) {
        return fromLong(i);
    }
}
