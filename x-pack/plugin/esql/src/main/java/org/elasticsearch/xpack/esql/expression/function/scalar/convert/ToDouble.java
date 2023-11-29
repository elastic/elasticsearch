/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public class ToDouble extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DOUBLE, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToDoubleFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToDoubleFromLongEvaluator.Factory::new), // CastLongToDoubleEvaluator would be a candidate, but not MV'd
        Map.entry(KEYWORD, ToDoubleFromStringEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToDoubleFromUnsignedLongEvaluator.Factory::new),
        Map.entry(LONG, ToDoubleFromLongEvaluator.Factory::new), // CastLongToDoubleEvaluator would be a candidate, but not MV'd
        Map.entry(INTEGER, ToDoubleFromIntEvaluator.Factory::new) // CastIntToDoubleEvaluator would be a candidate, but not MV'd
    );

    public ToDouble(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDouble(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDouble::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static double fromBoolean(boolean bool) {
        return bool ? 1d : 0d;
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { NumberFormatException.class })
    static double fromKeyword(BytesRef in) {
        return Double.parseDouble(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong")
    static double fromUnsignedLong(long l) {
        return unsignedLongAsNumber(l).doubleValue();
    }

    @ConvertEvaluator(extraName = "FromLong")
    static double fromLong(long l) {
        return l;
    }

    @ConvertEvaluator(extraName = "FromInt")
    static double fromInt(int i) {
        return i;
    }
}
