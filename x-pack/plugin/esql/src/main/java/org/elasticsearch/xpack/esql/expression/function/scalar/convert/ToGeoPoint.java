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

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.SpatialUtils.geoPointAsLong;
import static org.elasticsearch.xpack.ql.util.SpatialUtils.stringAsGeoPoint;

public class ToGeoPoint extends AbstractConvertFunction {

    private static final Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> EVALUATORS =
        Map.of(
            GEO_POINT,
            (fieldEval, source) -> fieldEval,
            LONG,
            (fieldEval, source) -> fieldEval,
            UNSIGNED_LONG,
            (fieldEval, source) -> fieldEval,
            KEYWORD,
            ToGeoPointFromStringEvaluator::new,
            TEXT,
            ToGeoPointFromStringEvaluator::new
        );

    public ToGeoPoint(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return GEO_POINT;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToGeoPoint(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToGeoPoint::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static long fromKeyword(BytesRef in) {
        return geoPointAsLong(stringAsGeoPoint(in.utf8ToString()));
    }
}
