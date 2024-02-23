/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;

public class StX extends UnaryScalarFunction {
    @FunctionInfo(returnType = "double", description = "Extracts the x-coordinate from a point geometry.")
    public StX(Source source, @Param(name = "point", type = { "geo_point", "cartesian_point" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatialPoint(field(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        EvalOperator.ExpressionEvaluator.Factory factory = toEvaluator.apply(field());
        return switch (field().dataType().typeName()) {
            case "geo_point" -> new StXFromGeoPointEvaluator.Factory(factory, source());
            case "cartesian_point" -> new StXFromCartesianPointEvaluator.Factory(factory, source());
            default -> throw new IllegalArgumentException("Invalid data type: " + field().dataType());
        };
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StX(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StX::new, field());
    }

    @ConvertEvaluator(extraName = "FromGeoPoint", warnExceptions = { IllegalArgumentException.class })
    static double fromGeoPoint(BytesRef in) {
        return GEO.wkbAsPoint(in).getX();
    }

    @ConvertEvaluator(extraName = "FromCartesianPoint", warnExceptions = { IllegalArgumentException.class })
    static double fromCartesianPoint(BytesRef in) {
        return CARTESIAN.wkbAsPoint(in).getX();
    }
}
