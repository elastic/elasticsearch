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
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Extracts the y-coordinate from a point geometry.
 * For cartesian geometries, the y-coordinate is the second coordinate.
 * For geographic geometries, the y-coordinate is the latitude.
 * The function `st_y` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_Y.html">PostGIS:ST_Y</a>.
 */
public class StY extends UnaryScalarFunction {
    @FunctionInfo(returnType = "double", description = "Extracts the y-coordinate from a point geometry.")
    public StY(Source source, @Param(name = "point", type = { "geo_point", "cartesian_point" }) Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return isSpatialPoint(field(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return new StYFromWKBEvaluator.Factory(toEvaluator.apply(field()), source());
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StY(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StY::new, field());
    }

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromWellKnownBinary(BytesRef in) {
        return UNSPECIFIED.wkbAsPoint(in).getY();
    }
}
