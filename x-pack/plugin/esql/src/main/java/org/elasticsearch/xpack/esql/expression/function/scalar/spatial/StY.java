/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;

/**
 * Extracts the y-coordinate from a point geometry.
 * For cartesian geometries, the y-coordinate is the second coordinate.
 * For geographic geometries, the y-coordinate is the latitude.
 * The function `st_y` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_Y.html">PostGIS:ST_Y</a>.
 */
public class StY extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StY", StY::new);

    @FunctionInfo(
        returnType = "double",
        description = "Extracts the `y` coordinate from the supplied point.\n"
            + "If the points is of type `geo_point` this is equivalent to extracting the `latitude` value.",
        examples = @Example(file = "spatial", tag = "st_x_y"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StY(
        Source source,
        @Param(
            name = "point",
            type = { "geo_point", "cartesian_point" },
            description = "Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`."
        ) Expression field
    ) {
        this(source, field, false);
    }

    private StY(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StY(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StY(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatialPoint(spatialField(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return switch (spatialField().dataType()) {
                case GEO_POINT -> new StYFromGeoDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                case CARTESIAN_POINT -> new StYFromCartesianDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                default -> throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
            };
        }
        return new StYFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StY(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StY::new, spatialField());
    }

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromWellKnownBinary(BytesRef in) {
        return UNSPECIFIED.wkbAsPoint(in).getY();
    }

    @ConvertEvaluator(extraName = "FromCartesianDocValues", warnExceptions = { IllegalArgumentException.class })
    static double fromCartesianDocValues(long encoded) {
        return CARTESIAN.decodeY(encoded);
    }

    @ConvertEvaluator(extraName = "FromGeoDocValues", warnExceptions = { IllegalArgumentException.class })
    static double fromGeoDocValues(long encoded) {
        return GEO.decodeY(encoded);
    }
}
