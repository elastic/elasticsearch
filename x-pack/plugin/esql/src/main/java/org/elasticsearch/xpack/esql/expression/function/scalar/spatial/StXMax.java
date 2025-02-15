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
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * Determines the maximum value of the x-coordinate from a geometry.
 * The function `st_xmax` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_XMAX.html">PostGIS:ST_XMAX</a>.
 */
public class StXMax extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StXMax", StXMax::new);

    @FunctionInfo(
        returnType = "double",
        description = "Extracts the maximum value of the `x` coordinates from the supplied geometry.\n"
            + "If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the maximum `longitude` value.",
        examples = @Example(file = "spatial_shapes", tag = "st_x_y_min_max")
    )
    public StXMax(
        Source source,
        @Param(
            name = "point",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
    }

    private StXMax(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (field().dataType() == GEO_POINT || field().dataType() == DataType.GEO_SHAPE) {
            return new StXMaxFromWKBGeoEvaluator.Factory(toEvaluator.apply(field()), source());
        }
        return new StXMaxFromWKBEvaluator.Factory(toEvaluator.apply(field()), source());
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StXMax(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StXMax::new, field());
    }

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromWellKnownBinary(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return point.getX();
        }
        var envelope = SpatialEnvelopeVisitor.visitCartesian(geometry);
        if (envelope.isPresent()) {
            return envelope.get().getMaxX();
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @ConvertEvaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static double fromWellKnownBinaryGeo(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return point.getX();
        }
        var envelope = SpatialEnvelopeVisitor.visitGeo(geometry, WrapLongitude.WRAP);
        if (envelope.isPresent()) {
            return envelope.get().getMaxX();
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }
}
