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
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * Determines the minimum bounding rectangle of a geometry.
 * The function `st_envelope` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_ENVELOPE.html">PostGIS:ST_ENVELOPE</a>.
 */
public class StEnvelope extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StEnvelope",
        StEnvelope::new
    );
    private DataType dataType;

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Determines the minimum bounding box of the supplied geometry.",
        examples = @Example(file = "spatial_shapes", tag = "st_envelope")
    )
    public StEnvelope(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
    }

    private StEnvelope(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        var resolution = isSpatial(field(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
        if (resolution.resolved()) {
            this.dataType = switch (field().dataType()) {
                case GEO_POINT, GEO_SHAPE -> GEO_SHAPE;
                case CARTESIAN_POINT, CARTESIAN_SHAPE -> CARTESIAN_SHAPE;
                default -> NULL;
            };
        }
        return resolution;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (field().dataType() == GEO_POINT || field().dataType() == DataType.GEO_SHAPE) {
            return new StEnvelopeFromWKBGeoEvaluator.Factory(source(), toEvaluator.apply(field()));
        }
        return new StEnvelopeFromWKBEvaluator.Factory(source(), toEvaluator.apply(field()));
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StEnvelope(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StEnvelope::new, field());
    }

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromWellKnownBinary(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point) {
            return wkb;
        }
        var envelope = SpatialEnvelopeVisitor.visitCartesian(geometry);
        if (envelope.isPresent()) {
            return UNSPECIFIED.asWkb(envelope.get());
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @ConvertEvaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromWellKnownBinaryGeo(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point) {
            return wkb;
        }
        var envelope = SpatialEnvelopeVisitor.visitGeo(geometry, WrapLongitude.WRAP);
        if (envelope.isPresent()) {
            return UNSPECIFIED.asWkb(envelope.get());
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }
}
