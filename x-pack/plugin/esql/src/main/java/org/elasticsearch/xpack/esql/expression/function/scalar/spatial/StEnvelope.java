/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Determines the minimum bounding rectangle of a geometry.
 * The function `st_envelope` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_ENVELOPE.html">PostGIS:ST_ENVELOPE</a>.
 */
public class StEnvelope extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StEnvelope",
        StEnvelope::new
    );
    private static final SpatialEnvelopeResults<BytesRefBlock.Builder> resultsBuilder = new SpatialEnvelopeResults<>();
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
        this(source, field, false);
    }

    private StEnvelope(Source source, Expression field, boolean useDocValues) {
        super(source, field, useDocValues);
    }

    private StEnvelope(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StEnvelope(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        var resolution = super.resolveType();
        if (resolution.resolved()) {
            this.dataType = switch (spatialField().dataType()) {
                case GEO_POINT, GEO_SHAPE -> GEO_SHAPE;
                case CARTESIAN_POINT, CARTESIAN_SHAPE -> CARTESIAN_SHAPE;
                default -> NULL;
            };
        }
        return resolution;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return switch (spatialField().dataType()) {
                case GEO_POINT -> new StEnvelopeFromDocValuesGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                case CARTESIAN_POINT -> new StEnvelopeFromDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                default -> throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
            };
        }
        if (spatialField().dataType() == GEO_POINT || spatialField().dataType() == DataType.GEO_SHAPE) {
            return new StEnvelopeFromWKBGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
        return new StEnvelopeFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StEnvelope(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StEnvelope::new, spatialField());
    }

    static void buildEnvelopeResults(BytesRefBlock.Builder results, Rectangle rectangle) {
        long encodedMin = CARTESIAN.pointAsLong(rectangle.getMinX(), rectangle.getMinY());
        long encodedMax = CARTESIAN.pointAsLong(rectangle.getMaxX(), rectangle.getMaxY());
        double minX = CARTESIAN.decodeX(encodedMin);
        double minY = CARTESIAN.decodeY(encodedMin);
        double maxX = CARTESIAN.decodeX(encodedMax);
        double maxY = CARTESIAN.decodeY(encodedMax);
        rectangle = new Rectangle(minX, maxX, maxY, minY);
        results.appendBytesRef(UNSPECIFIED.asWkb(rectangle));
    }

    static void buildGeoEnvelopeResults(BytesRefBlock.Builder results, Rectangle rectangle) {
        long encodedMin = GEO.pointAsLong(rectangle.getMinX(), rectangle.getMinY());
        long encodedMax = GEO.pointAsLong(rectangle.getMaxX(), rectangle.getMaxY());
        double minX = GEO.decodeX(encodedMin);
        double minY = GEO.decodeY(encodedMin);
        double maxX = GEO.decodeX(encodedMax);
        double maxY = GEO.decodeY(encodedMax);
        rectangle = new Rectangle(minX, maxX, maxY, minY);
        results.appendBytesRef(UNSPECIFIED.asWkb(rectangle));
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(BytesRefBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        var counter = new SpatialEnvelopeVisitor.CartesianPointVisitor();
        resultsBuilder.fromWellKnownBinary(results, p, wkbBlock, counter, StEnvelope::buildEnvelopeResults);
    }

    @Evaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinaryGeo(BytesRefBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        var counter = new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP);
        resultsBuilder.fromWellKnownBinary(results, p, wkbBlock, counter, StEnvelope::buildGeoEnvelopeResults);
    }

    @Evaluator(extraName = "FromDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValues(BytesRefBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        var counter = new SpatialEnvelopeVisitor.CartesianPointVisitor();
        resultsBuilder.fromDocValues(results, p, encodedBlock, counter, CARTESIAN, StEnvelope::buildEnvelopeResults);
    }

    @Evaluator(extraName = "FromDocValuesGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValuesGeo(BytesRefBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        var counter = new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP);
        resultsBuilder.fromDocValues(results, p, encodedBlock, counter, GEO, StEnvelope::buildEnvelopeResults);
    }
}
