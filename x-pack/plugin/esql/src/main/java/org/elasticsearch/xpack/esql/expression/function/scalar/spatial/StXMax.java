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
import org.elasticsearch.compute.data.DoubleBlock;
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

import static java.lang.Double.NEGATIVE_INFINITY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Determines the maximum value of the x-coordinate from a geometry.
 * The function `st_xmax` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_XMAX.html">PostGIS:ST_XMAX</a>.
 */
public class StXMax extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StXMax", StXMax::new);
    private static final SpatialEnvelopeResults<DoubleBlock.Builder> resultsBuilder = new SpatialEnvelopeResults<>();

    @FunctionInfo(
        returnType = "double",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Extracts the maximum value of the `x` coordinates from the supplied geometry.\n"
            + "If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the maximum `longitude` value.",
        examples = @Example(file = "spatial_shapes", tag = "st_x_y_min_max"),
        depthOffset = 1  // So this appears as a subsection of ST_ENVELOPE
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
        this(source, field, false);
    }

    private StXMax(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StXMax(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StXMax(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return switch (spatialField().dataType()) {
                case GEO_POINT -> new StXMaxFromDocValuesGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                case CARTESIAN_POINT -> new StXMaxFromDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                default -> throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
            };
        }
        if (spatialField().dataType() == GEO_POINT || spatialField().dataType() == DataType.GEO_SHAPE) {
            return new StXMaxFromWKBGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
        return new StXMaxFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StXMax(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StXMax::new, spatialField());
    }

    static void buildEnvelopeResults(DoubleBlock.Builder results, Rectangle rectangle) {
        results.appendDouble(CARTESIAN.decodeX(CARTESIAN.pointAsLong(rectangle.getMaxX(), 0)));
    }

    static void buildGeoEnvelopeResults(DoubleBlock.Builder results, Rectangle rectangle) {
        results.appendDouble(GEO.decodeX(GEO.pointAsLong(rectangle.getMaxX(), 0)));
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(DoubleBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        var counter = new SpatialEnvelopeVisitor.CartesianPointVisitor();
        resultsBuilder.fromWellKnownBinary(results, p, wkbBlock, counter, StXMax::buildEnvelopeResults);
    }

    @Evaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinaryGeo(DoubleBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        var counter = new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP);
        resultsBuilder.fromWellKnownBinary(results, p, wkbBlock, counter, StXMax::buildGeoEnvelopeResults);
    }

    @Evaluator(extraName = "FromDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValues(DoubleBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        resultsBuilder.fromDocValuesLinear(results, p, encodedBlock, NEGATIVE_INFINITY, (v, e) -> Math.max(v, CARTESIAN.decodeX(e)));
    }

    @Evaluator(extraName = "FromDocValuesGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValuesGeo(DoubleBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        var counter = new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP);
        resultsBuilder.fromDocValues(results, p, encodedBlock, counter, GEO, StXMax::buildGeoEnvelopeResults);
    }
}
