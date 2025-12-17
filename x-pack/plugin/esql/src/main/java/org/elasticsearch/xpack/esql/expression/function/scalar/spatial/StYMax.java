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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Determines the maximum value of the y-coordinate from a geometry.
 * The function `st_ymax` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_YMAX.html">PostGIS:ST_YMAX</a>.
 */
public class StYMax extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StYMax", StYMax::new);

    @FunctionInfo(
        returnType = "double",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = "Extracts the maximum value of the `y` coordinates from the supplied geometry.\n"
            + "If the geometry is of type `geo_point` or `geo_shape` this is equivalent to extracting the maximum `latitude` value.",
        examples = @Example(file = "spatial_shapes", tag = "st_x_y_min_max"),
        depthOffset = 1  // So this appears as a subsection of ST_ENVELOPE
    )
    public StYMax(
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

    private StYMax(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StYMax(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StYMax(source(), spatialField(), useDocValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (spatialDocValues) {
            return switch (spatialField().dataType()) {
                case GEO_POINT -> new StYMaxFromDocValuesGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                case CARTESIAN_POINT -> new StYMaxFromDocValuesEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
                default -> throw new IllegalArgumentException("Cannot use doc values for type " + spatialField().dataType());
            };
        }
        if (spatialField().dataType() == GEO_POINT || spatialField().dataType() == DataType.GEO_SHAPE) {
            return new StYMaxFromWKBGeoEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
        }
        return new StYMaxFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StYMax(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StYMax::new, spatialField());
    }

    private static void fromWellKnownBinary(
        DoubleBlock.Builder results,
        @Position int p,
        BytesRefBlock wkbBlock,
        SpatialEnvelopeVisitor.PointVisitor pointVisitor
    ) {
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        BytesRef scratch = new BytesRef();
        var visitor = new SpatialEnvelopeVisitor(pointVisitor);
        for (int i = 0; i < valueCount; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            var geometry = UNSPECIFIED.wkbToGeometry(wkb);
            geometry.visit(visitor);
        }
        if (pointVisitor.isValid()) {
            results.appendDouble(visitor.getResult().getMaxY());
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(DoubleBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        fromWellKnownBinary(results, p, wkbBlock, new SpatialEnvelopeVisitor.CartesianPointVisitor());
    }

    @Evaluator(extraName = "FromWKBGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinaryGeo(DoubleBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        fromWellKnownBinary(results, p, wkbBlock, new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP));
    }

    private static void fromDocValues(
        DoubleBlock.Builder results,
        @Position int p,
        LongBlock encodedBlock,
        SpatialCoordinateTypes spatialCoordinateType
    ) {
        int firstValueIndex = encodedBlock.getFirstValueIndex(p);
        int valueCount = encodedBlock.getValueCount(p);
        if (valueCount == 0) {
            results.appendNull();
            return;
        }
        double maxY = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < valueCount; i++) {
            long encoded = encodedBlock.getLong(firstValueIndex + i);
            maxY = Math.max(maxY, spatialCoordinateType.decodeY(encoded));
        }
        if (Double.isFinite(maxY)) {
            results.appendDouble(maxY);
            return;
        }
        throw new IllegalArgumentException("Cannot determine envelope of geometry");
    }

    @Evaluator(extraName = "FromDocValues", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValues(DoubleBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        fromDocValues(results, p, encodedBlock, SpatialCoordinateTypes.CARTESIAN);
    }

    @Evaluator(extraName = "FromDocValuesGeo", warnExceptions = { IllegalArgumentException.class })
    static void fromDocValuesGeo(DoubleBlock.Builder results, @Position int p, LongBlock encodedBlock) {
        fromDocValues(results, p, encodedBlock, SpatialCoordinateTypes.GEO);
    }
}
