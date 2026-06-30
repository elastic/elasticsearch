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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class StUnion extends BinarySpatialGeometryFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StUnion", StUnion::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(StUnion.class).binary(StUnion::new).name("st_union");

    private static final SpatialBinaryGeometryBlockProcessor unspecifiedProcessor = new SpatialBinaryGeometryBlockProcessor(
        UNSPECIFIED,
        Geometry::union
    );
    private static final SpatialBinaryGeometryBlockProcessor geoProcessor = new SpatialBinaryGeometryBlockProcessor(GEO, Geometry::union);
    private static final SpatialBinaryGeometryBlockProcessor cartesianProcessor = new SpatialBinaryGeometryBlockProcessor(
        CARTESIAN,
        Geometry::union
    );

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        briefSummary = "Returns the geometric union of two geometries.",
        description = "Returns the geometric union of two geometries. "
            + "The result is a geometry that covers all points covered by either input geometry. "
            + "Both geometries must share the same coordinate reference system.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        examples = @Example(file = "spatial-jts", tag = "st_union"),
        depthOffset = 1
    )
    public StUnion(
        Source source,
        @Param(
            name = "geomA",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression left,
        @Param(
            name = "geomB",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "Must share the same coordinate reference system as the first parameter. "
                + "If `null`, the function returns `null`."
        ) Expression right
    ) {
        this(source, left, right, false, false);
    }

    private StUnion(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues);
    }

    private StUnion(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StUnion(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StUnion::new, left, right);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public BinarySpatialGeometryFunction withDocValues(boolean foundLeft, boolean foundRight) {
        return new StUnion(source(), left, right, leftDocValues || foundLeft, rightDocValues || foundRight);
    }

    @Override
    protected Geometry jtsOperation(Geometry leftGeom, Geometry rightGeom) {
        return leftGeom.union(rightGeom);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory leftEval = toEvaluator.apply(left);
        ExpressionEvaluator.Factory rightEval = toEvaluator.apply(right);
        if (leftDocValues && rightDocValues) {
            if (DataType.isSpatialGeo(left.dataType())) {
                return new StUnionBothGeoPointDocValuesEvaluator.Factory(source(), leftEval, rightEval);
            } else {
                return new StUnionBothCartesianPointDocValuesEvaluator.Factory(source(), leftEval, rightEval);
            }
        } else if (leftDocValues && left.dataType() == DataType.GEO_POINT) {
            return new StUnionSourceAndGeoPointDocValuesEvaluator.Factory(source(), rightEval, leftEval);
        } else if (leftDocValues && left.dataType() == DataType.CARTESIAN_POINT) {
            return new StUnionSourceAndCartesianPointDocValuesEvaluator.Factory(source(), rightEval, leftEval);
        } else if (rightDocValues && right.dataType() == DataType.GEO_POINT) {
            return new StUnionSourceAndGeoPointDocValuesEvaluator.Factory(source(), leftEval, rightEval);
        } else if (rightDocValues && right.dataType() == DataType.CARTESIAN_POINT) {
            return new StUnionSourceAndCartesianPointDocValuesEvaluator.Factory(source(), leftEval, rightEval);
        } else {
            return new StUnionSourceAndSourceEvaluator.Factory(source(), leftEval, rightEval);
        }
    }

    @Evaluator(extraName = "SourceAndSource", warnExceptions = { IllegalArgumentException.class })
    static void processSourceAndSource(BytesRefBlock.Builder builder, @Position int p, BytesRefBlock left, BytesRefBlock right) {
        unspecifiedProcessor.processSourceAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "SourceAndGeoPointDocValues", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processSourceAndGeoPointDocValues(BytesRefBlock.Builder builder, @Position int p, BytesRefBlock left, LongBlock right)
        throws IOException {
        geoProcessor.processSourceAndDocValues(builder, p, left, right);
    }

    @Evaluator(extraName = "SourceAndCartesianPointDocValues", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processSourceAndCartesianPointDocValues(BytesRefBlock.Builder builder, @Position int p, BytesRefBlock left, LongBlock right)
        throws IOException {
        cartesianProcessor.processSourceAndDocValues(builder, p, left, right);
    }

    @Evaluator(extraName = "BothGeoPointDocValues", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processBothGeoPointDocValues(BytesRefBlock.Builder builder, @Position int p, LongBlock left, LongBlock right)
        throws IOException {
        geoProcessor.processBothDocValues(builder, p, left, right);
    }

    @Evaluator(extraName = "BothCartesianPointDocValues", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processBothCartesianPointDocValues(BytesRefBlock.Builder builder, @Position int p, LongBlock left, LongBlock right)
        throws IOException {
        cartesianProcessor.processBothDocValues(builder, p, left, right);
    }
}
