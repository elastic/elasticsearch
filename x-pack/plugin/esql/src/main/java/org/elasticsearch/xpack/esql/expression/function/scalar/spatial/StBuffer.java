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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

public class StBuffer extends SpatialDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StBuffer", StBuffer::new);
    private static final SpatialGeometryBlockProcessor processor = new SpatialGeometryBlockProcessor(UNSPECIFIED, StBuffer::bufferOp);
    private static final SpatialGeometryBlockProcessor geoProcessor = new SpatialGeometryBlockProcessor(GEO, StBuffer::bufferOp);
    private static final SpatialGeometryBlockProcessor cartesianProcessor = new SpatialGeometryBlockProcessor(
        CARTESIAN,
        StBuffer::bufferOp
    );

    /**
     * Wraps {@link BufferOp#bufferOp} to return the original geometry when distance is zero,
     * matching PostGIS behavior. JTS returns POLYGON EMPTY for points/lines with zero distance,
     * but the expected behavior is to return the original geometry unchanged.
     */
    private static Geometry bufferOp(Geometry geometry, double distance) {
        if (distance == 0) {
            return geometry;
        }
        return BufferOp.bufferOp(geometry, distance);
    }

    private final Expression geometry;
    private final Expression distance;

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        description = "Computes a buffer area around the input geometry at the specified distance. "
            + "The distance is in the units of the input spatial reference system. "
            + "Positive distances expand the geometry, negative distances shrink it. "
            + "Points and lines become polygons when buffered.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        examples = @Example(file = "spatial-jts", tag = "st_buffer"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StBuffer(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression geometry,
        @Param(
            name = "distance",
            type = { "double", "float", "long", "integer" },
            description = "Buffer distance in the units of the input spatial reference system"
        ) Expression distance
    ) {
        this(source, geometry, distance, false);
    }

    private StBuffer(Source source, Expression geometry, Expression distance, boolean spatialDocValues) {
        super(source, List.of(geometry, distance), spatialDocValues);
        this.geometry = geometry;
        this.distance = distance;
    }

    private StBuffer(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        if (distance.dataType() == DataType.NULL || geometry.dataType() == DataType.NULL) {
            return DataType.NULL;
        }
        return DataType.isSpatialGeo(geometry.dataType()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StBuffer(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StBuffer::new, geometry, distance);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(geometry);
        out.writeNamedWriteable(distance);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StBuffer(source(), geometry, distance, useDocValues);
    }

    @Override
    public Expression spatialField() {
        return geometry;
    }

    Expression distance() {
        return distance;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        TypeResolution spatialResolved = isSpatial(geometry, sourceText(), FIRST);
        if (spatialResolved.unresolved()) {
            return spatialResolved;
        }
        return isType(
            distance,
            t -> t == DOUBLE || t == FLOAT || t == LONG || t == INTEGER,
            sourceText(),
            SECOND,
            "double",
            "float",
            "long",
            "integer"
        );
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory geometryEvaluator = toEvaluator.apply(geometry);

        if (distance.foldable() == false) {
            throw new IllegalArgumentException("distance must be foldable");
        }
        var distanceExpression = distance.fold(toEvaluator.foldCtx());
        double inputDistance = getInputDistance(distanceExpression);
        if (spatialDocValues && geometry.dataType() == DataType.GEO_POINT) {
            return new StBufferNonFoldableGeoPointDocValuesAndFoldableDistanceEvaluator.Factory(source(), geometryEvaluator, inputDistance);
        } else if (spatialDocValues && geometry.dataType() == DataType.CARTESIAN_POINT) {
            return new StBufferNonFoldableCartesianPointDocValuesAndFoldableDistanceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputDistance
            );
        }
        return new StBufferNonFoldableGeometryAndFoldableDistanceEvaluator.Factory(source(), geometryEvaluator, inputDistance);
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && distance.foldable();
    }

    @Override
    public Object fold(FoldContext foldCtx) {
        var distanceExpression = distance.fold(foldCtx);
        double inputDistance = getInputDistance(distanceExpression);
        Object input = geometry.fold(foldCtx);
        return switch (input) {
            case null -> null;
            case List<?> list -> processor.processSingleGeometry(processor.asJtsGeometry(list), inputDistance);
            case BytesRef inputGeometry -> processor.processSingleGeometry(inputGeometry, inputDistance);
            default -> throw new IllegalArgumentException("unsupported block type: " + input.getClass().getSimpleName());
        };
    }

    @Evaluator(extraName = "NonFoldableGeometryAndFoldableDistance", warnExceptions = { IllegalArgumentException.class })
    static void processNonFoldableGeometryAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double distance
    ) {
        processor.processGeometries(builder, p, geometry, distance);
    }

    @Evaluator(
        extraName = "NonFoldableGeoPointDocValuesAndFoldableDistance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processGeoPointDocValuesAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock point,
        @Fixed double distance
    ) throws IOException {
        geoProcessor.processPoints(builder, p, point, distance);
    }

    static double getInputDistance(Object distanceExpression) {
        if (distanceExpression instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalArgumentException("distance for st_buffer must be an integer or floating-point number");
    }

    @Evaluator(
        extraName = "NonFoldableCartesianPointDocValuesAndFoldableDistance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processCartesianPointDocValuesAndConstantDistance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock left,
        @Fixed double distance
    ) throws IOException {
        cartesianProcessor.processPoints(builder, p, left, distance);
    }
}
