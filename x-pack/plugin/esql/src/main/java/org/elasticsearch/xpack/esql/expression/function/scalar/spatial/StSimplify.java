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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class StSimplify extends SpatialDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StSimplify",
        StSimplify::new
    );
    private static final BlockProcessor processor = new BlockProcessor(UNSPECIFIED);
    private static final BlockProcessor geoProcessor = new BlockProcessor(GEO);
    private static final BlockProcessor cartesianProcessor = new BlockProcessor(CARTESIAN);
    private final Expression geometry;
    private final Expression tolerance;

    @FunctionInfo(
        returnType = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
        description = "Simplifies the input geometry by applying the Douglas-Peucker algorithm with a specified tolerance. "
            + "Vertices that fall within the tolerance distance from the simplified shape are removed. "
            + "Note that the resulting geometry may be invalid, even if the original input was valid.",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        examples = @Example(file = "spatial-jts", tag = "st_simplify")
    )
    public StSimplify(
        Source source,
        @Param(
            name = "geometry",
            type = { "geo_point", "geo_shape", "cartesian_point", "cartesian_shape" },
            description = "Expression of type `geo_point`, `geo_shape`, `cartesian_point` or `cartesian_shape`. "
                + "If `null`, the function returns `null`."
        ) Expression geometry,
        @Param(
            name = "tolerance",
            type = { "double" },
            description = "Tolerance for the geometry simplification, in the units of the input SRS"
        ) Expression tolerance
    ) {
        this(source, geometry, tolerance, false);
    }

    private StSimplify(Source source, Expression geometry, Expression tolerance, boolean spatialDocValues) {
        super(source, List.of(geometry, tolerance), spatialDocValues);
        this.geometry = geometry;
        this.tolerance = tolerance;
    }

    private StSimplify(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        return tolerance.dataType() == DataType.NULL ? DataType.NULL : geometry.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StSimplify(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StSimplify::new, geometry, tolerance);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(geometry);
        out.writeNamedWriteable(tolerance);
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StSimplify(source(), geometry, tolerance, true);
    }

    @Override
    public Expression spatialField() {
        return geometry;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        EvalOperator.ExpressionEvaluator.Factory geometryEvaluator = toEvaluator.apply(geometry);

        if (tolerance.foldable() == false) {
            throw new IllegalArgumentException("tolerance must be foldable");
        }
        var toleranceExpression = tolerance.fold(toEvaluator.foldCtx());
        double inputTolerance = getInputTolerance(toleranceExpression);
        if (spatialDocValues && geometry.dataType() == DataType.GEO_POINT) {
            return new StSimplifyNonFoldableGeoPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        } else if (spatialDocValues && geometry.dataType() == DataType.CARTESIAN_POINT) {
            return new StSimplifyNonFoldableCartesianPointDocValuesAndFoldableToleranceEvaluator.Factory(
                source(),
                geometryEvaluator,
                inputTolerance
            );
        }
        return new StSimplifyNonFoldableGeometryAndFoldableToleranceEvaluator.Factory(source(), geometryEvaluator, inputTolerance);
    }

    @Override
    public boolean foldable() {
        return geometry.foldable() && tolerance.foldable();
    }

    @Override
    public Object fold(FoldContext foldCtx) {
        var toleranceExpression = tolerance.fold(foldCtx);
        double inputTolerance = getInputTolerance(toleranceExpression);
        Object input = geometry.fold(foldCtx);
        if (input instanceof List<?> list) {
            // TODO: Consider if this should compact to a GeometryCollection instead, which is what we do for fields
            ArrayList<BytesRef> results = new ArrayList<>(list.size());
            for (Object o : list) {
                if (o instanceof BytesRef inputGeometry) {
                    results.add(BlockProcessor.geoSourceAndConstantTolerance(inputGeometry, inputTolerance));
                } else {
                    throw new IllegalArgumentException("unsupported list element type: " + o.getClass().getSimpleName());
                }
            }
            return results;
        } else if (input instanceof BytesRef inputGeometry) {
            return BlockProcessor.geoSourceAndConstantTolerance(inputGeometry, inputTolerance);
        } else {
            throw new IllegalArgumentException("unsupported block type: " + input.getClass().getSimpleName());
        }
    }

    @Evaluator(extraName = "NonFoldableGeometryAndFoldableTolerance", warnExceptions = { IllegalArgumentException.class })
    static void processNonFoldableGeometryAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        BytesRefBlock geometry,
        @Fixed double tolerance
    ) {
        processor.processGeometries(builder, p, geometry, tolerance);
    }

    @Evaluator(
        extraName = "NonFoldableGeoPointDocValuesAndFoldableTolerance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processGeoPointDocValuesAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock point,
        @Fixed double tolerance
    ) throws IOException {
        geoProcessor.processPoints(builder, p, point, tolerance);
    }

    private static double getInputTolerance(Object toleranceExpression) {
        double inputTolerance;

        if (toleranceExpression instanceof Number number) {
            inputTolerance = number.doubleValue();
        } else {
            throw new IllegalArgumentException("tolerance for st_simplify must be an integer or floating-point number");
        }

        if (inputTolerance < 0) {
            throw new IllegalArgumentException("tolerance must not be negative");
        }
        return inputTolerance;
    }

    @Evaluator(
        extraName = "NonFoldableCartesianPointDocValuesAndFoldableTolerance",
        warnExceptions = { IllegalArgumentException.class, IOException.class }
    )
    static void processCartesianPointDocValuesAndConstantTolerance(
        BytesRefBlock.Builder builder,
        @Position int p,
        LongBlock left,
        @Fixed double tolerance
    ) throws IOException {
        cartesianProcessor.processPoints(builder, p, left, tolerance);
    }

    private static class BlockProcessor {
        private final SpatialCoordinateTypes spatialCoordinateType;
        private final GeometryFactory geometryFactory = new GeometryFactory();

        BlockProcessor(SpatialCoordinateTypes spatialCoordinateType) {
            this.spatialCoordinateType = spatialCoordinateType;
        }

        private static BytesRef geoSourceAndConstantTolerance(BytesRef inputGeometry, double inputTolerance) {
            if (inputGeometry == null) {
                return null;
            }
            try {
                Geometry jtsGeometry = UNSPECIFIED.wkbToJtsGeometry(inputGeometry);
                Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, inputTolerance);
                return UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry);
            } catch (ParseException e) {
                throw new IllegalArgumentException("could not parse the geometry expression: " + e);
            }
        }

        void processPoints(BytesRefBlock.Builder builder, int p, LongBlock left, double tolerance) throws IOException {
            if (left.getValueCount(p) < 1) {
                builder.appendNull();
            } else {
                final Geometry jtsGeometry = asJtsMultiPoint(left, p, spatialCoordinateType::longAsPoint);
                Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, tolerance);
                builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry));
            }
        }

        void processGeometries(BytesRefBlock.Builder builder, int p, BytesRefBlock left, double tolerance) {
            if (left.getValueCount(p) < 1) {
                builder.appendNull();
            } else {
                final Geometry jtsGeometry = asJtsGeometry(left, p);
                Geometry simplifiedGeometry = DouglasPeuckerSimplifier.simplify(jtsGeometry, tolerance);
                builder.appendBytesRef(UNSPECIFIED.jtsGeometryToWkb(simplifiedGeometry));
            }
        }

        Geometry asJtsMultiPoint(LongBlock valueBlock, int position, Function<Long, Point> decoder) {
            final int firstValueIndex = valueBlock.getFirstValueIndex(position);
            final int valueCount = valueBlock.getValueCount(position);
            if (valueCount == 1) {
                Point point = decoder.apply(valueBlock.getLong(firstValueIndex));
                return geometryFactory.createPoint(new Coordinate(point.getX(), point.getY()));
            }
            final Coordinate[] coordinates = new Coordinate[valueCount];
            for (int i = 0; i < valueCount; i++) {
                Point point = decoder.apply(valueBlock.getLong(firstValueIndex + i));
                coordinates[i] = new Coordinate(point.getX(), point.getY());
            }
            return geometryFactory.createMultiPointFromCoords(coordinates);
        }

        Geometry asJtsGeometry(BytesRefBlock valueBlock, int position) {
            try {
                final int firstValueIndex = valueBlock.getFirstValueIndex(position);
                final int valueCount = valueBlock.getValueCount(position);
                BytesRef scratch = new BytesRef();
                if (valueCount == 1) {
                    return UNSPECIFIED.wkbToJtsGeometry(valueBlock.getBytesRef(firstValueIndex, scratch));
                }
                final Geometry[] geometries = new Geometry[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    BytesRef wkb = valueBlock.getBytesRef(firstValueIndex + i, scratch);
                    geometries[i] = UNSPECIFIED.wkbToJtsGeometry(valueBlock.getBytesRef(firstValueIndex, scratch));
                }
                return geometryFactory.createGeometryCollection(geometries);
            } catch (ParseException e) {
                throw new IllegalArgumentException("could not parse the geometry expression: " + e);
            }
        }
    }
}
