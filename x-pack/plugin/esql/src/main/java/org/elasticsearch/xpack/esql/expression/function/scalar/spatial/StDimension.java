/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
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

import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Returns the topological dimension of the geometry.
 * Points and multi-points have dimension 0, lines and multi-lines have dimension 1,
 * polygons and multi-polygons have dimension 2, and geometry collections return the
 * maximum dimension of their components.
 * The function {@code st_dimension} is defined in the
 * <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively, it is well described in PostGIS documentation at
 * <a href="https://postgis.net/docs/ST_Dimension.html">PostGIS:ST_Dimension</a>.
 */
public class StDimension extends SpatialUnaryDocValuesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StDimension",
        StDimension::new
    );

    private static final GeometryDimensionVisitor DIMENSION_VISITOR = new GeometryDimensionVisitor();

    @FunctionInfo(
        returnType = "integer",
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.4.0") },
        description = "Returns the topological dimension of the supplied geometry.\n"
            + "Points and multi-points return `0`, lines and multi-lines return `1`, "
            + "polygons and multi-polygons return `2`, and geometry collections return the maximum dimension of their components.",
        examples = @Example(file = "spatial_shapes", tag = "st_dimension"),
        depthOffset = 1  // So this appears as a subsection of geometry functions
    )
    public StDimension(
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

    private StDimension(Source source, Expression field, boolean spatialDocValues) {
        super(source, field, spatialDocValues);
    }

    private StDimension(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (DataType.isSpatialPoint(spatialField().dataType())) {
            // Points always have dimension 0, so return a constant
            return new ConstantPointDimensionEvaluator.Factory(toEvaluator.apply(spatialField()));
        }
        return new StDimensionFromWKBEvaluator.Factory(source(), toEvaluator.apply(spatialField()));
    }

    @Override
    public SpatialDocValuesFunction withDocValues(boolean useDocValues) {
        return new StDimension(source(), spatialField(), useDocValues);
    }

    @Override
    public DataType dataType() {
        return INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StDimension(source(), newChildren.getFirst(), spatialDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StDimension::new, spatialField());
    }

    @Evaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static void fromWellKnownBinary(IntBlock.Builder results, @Position int p, BytesRefBlock wkbBlock) {
        BytesRef scratch = new BytesRef();
        int firstValueIndex = wkbBlock.getFirstValueIndex(p);
        int valueCount = wkbBlock.getValueCount(p);
        int maxDimension = 0;
        for (int i = 0; i < valueCount && maxDimension < 2; i++) {
            BytesRef wkb = wkbBlock.getBytesRef(firstValueIndex + i, scratch);
            Geometry geometry = UNSPECIFIED.wkbToGeometry(wkb);
            int dimension = geometry.visit(DIMENSION_VISITOR);
            maxDimension = Math.max(maxDimension, dimension);
        }
        results.appendInt(maxDimension);
    }

    /**
     * Evaluator that returns a constant integer for every non-null position.
     * Used for point types where the dimension is always 0.
     */
    static class ConstantPointDimensionEvaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantPointDimensionEvaluator.class);
        private final DriverContext context;
        private final ExpressionEvaluator field;

        ConstantPointDimensionEvaluator(DriverContext context, ExpressionEvaluator field) {
            this.context = context;
            this.field = field;
        }

        @Override
        public Block eval(Page page) {
            try (Block fieldBlock = field.eval(page)) {
                int positionCount = page.getPositionCount();
                if (fieldBlock.areAllValuesNull()) {
                    return context.blockFactory().newConstantNullBlock(positionCount);
                }
                if (fieldBlock.asVector() != null) {
                    // No nulls, return constant
                    return context.blockFactory().newConstantIntBlockWith(0, positionCount);
                }
                try (IntBlock.Builder result = context.blockFactory().newIntBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        if (fieldBlock.isNull(p)) {
                            result.appendNull();
                        } else {
                            result.appendInt(0);
                        }
                    }
                    return result.build();
                }
            }
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += field.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field);
        }

        @Override
        public String toString() {
            return "StDimensionConstantPointEvaluator[field=" + field + "]";
        }

        record Factory(ExpressionEvaluator.Factory field) implements ExpressionEvaluator.Factory {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new ConstantPointDimensionEvaluator(context, field.get(context));
            }

            @Override
            public String toString() {
                return "StDimensionConstantPointEvaluator[field=" + field + "]";
            }
        }
    }

    /**
     * Visitor that computes the topological dimension of a geometry.
     * Points have dimension 0, lines have dimension 1, polygons have dimension 2.
     * For geometry collections, returns the maximum dimension of the components.
     */
    static class GeometryDimensionVisitor implements GeometryVisitor<Integer, RuntimeException> {
        @Override
        public Integer visit(Circle circle) {
            return 2;
        }

        @Override
        public Integer visit(GeometryCollection<?> collection) {
            int maxDimension = 0;
            for (Geometry geometry : collection) {
                maxDimension = Math.max(maxDimension, geometry.visit(this));
                if (maxDimension == 2) {
                    break;
                }
            }
            return maxDimension;
        }

        @Override
        public Integer visit(Line line) {
            return 1;
        }

        @Override
        public Integer visit(LinearRing ring) {
            return 1;
        }

        @Override
        public Integer visit(MultiLine multiLine) {
            return 1;
        }

        @Override
        public Integer visit(MultiPoint multiPoint) {
            return 0;
        }

        @Override
        public Integer visit(MultiPolygon multiPolygon) {
            return 2;
        }

        @Override
        public Integer visit(Point point) {
            return 0;
        }

        @Override
        public Integer visit(Polygon polygon) {
            return 2;
        }

        @Override
        public Integer visit(Rectangle rectangle) {
            return 2;
        }
    }
}
