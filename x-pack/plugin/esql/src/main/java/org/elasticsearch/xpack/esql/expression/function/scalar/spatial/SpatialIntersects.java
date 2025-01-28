/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;

/**
 * This is the primary class for supporting the function ST_INTERSECTS.
 * The bulk of the capabilities are within the parent class SpatialRelatesFunction,
 * which supports all the relations in the ShapeField.QueryRelation enum.
 * Here we simply wire the rules together specific to ST_INTERSECTS and QueryRelation.INTERSECTS.
 */
public class SpatialIntersects extends SpatialRelatesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialIntersects",
        SpatialIntersects::new
    );

    // public for test access with reflection
    public static final SpatialRelations GEO = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Intersects")
    );
    // public for test access with reflection
    public static final SpatialRelations CARTESIAN = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Intersects")
    );

    @FunctionInfo(returnType = { "boolean" }, description = """
        Returns true if two geometries intersect.
        They intersect if they have any point in common, including their interior points
        (points along lines or within polygons).
        This is the inverse of the <<esql-st_disjoint,ST_DISJOINT>> function.
        In mathematical terms: ST_Intersects(A, B) ⇔ A ⋂ B ≠ ∅""", examples = @Example(file = "spatial", tag = "st_intersects-airports"))
    public SpatialIntersects(
        Source source,
        @Param(name = "geomA", type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" }, description = """
            Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`.
            If `null`, the function returns `null`.""") Expression left,
        @Param(name = "geomB", type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" }, description = """
            Expression of type `geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`.
            If `null`, the function returns `null`.
            The second parameter must also have the same coordinate system as the first.
            This means it is not possible to combine `geo_*` and `cartesian_*` parameters.""") Expression right
    ) {
        this(source, left, right, false, false);
    }

    private SpatialIntersects(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues);
    }

    private SpatialIntersects(StreamInput in) throws IOException {
        super(in, false, false);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ShapeRelation queryRelation() {
        return ShapeRelation.INTERSECTS;
    }

    @Override
    public SpatialIntersects withDocValues(boolean foundLeft, boolean foundRight) {
        // Only update the docValues flags if the field is found in the attributes
        boolean leftDV = leftDocValues || foundLeft;
        boolean rightDV = rightDocValues || foundRight;
        return new SpatialIntersects(source(), left(), right(), leftDV, rightDV);
    }

    @Override
    protected SpatialIntersects replaceChildren(Expression newLeft, Expression newRight) {
        return new SpatialIntersects(source(), newLeft, newRight, leftDocValues, rightDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, SpatialIntersects::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        try {
            GeometryDocValueReader docValueReader = asGeometryDocValueReader(ctx, crsType(), left());
            Component2D component2D = asLuceneComponent2D(ctx, crsType(), right());
            return (crsType() == SpatialCrsType.GEO)
                ? GEO.geometryRelatesGeometry(docValueReader, component2D)
                : CARTESIAN.geometryRelatesGeometry(docValueReader, component2D);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to fold constant fields: " + e.getMessage(), e);
        }
    }

    @Override
    Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules() {
        return evaluatorMap;
    }

    private static final Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorMap = new HashMap<>();

    static {
        // Support geo_point and geo_shape from source and constant combinations
        for (DataType spatialType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
            for (DataType otherType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(SpatialIntersectsGeoSourceAndSourceEvaluator.Factory::new)
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                        SpatialIntersectsGeoSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialIntersectsGeoPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                            SpatialIntersectsGeoPointDocValuesAndConstantEvaluator.Factory::new
                        )
                    );
                }
            }
        }

        // Support cartesian_point and cartesian_shape from source and constant combinations
        for (DataType spatialType : new DataType[] { CARTESIAN_POINT, CARTESIAN_SHAPE }) {
            for (DataType otherType : new DataType[] { CARTESIAN_POINT, CARTESIAN_SHAPE }) {
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                        SpatialIntersectsCartesianSourceAndSourceEvaluator.Factory::new
                    )
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                        SpatialIntersectsCartesianSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialIntersectsCartesianPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                            SpatialIntersectsCartesianPointDocValuesAndConstantEvaluator.Factory::new
                        )
                    );
                }
            }
        }
    }

    @Evaluator(extraName = "GeoSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndConstant(BooleanBlock.Builder results, int p, BytesRefBlock left, @Fixed Component2D right)
        throws IOException {
        GEO.processSourceAndConstant(results, p, left, right);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndSource(BooleanBlock.Builder builder, int p, BytesRefBlock left, BytesRefBlock right) throws IOException {
        GEO.processSourceAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndConstant(BooleanBlock.Builder builder, int p, LongBlock left, @Fixed Component2D right)
        throws IOException {
        GEO.processPointDocValuesAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndSource(BooleanBlock.Builder builder, int p, LongBlock left, BytesRefBlock right)
        throws IOException {
        GEO.processPointDocValuesAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianSourceAndConstant(BooleanBlock.Builder builder, int p, BytesRefBlock left, @Fixed Component2D right)
        throws IOException {
        CARTESIAN.processSourceAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianSourceAndSource(BooleanBlock.Builder builder, int p, BytesRefBlock left, BytesRefBlock right)
        throws IOException {
        CARTESIAN.processSourceAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianPointDocValuesAndConstant(BooleanBlock.Builder builder, int p, LongBlock left, @Fixed Component2D right)
        throws IOException {
        CARTESIAN.processPointDocValuesAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianPointDocValuesAndSource(BooleanBlock.Builder builder, int p, LongBlock left, BytesRefBlock right)
        throws IOException {
        CARTESIAN.processPointDocValuesAndSource(builder, p, left, right);
    }
}
