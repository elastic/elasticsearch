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
import org.elasticsearch.compute.ann.Position;
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
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEOTILE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.makeGeometryFromLiteral;

/**
 * This is the primary class for supporting the function ST_DISJOINT.
 * The bulk of the capabilities are within the parent class SpatialRelatesFunction,
 * which supports all the relations in the ShapeField.QueryRelation enum.
 * Here we simply wire the rules together specific to ST_DISJOINT and QueryRelation.DISJOINT.
 */
public class SpatialDisjoint extends SpatialRelatesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialDisjoint",
        SpatialDisjoint::new
    );

    // public for test access with reflection
    public static final SpatialRelations GEO = new SpatialRelations(
        ShapeField.QueryRelation.DISJOINT,
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Disjoint")
    );
    // public for test access with reflection
    public static final SpatialRelations CARTESIAN = new SpatialRelations(
        ShapeField.QueryRelation.DISJOINT,
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Disjoint")
    );

    @FunctionInfo(
        returnType = { "boolean" },
        description = """
            Returns whether the two geometries or geometry columns are disjoint.
            This is the inverse of the <<esql-st_intersects,ST_INTERSECTS>> function.
            In mathematical terms: ST_Disjoint(A, B) ⇔ A ⋂ B = ∅""",
        examples = @Example(file = "spatial_shapes", tag = "st_disjoint-airport_city_boundaries")
    )
    public SpatialDisjoint(
        Source source,
        @Param(
            name = "geomA",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape", "geohash", "geotile", "geohex" },
            description = """
                Expression that is either a geometry (`geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`)
                or a geo-grid value (`geohash`, `geotile`, `geohex`).
                If `null`, the function returns `null`."""
        ) Expression left,
        @Param(
            name = "geomB",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape", "geohash", "geotile", "geohex" },
            description = """
                Expression that is either a geometry (`geo_point`, `cartesian_point`, `geo_shape` or `cartesian_shape`)
                or a geo-grid value (`geohash`, `geotile`, `geohex`).
                If `null`, the function returns `null`.
                The second parameter must also have the same coordinate system as the first.
                This means it is not possible to combine `geo_*` and `cartesian_*` parameters."""
        ) Expression right
    ) {
        this(source, left, right, false, false);
    }

    private SpatialDisjoint(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues, true);
    }

    private SpatialDisjoint(StreamInput in) throws IOException {
        super(in, false, false, true);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ShapeRelation queryRelation() {
        return ShapeRelation.DISJOINT;
    }

    @Override
    public SpatialDisjoint withDocValues(boolean foundLeft, boolean foundRight) {
        // Only update the docValues flags if the field is found in the attributes
        boolean leftDV = leftDocValues || foundLeft;
        boolean rightDV = rightDocValues || foundRight;
        return new SpatialDisjoint(source(), left(), right(), leftDV, rightDV);
    }

    @Override
    protected SpatialDisjoint replaceChildren(Expression newLeft, Expression newRight) {
        return new SpatialDisjoint(source(), newLeft, newRight, leftDocValues, rightDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, SpatialDisjoint::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        try {
            if (DataType.isGeoGrid(left().dataType())) {
                return foldGeoGrid(ctx, right(), left(), left().dataType());
            } else if (DataType.isGeoGrid(right().dataType())) {
                return foldGeoGrid(ctx, left(), right(), right().dataType());
            }
            GeometryDocValueReader docValueReader = asGeometryDocValueReader(ctx, crsType(), left());
            Component2D component2D = asLuceneComponent2D(ctx, crsType(), right());
            return (crsType() == SpatialCrsType.GEO)
                ? GEO.geometryRelatesGeometry(docValueReader, component2D)
                : CARTESIAN.geometryRelatesGeometry(docValueReader, component2D);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to fold constant fields: " + e.getMessage(), e);
        }
    }

    private Object foldGeoGrid(FoldContext ctx, Expression spatialExp, Expression gridExp, DataType gridType) throws IOException {
        long gridId = (Long) valueOf(ctx, gridExp);
        return GEO.compareGeometryAndGrid(makeGeometryFromLiteral(ctx, spatialExp), gridId, gridType);
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
                    new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(SpatialDisjointGeoSourceAndSourceEvaluator.Factory::new)
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                        SpatialDisjointGeoSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialDisjointGeoPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                            SpatialDisjointGeoPointDocValuesAndConstantEvaluator.Factory::new
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
                        SpatialDisjointCartesianSourceAndSourceEvaluator.Factory::new
                    )
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                        SpatialDisjointCartesianSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialDisjointCartesianPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantFactory(
                            SpatialDisjointCartesianPointDocValuesAndConstantEvaluator.Factory::new
                        )
                    );
                }
            }
        }

        // Support geo_point and geo-grid types
        for (DataType gridType : new DataType[] { GEOHASH, GEOTILE, GEOHEX }) {
            evaluatorMap.put(
                SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(GEO_POINT, gridType),
                new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantGridFactory(
                    (s, w, g) -> new SpatialDisjointGeoSourceAndConstantGridEvaluator.Factory(s, w, g, gridType)
                )
            );
            evaluatorMap.put(
                SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(GEO_POINT, gridType).withLeftDocValues(),
                new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantGridFactory(
                    (s, w, g) -> new SpatialDisjointGeoPointDocValuesAndConstantGridEvaluator.Factory(s, w, g, gridType)
                )
            );
            evaluatorMap.put(
                SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(GEO_POINT, gridType),
                new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                    (s, w, g) -> new SpatialDisjointGeoSourceAndSourceGridEvaluator.Factory(s, w, g, gridType)
                )
            );
            evaluatorMap.put(
                SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(GEO_POINT, gridType).withLeftDocValues(),
                new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                    (s, w, g) -> new SpatialDisjointGeoPointDocValuesAndSourceGridEvaluator.Factory(s, w, g, gridType)
                )
            );
        }
    }

    @Evaluator(extraName = "GeoSourceAndConstantGrid", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndConstantGrid(
        BooleanBlock.Builder results,
        @Position int p,
        BytesRefBlock wkb,
        @Fixed long gridId,
        @Fixed DataType gridType
    ) throws IOException {
        GEO.processSourceAndConstantGrid(results, p, wkb, gridId, gridType);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstantGrid", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndConstantGrid(
        BooleanBlock.Builder results,
        @Position int p,
        LongBlock encodedPoints,
        @Fixed long gridId,
        @Fixed DataType gridType
    ) throws IOException {
        GEO.processGeoPointDocValuesAndConstantGrid(results, p, encodedPoints, gridId, gridType);
    }

    @Evaluator(extraName = "GeoSourceAndSourceGrid", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndSourceGrid(
        BooleanBlock.Builder results,
        @Position int p,
        BytesRefBlock wkb,
        LongBlock gridId,
        @Fixed DataType gridType
    ) throws IOException {
        GEO.processSourceAndSourceGrid(results, p, wkb, gridId, gridType);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSourceGrid", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndSourceGrid(
        BooleanBlock.Builder results,
        @Position int p,
        LongBlock encodedPoints,
        LongBlock gridIds,
        @Fixed DataType gridType
    ) throws IOException {
        GEO.processGeoPointDocValuesAndSourceGrid(results, p, encodedPoints, gridIds, gridType);
    }

    @Evaluator(extraName = "GeoSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndConstant(BooleanBlock.Builder results, @Position int p, BytesRefBlock left, @Fixed Component2D right)
        throws IOException {
        GEO.processSourceAndConstant(results, p, left, right);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoSourceAndSource(BooleanBlock.Builder builder, @Position int p, BytesRefBlock left, BytesRefBlock right)
        throws IOException {
        GEO.processSourceAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndConstant(BooleanBlock.Builder builder, @Position int p, LongBlock left, @Fixed Component2D right)
        throws IOException {
        GEO.processPointDocValuesAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processGeoPointDocValuesAndSource(BooleanBlock.Builder builder, @Position int p, LongBlock left, BytesRefBlock right)
        throws IOException {
        GEO.processPointDocValuesAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianSourceAndConstant(
        BooleanBlock.Builder builder,
        @Position int p,
        BytesRefBlock left,
        @Fixed Component2D right
    ) throws IOException {
        CARTESIAN.processSourceAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianSourceAndSource(BooleanBlock.Builder builder, @Position int p, BytesRefBlock left, BytesRefBlock right)
        throws IOException {
        CARTESIAN.processSourceAndSource(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianPointDocValuesAndConstant(
        BooleanBlock.Builder builder,
        @Position int p,
        LongBlock left,
        @Fixed Component2D right
    ) throws IOException {
        CARTESIAN.processPointDocValuesAndConstant(builder, p, left, right);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static void processCartesianPointDocValuesAndSource(BooleanBlock.Builder builder, @Position int p, LongBlock left, BytesRefBlock right)
        throws IOException {
        CARTESIAN.processPointDocValuesAndSource(builder, p, left, right);
    }
}
