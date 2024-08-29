/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
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
import java.util.Set;

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
    public SpatialIntersects withDocValues(Set<FieldAttribute> attributes) {
        // Only update the docValues flags if the field is found in the attributes
        boolean leftDV = leftDocValues || foundField(left(), attributes);
        boolean rightDV = rightDocValues || foundField(right(), attributes);
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
    public Object fold() {
        try {
            GeometryDocValueReader docValueReader = asGeometryDocValueReader(crsType(), left());
            Component2D component2D = asLuceneComponent2D(crsType(), right());
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
    static boolean processGeoSourceAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndConstant(long leftValue, @Fixed Component2D rightValue) {
        return GEO.pointRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndConstant(long leftValue, @Fixed Component2D rightValue) {
        return CARTESIAN.pointRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource")
    static boolean processCartesianPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.pointRelatesGeometry(leftValue, geometry);
    }
}
