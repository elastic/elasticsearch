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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
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
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2Ds;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.makeGeometryFromLiteral;

/**
 * This is the primary class for supporting the function ST_CONTAINS.
 * The bulk of the capabilities are within the parent class SpatialRelatesFunction,
 * which supports all the relations in the ShapeField.QueryRelation enum.
 * Here we simply wire the rules together specific to ST_CONTAINS and QueryRelation.CONTAINS.
 */
public class SpatialContains extends SpatialRelatesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialContains",
        SpatialContains::new
    );

    // public for test access with reflection
    public static final SpatialRelationsContains GEO = new SpatialRelationsContains(
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Contains")
    );
    // public for test access with reflection
    public static final SpatialRelationsContains CARTESIAN = new SpatialRelationsContains(
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Contains")
    );

    /**
     * We override the normal behaviour for CONTAINS because we need to test each component separately.
     * This applies to multi-component geometries (MultiPolygon, etc.) as well as polygons that cross the dateline.
     */
    static final class SpatialRelationsContains extends SpatialRelations {
        SpatialRelationsContains(SpatialCoordinateTypes spatialCoordinateType, CoordinateEncoder encoder, ShapeIndexer shapeIndexer) {
            super(ShapeField.QueryRelation.CONTAINS, spatialCoordinateType, encoder, shapeIndexer);
        }

        @Override
        protected boolean geometryRelatesGeometry(BytesRef left, BytesRef right) throws IOException {
            Component2D[] rightComponent2Ds = asLuceneComponent2Ds(crsType, fromBytesRef(right));
            return geometryRelatesGeometries(left, rightComponent2Ds);
        }

        private boolean geometryRelatesGeometries(BytesRef left, Component2D[] rightComponent2Ds) throws IOException {
            Geometry leftGeom = fromBytesRef(left);
            GeometryDocValueReader leftDocValueReader = asGeometryDocValueReader(coordinateEncoder, shapeIndexer, leftGeom);
            return geometryRelatesGeometries(leftDocValueReader, rightComponent2Ds);
        }

        private boolean geometryRelatesGeometries(GeometryDocValueReader leftDocValueReader, Component2D[] rightComponent2Ds)
            throws IOException {
            for (Component2D rightComponent2D : rightComponent2Ds) {
                // Every component of the right geometry must be contained within the left geometry for this to pass
                if (geometryRelatesGeometry(leftDocValueReader, rightComponent2D) == false) {
                    return false;
                }
            }
            return true;
        }

        private boolean pointRelatesGeometries(long encoded, Component2D[] rightComponent2Ds) {
            // This code path exists for doc-values points, and we could consider re-using the point class to reduce garbage creation
            Point point = spatialCoordinateType.longAsPoint(encoded);
            for (Component2D rightComponent2D : rightComponent2Ds) {
                // Every component of the right geometry must be contained within the left geometry for this to pass
                if (pointRelatesGeometry(point, rightComponent2D) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    @FunctionInfo(
        returnType = { "boolean" },
        description = """
            Returns whether the first geometry contains the second geometry.
            This is the inverse of the <<esql-st_within,ST_WITHIN>> function.""",
        examples = @Example(file = "spatial_shapes", tag = "st_contains-airport_city_boundaries")
    )
    public SpatialContains(
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

    SpatialContains(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues);
    }

    private SpatialContains(StreamInput in) throws IOException {
        super(in, false, false);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ShapeField.QueryRelation queryRelation() {
        return ShapeField.QueryRelation.CONTAINS;
    }

    @Override
    public SpatialContains withDocValues(Set<FieldAttribute> attributes) {
        // Only update the docValues flags if the field is found in the attributes
        boolean leftDV = leftDocValues || foundField(left(), attributes);
        boolean rightDV = rightDocValues || foundField(right(), attributes);
        return new SpatialContains(source(), left(), right(), leftDV, rightDV);
    }

    @Override
    protected SpatialContains replaceChildren(Expression newLeft, Expression newRight) {
        return new SpatialContains(source(), newLeft, newRight, leftDocValues, rightDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, SpatialContains::new, left(), right());
    }

    @Override
    public Object fold() {
        try {
            GeometryDocValueReader docValueReader = asGeometryDocValueReader(crsType, left());
            Geometry rightGeom = makeGeometryFromLiteral(right());
            Component2D[] components = asLuceneComponent2Ds(crsType, rightGeom);
            return (crsType == SpatialCrsType.GEO)
                ? GEO.geometryRelatesGeometries(docValueReader, components)
                : CARTESIAN.geometryRelatesGeometries(docValueReader, components);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to fold constant fields: " + e.getMessage(), e);
        }
    }

    @Override
    Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules() {
        return evaluatorMap;
    }

    /**
     * To keep the number of evaluators to a minimum, we swap the arguments to get the WITHIN relation.
     * This also makes other optimizations, like lucene-pushdown, simpler to develop.
     */
    @Override
    public SpatialRelatesFunction surrogate() {
        if (left().foldable() && right().foldable() == false) {
            return new SpatialWithin(source(), right(), left(), rightDocValues, leftDocValues);
        }
        return this;
    }

    private static final Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorMap = new HashMap<>();

    static {
        // Support geo_point and geo_shape from source and constant combinations
        for (DataType spatialType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
            for (DataType otherType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(SpatialContainsGeoSourceAndSourceEvaluator.Factory::new)
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantArrayFactory(
                        SpatialContainsGeoSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialContainsGeoPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantArrayFactory(
                            SpatialContainsGeoPointDocValuesAndConstantEvaluator.Factory::new
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
                        SpatialContainsCartesianSourceAndSourceEvaluator.Factory::new
                    )
                );
                evaluatorMap.put(
                    SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantArrayFactory(
                        SpatialContainsCartesianSourceAndConstantEvaluator.Factory::new
                    )
                );
                if (DataType.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSources(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorFactoryWithFields(
                            SpatialContainsCartesianPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorFactory.SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withLeftDocValues(),
                        new SpatialEvaluatorFactory.SpatialEvaluatorWithConstantArrayFactory(
                            SpatialContainsCartesianPointDocValuesAndConstantEvaluator.Factory::new
                        )
                    );
                }
            }
        }
    }

    @Evaluator(extraName = "GeoSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndConstant(BytesRef leftValue, @Fixed Component2D[] rightValue) throws IOException {
        return GEO.geometryRelatesGeometries(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndConstant(long leftValue, @Fixed Component2D[] rightValue) {
        return GEO.pointRelatesGeometries(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndConstant(BytesRef leftValue, @Fixed Component2D[] rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometries(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndConstant(long leftValue, @Fixed Component2D[] rightValue) {
        return CARTESIAN.pointRelatesGeometries(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource")
    static boolean processCartesianPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.pointRelatesGeometry(leftValue, geometry);
    }
}
