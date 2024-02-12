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
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;

public class SpatialIntersects extends SpatialRelatesFunction {
    protected static final SpatialRelations GEO = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Intersects")
    );
    protected static final SpatialRelations CARTESIAN = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Intersects")
    );

    @FunctionInfo(returnType = { "boolean" }, description = "Returns whether the two geometries or geometry columns intersect.")
    public SpatialIntersects(
        Source source,
        @Param(
            name = "field",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" },
            description = "Geometry column name or variable of geometry type"
        ) Expression left,
        @Param(
            name = "other",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape", "keyword", "text" },
            description = "Geometry column name or variable of geometry type or a string literal containing a WKT geometry"
        ) Expression right
    ) {
        this(source, left, right, false);
    }

    private SpatialIntersects(Source source, Expression left, Expression right, boolean useDocValues) {
        super(source, left, right, useDocValues);
    }

    @Override
    public ShapeField.QueryRelation queryRelation() {
        return ShapeField.QueryRelation.INTERSECTS;
    }

    @Override
    public SpatialIntersects withDocValues() {
        return new SpatialIntersects(source(), left(), right(), true);
    }

    @Override
    protected SpatialIntersects replaceChildren(Expression newLeft, Expression newRight) {
        return new SpatialIntersects(source(), newLeft, newRight, useDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, SpatialIntersects::new, left(), right());
    }

    protected Map<SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules() {
        HashMap<SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorMap = new HashMap<>();

        // Support geo_point and geo_shape from source and constant combinations
        for (DataType spatialType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
            for (DataType otherType : new DataType[] { GEO_POINT, GEO_SHAPE, DataTypes.KEYWORD }) {
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromSources(spatialType, otherType),
                    new SpatialEvaluatorFactoryWithFields(
                        // Both Geometry and String fields are backed by BytesRef, so we need different evaluators
                        otherType == DataTypes.KEYWORD
                            ? SpatialIntersectsGeoSourceAndStringEvaluator.Factory::new
                            : SpatialIntersectsGeoSourceAndSourceEvaluator.Factory::new
                    )
                );
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoSourceAndConstantEvaluator.Factory::new)
                );
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromConstants(spatialType, otherType),
                    new SpatialEvaluatorWithConstantsFactory(SpatialIntersectsGeoConstantAndConstantEvaluator.Factory::new)
                );
                if (EsqlDataTypes.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorKey.fromSources(spatialType, otherType).withDocValues(),
                        new SpatialEvaluatorFactoryWithFields(
                            // Both Geometry and String fields are backed by BytesRef, so we need different evaluators
                            otherType == DataTypes.KEYWORD
                                ? SpatialIntersectsGeoPointDocValuesAndStringEvaluator.Factory::new
                                : SpatialIntersectsGeoPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withDocValues(),
                        new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoPointDocValuesAndConstantEvaluator.Factory::new)
                    );
                }
            }
        }

        // Support cartesian_point and cartesian_shape from source and constant combinations
        for (DataType spatialType : new DataType[] { CARTESIAN_POINT, CARTESIAN_SHAPE }) {
            for (DataType otherType : new DataType[] { CARTESIAN_POINT, CARTESIAN_SHAPE, DataTypes.KEYWORD }) {
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromSources(spatialType, otherType),
                    new SpatialEvaluatorFactoryWithFields(
                        // Both Geometry and String fields are backed by BytesRef, so we need different evaluators
                        otherType == DataTypes.KEYWORD
                            ? SpatialIntersectsCartesianSourceAndStringEvaluator.Factory::new
                            : SpatialIntersectsCartesianSourceAndSourceEvaluator.Factory::new
                    )
                );
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType),
                    new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianSourceAndConstantEvaluator.Factory::new)
                );
                evaluatorMap.put(
                    SpatialEvaluatorKey.fromConstants(spatialType, otherType),
                    new SpatialEvaluatorWithConstantsFactory(SpatialIntersectsCartesianConstantAndConstantEvaluator.Factory::new)
                );
                if (EsqlDataTypes.isSpatialPoint(spatialType)) {
                    evaluatorMap.put(
                        SpatialEvaluatorKey.fromSources(spatialType, otherType).withDocValues(),
                        new SpatialEvaluatorFactoryWithFields(
                            // Both Geometry and String fields are backed by BytesRef, so we need different evaluators
                            otherType == DataTypes.KEYWORD
                                ? SpatialIntersectsCartesianPointDocValuesAndStringEvaluator.Factory::new
                                : SpatialIntersectsCartesianPointDocValuesAndSourceEvaluator.Factory::new
                        )
                    );
                    evaluatorMap.put(
                        SpatialEvaluatorKey.fromSourceAndConstant(spatialType, otherType).withDocValues(),
                        new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianPointDocValuesAndConstantEvaluator.Factory::new)
                    );
                }
            }
        }

        // If both sides are string fields or constants we will treat generically since we do not know the CRS.
        // This is mostly a match for PostGIS, which assumes SRID=0 for WKT where SRID is not specified.
        // Elasticsearch does not have a concept of SRID in WKT parsing, and instead determines CRS from context.
        // In most cases this results in the same behavior as PostGIS, but there are some edge cases where it does not.
        SpatialEvaluatorKey key = SpatialEvaluatorKey.fromSources(DataTypes.KEYWORD, DataTypes.KEYWORD);
        evaluatorMap.put(key, new SpatialEvaluatorFactoryWithFields(SpatialIntersectsCartesianStringAndStringEvaluator.Factory::new));
        evaluatorMap.put(
            key.withConstants(false, true),
            new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianStringAndConstantEvaluator.Factory::new)
        );
        evaluatorMap.put(
            key.withConstants(true, true),
            new SpatialEvaluatorWithConstantsFactory(SpatialIntersectsCartesianConstantAndConstantEvaluator.Factory::new)
        );
        return evaluatorMap;
    }

    @Evaluator(extraName = "GeoSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoSourceAndString", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndString(BytesRef leftValue, BytesRef rightValue) throws IOException {
        BytesRef geometry = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(rightValue.utf8ToString());
        return GEO.geometryRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndConstant(long leftValue, @Fixed Component2D rightValue) {
        return GEO.pointRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndString", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndString(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(rightValue.utf8ToString());
        return GEO.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "GeoConstantAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoConstantAndConstant(@Fixed GeometryDocValueReader leftValue, @Fixed Component2D rightValue)
        throws IOException {
        return GEO.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoStringAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoStringAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        BytesRef leftGeom = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(leftValue.utf8ToString());
        return GEO.geometryRelatesGeometry(leftGeom, rightValue);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianSourceAndString", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndString(BytesRef leftValue, BytesRef rightValue) throws IOException {
        BytesRef geometry = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(rightValue.utf8ToString());
        return CARTESIAN.geometryRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndConstant(long leftValue, @Fixed Component2D rightValue) {
        return CARTESIAN.pointRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndString", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndString(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(rightValue.utf8ToString());
        return CARTESIAN.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource")
    static boolean processCartesianPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.pointRelatesGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianConstantAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianConstantAndConstant(@Fixed GeometryDocValueReader leftValue, @Fixed Component2D rightValue)
        throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianStringAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianStringAndConstant(BytesRef leftValue, @Fixed Component2D rightValue) throws IOException {
        BytesRef leftGeom = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(leftValue.utf8ToString());
        return CARTESIAN.geometryRelatesGeometry(leftGeom, rightValue);
    }

    @Evaluator(extraName = "CartesianStringAndString", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianStringAndString(BytesRef leftValue, BytesRef rightValue) throws IOException {
        BytesRef leftGeom = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(leftValue.utf8ToString());
        BytesRef rightGeom = SpatialCoordinateTypes.UNSPECIFIED.wktToWkb(rightValue.utf8ToString());
        return CARTESIAN.geometryRelatesGeometry(leftGeom, rightGeom);
    }
}
