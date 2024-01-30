/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

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
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;

public class SpatialIntersects extends SpatialRelatesFunction {
    private static final SpatialRelations GEO = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Intersects")
    );
    private static final SpatialRelations CARTESIAN = new SpatialRelations(
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

        // Support geo_point and geo_shape for non-doc-values combinations
        for (DataType geoDataType : new DataType[] { GEO_POINT, GEO_SHAPE }) {
            evaluatorMap.put(
                new SpatialEvaluatorKey(geoDataType, false, false, false),
                new SpatialEvaluatorFactoryWithFields(SpatialIntersectsGeoSourceAndSourceEvaluator.Factory::new)
            );
            evaluatorMap.put(
                new SpatialEvaluatorKey(geoDataType, false, false, true),
                new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoSourceAndConstantEvaluator.Factory::new, false)
            );
            evaluatorMap.put(
                new SpatialEvaluatorKey(geoDataType, false, true, false),
                new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoSourceAndConstantEvaluator.Factory::new, true)
            );
        }

        // Support cartesian_point and cartesian_shape for non-doc-values combinations
        for (DataType cartesianDataType : new DataType[] { CARTESIAN_POINT, CARTESIAN_SHAPE }) {
            evaluatorMap.put(
                new SpatialEvaluatorKey(cartesianDataType, false, false, false),
                new SpatialEvaluatorFactoryWithFields(SpatialIntersectsCartesianSourceAndSourceEvaluator.Factory::new)
            );
            evaluatorMap.put(
                new SpatialEvaluatorKey(cartesianDataType, false, false, true),
                new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianSourceAndConstantEvaluator.Factory::new, false)
            );
            evaluatorMap.put(
                new SpatialEvaluatorKey(cartesianDataType, false, true, false),
                new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianSourceAndConstantEvaluator.Factory::new, true)
            );
        }

        // If both sides are constant strings, we will treat generically
        for (DataType dataType : new DataType[] { GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE }) {
            evaluatorMap.put(
                new SpatialEvaluatorKey(dataType, false, true, true),
                new SpatialEvaluatorWithConstantsFactory(SpatialIntersectsConstantAndConstantEvaluator.Factory::new)
            );
        }

        // For doc-values we only support points TODO: support shapes as doc-values
        evaluatorMap.put(
            new SpatialEvaluatorKey(GEO_POINT, true, false, false),
            new SpatialEvaluatorFactoryWithFields(SpatialIntersectsGeoPointDocValuesAndSourceEvaluator.Factory::new)
        );
        evaluatorMap.put(
            new SpatialEvaluatorKey(GEO_POINT, true, false, true),
            new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoPointDocValuesAndConstantEvaluator.Factory::new, false)
        );
        evaluatorMap.put(
            new SpatialEvaluatorKey(GEO_POINT, true, true, false),
            new SpatialEvaluatorWithConstantFactory(SpatialIntersectsGeoPointDocValuesAndConstantEvaluator.Factory::new, true)
        );
        evaluatorMap.put(
            new SpatialEvaluatorKey(CARTESIAN_POINT, true, false, false),
            new SpatialEvaluatorFactoryWithFields(SpatialIntersectsCartesianPointDocValuesAndSourceEvaluator.Factory::new)
        );
        evaluatorMap.put(
            new SpatialEvaluatorKey(CARTESIAN_POINT, true, false, true),
            new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianPointDocValuesAndConstantEvaluator.Factory::new, false)
        );
        evaluatorMap.put(
            new SpatialEvaluatorKey(CARTESIAN_POINT, true, true, false),
            new SpatialEvaluatorWithConstantFactory(SpatialIntersectsCartesianPointDocValuesAndConstantEvaluator.Factory::new, true)
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

    @Evaluator(extraName = "ConstantAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processConstantAndConstant(@Fixed GeometryDocValueReader leftValue, @Fixed Component2D rightValue) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(leftValue, rightValue);
    }
}
