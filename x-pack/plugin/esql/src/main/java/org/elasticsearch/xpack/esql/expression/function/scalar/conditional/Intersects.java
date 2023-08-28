/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.EqualsLongsEvaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialUtils;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.type.DataTypes.GEO_POINT;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;

public class Intersects extends BinaryScalarFunction implements EvaluatorMapper {

    public Intersects(Source source, Expression timestamp, Expression argument) {
        super(source, timestamp, argument);
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isGeoPointOrString(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isGeoPointOrString(right(), sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isGeoPointOrString(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, dt -> dt == GEO_POINT || isString(dt), operationName, paramOrd, "geo_point", "keyword", "text");
    }

    @Override
    protected Intersects replaceChildren(Expression newLeft, Expression newRight) {
        return new Intersects(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Intersects::new, left(), right());
    }

    @Override
    public boolean foldable() {
        return left().foldable() && right().foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        if (isString(left().dataType())) {
            if (isString(right().dataType())) {
                if (left().foldable()) {
                    if (right().foldable()) {
                        // Both are constant strings
                        return () -> new IntersectsConstantConstantEvaluator(asGeometry(left()), asGeometry(right()));
                    } else {
                        // Left is a constant string, right is a variable string
                        return () -> new IntersectsConstantStringEvaluator(asGeometry(left()), toEvaluator.apply(right()).get());
                    }
                } else if (right().foldable()) {
                    // Left is a variable string, and right is a constant string
                    return () -> new IntersectsConstantStringEvaluator(asGeometry(right()), toEvaluator.apply(left()).get());
                } else {
                    // Both are variable strings
                    return () -> new IntersectsStringStringEvaluator(toEvaluator.apply(left()).get(), toEvaluator.apply(right()).get());
                }
            } else if (left().foldable()) {
                // Left is constant string, right is long
                return () -> new IntersectsLongConstantEvaluator(toEvaluator.apply(right()).get(), asGeometry(left()));
            } else {
                // Left is variable string, right is long
                return () -> new IntersectsLongStringEvaluator(toEvaluator.apply(right()).get(), toEvaluator.apply(left()).get());
            }
        } else if (isString(right().dataType())) {
            if (right().foldable()) {
                // Left is long, right is constant string
                return () -> new IntersectsLongConstantEvaluator(toEvaluator.apply(left()).get(), asGeometry(right()));
            } else {
                // Left is long, right is variable string
                return () -> new IntersectsLongStringEvaluator(toEvaluator.apply(left()).get(), toEvaluator.apply(right()).get());
            }
        } else {
            // left and right are longs
            return () -> new EqualsLongsEvaluator(toEvaluator.apply(left()).get(), toEvaluator.apply(right()).get());
        }
    }

    private static Geometry asGeometry(Expression expression) {
        Object result = expression.fold();
        if (result instanceof BytesRef bytesRef) {
            return SpatialUtils.stringAsGeometry(bytesRef.utf8ToString());
        } else {
            throw new IllegalArgumentException("Invalid constant string: " + result);
        }
    }

    @Evaluator(extraName = "LongConstant")
    static boolean processConstant(long leftValue, @Fixed Geometry rightValue) {
        return pointIntersectsGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "LongString")
    static boolean process(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialUtils.stringAsGeometry(rightValue.utf8ToString());
        return pointIntersectsGeometry(leftValue, geometry);
    }

    @Evaluator(extraName = "ConstantConstant")
    static boolean processConstant(@Fixed Geometry leftValue, @Fixed Geometry rightValue) {
        return geometryIntersectsGeometry(leftValue, rightValue);
    }

    @Evaluator(extraName = "ConstantString")
    static boolean processConstant(@Fixed Geometry leftValue, BytesRef rightValue) {
        Geometry rightGeom = SpatialUtils.stringAsGeometry(rightValue.utf8ToString());
        return geometryIntersectsGeometry(leftValue, rightGeom);
    }

    @Evaluator(extraName = "StringString")
    static boolean process(BytesRef leftValue, BytesRef rightValue) {
        Geometry leftGeom = SpatialUtils.stringAsGeometry(leftValue.utf8ToString());
        Geometry rightGeom = SpatialUtils.stringAsGeometry(rightValue.utf8ToString());
        return geometryIntersectsGeometry(leftGeom, rightGeom);
    }

    private static boolean pointIntersectsGeometry(Point point, Geometry geometry) {
        return pointIntersectsGeometry(new GeoPoint(point.getLat(), point.getLon()).getEncoded(), geometry);
    }

    private static boolean pointIntersectsGeometry(long encodedPoint, Geometry geometry) {
        GeoPoint geoPoint = SpatialUtils.longAsGeoPoint(encodedPoint);
        if (geometry instanceof Point point) {
            GeoPoint gp = new GeoPoint(point.getLat(), point.getLon());
            return gp.getEncoded() == geoPoint.getEncoded();
        } else if (geometry instanceof Line line) {
            return geometryContainsPoint(toLuceneLine(line), geoPoint);
        } else if (geometry instanceof Rectangle rectangle) {
            return geometryContainsPoint(toLucenePolygon(rectangle), geoPoint);
        } else if (geometry instanceof Polygon polygon) {
            return geometryContainsPoint(toLucenePolygon(polygon), geoPoint);
        }
        return false;
    }

    private static boolean geometryIntersectsGeometry(Geometry left, Geometry right) {
        if (left instanceof Point point) {
            return pointIntersectsGeometry(point, right);
        } else if (right instanceof Point point) {
            return pointIntersectsGeometry(point, left);
        } else {
            // TODO: support other combinations
            throw new IllegalArgumentException(
                "Can only compare point to other geometries, but was provided left:" + left.type() + " and right:" + right.type()
            );
        }
    }

    private static boolean geometryContainsPoint(LatLonGeometry geometry, GeoPoint geoPoint) {
        Component2D component2D = LatLonGeometry.create(geometry);
        return component2D.contains(geoPoint.getX(), geoPoint.getY());
    }

    private static org.apache.lucene.geo.Polygon toLucenePolygon(Polygon polygon) {
        org.apache.lucene.geo.Polygon[] holes = new org.apache.lucene.geo.Polygon[polygon.getNumberOfHoles()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.Polygon(polygon.getHole(i).getY(), polygon.getHole(i).getX());
        }
        return new org.apache.lucene.geo.Polygon(polygon.getPolygon().getY(), polygon.getPolygon().getX(), holes);
    }

    private static org.apache.lucene.geo.Polygon toLucenePolygon(Rectangle r) {
        return new org.apache.lucene.geo.Polygon(
            new double[] { r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat() },
            new double[] { r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon() }
        );
    }

    private static org.apache.lucene.geo.Line toLuceneLine(Line line) {
        return new org.apache.lucene.geo.Line(line.getLats(), line.getLons());
    }
}
