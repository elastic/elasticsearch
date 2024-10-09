/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.makeGeometryFromLiteral;

/**
 * Computes the distance between two points.
 * For cartesian geometries, this is the pythagorean distance in the same units as the original coordinates.
 * For geographic geometries, this is the circular distance along the great circle in meters.
 * The function `st_distance` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is described in PostGIS documentation at <a href="https://postgis.net/docs/ST_Distance.html">PostGIS:ST_Distance</a>.
 */
public class StDistance extends BinarySpatialFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StDistance",
        StDistance::new
    );

    // public for test access with reflection
    public static final DistanceCalculator GEO = new GeoDistanceCalculator();
    // public for test access with reflection
    public static final DistanceCalculator CARTESIAN = new CartesianDistanceCalculator();

    protected static class GeoDistanceCalculator extends DistanceCalculator {
        protected GeoDistanceCalculator() {
            super(SpatialCoordinateTypes.GEO, CoordinateEncoder.GEO);
        }

        @Override
        protected double distance(Point left, Point right) {
            return SloppyMath.haversinMeters(
                GeoUtils.quantizeLat(left.getY()),
                GeoUtils.quantizeLon(left.getX()),
                GeoUtils.quantizeLat(right.getY()),
                GeoUtils.quantizeLon(right.getX())
            );
        }
    }

    protected static class CartesianDistanceCalculator extends DistanceCalculator {

        protected CartesianDistanceCalculator() {
            super(SpatialCoordinateTypes.CARTESIAN, CoordinateEncoder.CARTESIAN);
        }

        @Override
        protected double distance(Point left, Point right) {
            final double diffX = left.getX() - right.getX();
            final double diffY = left.getY() - right.getY();
            return Math.sqrt(diffX * diffX + diffY * diffY);
        }
    }

    /**
     * This class is a CRS specific interface for generalizing distance calculations for the various possible ways
     * that the geometries can be provided, from source, from evals, from literals and from doc values.
     */
    public abstract static class DistanceCalculator extends BinarySpatialComparator<Double> {

        protected DistanceCalculator(SpatialCoordinateTypes spatialCoordinateType, CoordinateEncoder encoder) {
            super(spatialCoordinateType, encoder);
        }

        @Override
        protected Double compare(BytesRef left, BytesRef right) throws IOException {
            return distance(left, right);
        }

        protected abstract double distance(Point left, Point right);

        protected double distance(long encoded, Geometry right) {
            Point point = spatialCoordinateType.longAsPoint(encoded);
            return distance(point, (Point) right);
        }

        protected double distance(Geometry left, Geometry right) {
            return distance((Point) left, (Point) right);
        }

        public double distance(BytesRef left, BytesRef right) {
            return distance(this.fromBytesRef(left), this.fromBytesRef(right));
        }

        public double distance(BytesRef left, Point right) {
            return distance(this.fromBytesRef(left), right);
        }
    }

    @FunctionInfo(
        returnType = "double",
        description = """
            Computes the distance between two points.
            For cartesian geometries, this is the pythagorean distance in the same units as the original coordinates.
            For geographic geometries, this is the circular distance along the great circle in meters.""",
        examples = @Example(file = "spatial", tag = "st_distance-airports")
    )
    public StDistance(
        Source source,
        @Param(name = "geomA", type = { "geo_point", "cartesian_point" }, description = """
            Expression of type `geo_point` or `cartesian_point`.
            If `null`, the function returns `null`.""") Expression left,
        @Param(name = "geomB", type = { "geo_point", "cartesian_point" }, description = """
            Expression of type `geo_point` or `cartesian_point`.
            If `null`, the function returns `null`.
            The second parameter must also have the same coordinate system as the first.
            This means it is not possible to combine `geo_point` and `cartesian_point` parameters.""") Expression right
    ) {
        super(source, left, right, false, false, true);
    }

    protected StDistance(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues, true);
    }

    private StDistance(StreamInput in) throws IOException {
        super(in, false, false, true);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    protected StDistance replaceChildren(Expression newLeft, Expression newRight) {
        return new StDistance(source(), newLeft, newRight, leftDocValues, rightDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StDistance::new, left(), right());
    }

    @Override
    public Object fold() {
        var leftGeom = makeGeometryFromLiteral(left());
        var rightGeom = makeGeometryFromLiteral(right());
        return (crsType() == SpatialCrsType.GEO) ? GEO.distance(leftGeom, rightGeom) : CARTESIAN.distance(leftGeom, rightGeom);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (right().foldable()) {
            return toEvaluator(toEvaluator, left(), makeGeometryFromLiteral(right()), leftDocValues);
        } else if (left().foldable()) {
            return toEvaluator(toEvaluator, right(), makeGeometryFromLiteral(left()), rightDocValues);
        } else {
            EvalOperator.ExpressionEvaluator.Factory leftE = toEvaluator.apply(left());
            EvalOperator.ExpressionEvaluator.Factory rightE = toEvaluator.apply(right());
            if (crsType() == SpatialCrsType.GEO) {
                if (leftDocValues) {
                    return new StDistanceGeoPointDocValuesAndSourceEvaluator.Factory(source(), leftE, rightE);
                } else if (rightDocValues) {
                    return new StDistanceGeoPointDocValuesAndSourceEvaluator.Factory(source(), rightE, leftE);
                } else {
                    return new StDistanceGeoSourceAndSourceEvaluator.Factory(source(), leftE, rightE);
                }
            } else if (crsType() == SpatialCrsType.CARTESIAN) {
                if (leftDocValues) {
                    return new StDistanceCartesianPointDocValuesAndSourceEvaluator.Factory(source(), leftE, rightE);
                } else if (rightDocValues) {
                    return new StDistanceCartesianPointDocValuesAndSourceEvaluator.Factory(source(), rightE, leftE);
                } else {
                    return new StDistanceCartesianSourceAndSourceEvaluator.Factory(source(), leftE, rightE);
                }
            }
        }
        throw EsqlIllegalArgumentException.illegalDataType(crsType().name());
    }

    private EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        ToEvaluator toEvaluator,
        Expression field,
        Geometry geometry,
        boolean docValues
    ) {
        if (geometry instanceof Point point) {
            return toEvaluator(toEvaluator, field, point, docValues);
        } else {
            throw new IllegalArgumentException("Unsupported geometry type for ST_DISTANCE: " + geometry.type().name());
        }
    }

    private EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        ToEvaluator toEvaluator,
        Expression field,
        Point point,
        boolean docValues
    ) {
        EvalOperator.ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (crsType() == SpatialCrsType.GEO) {
            if (docValues) {
                return new StDistanceGeoPointDocValuesAndConstantEvaluator.Factory(source(), fieldEvaluator, point);
            } else {
                return new StDistanceGeoSourceAndConstantEvaluator.Factory(source(), fieldEvaluator, point);
            }
        } else if (crsType() == SpatialCrsType.CARTESIAN) {
            if (docValues) {
                return new StDistanceCartesianPointDocValuesAndConstantEvaluator.Factory(source(), fieldEvaluator, point);
            } else {
                return new StDistanceCartesianSourceAndConstantEvaluator.Factory(source(), fieldEvaluator, point);
            }
        }
        throw EsqlIllegalArgumentException.illegalDataType(crsType().name());
    }

    @Evaluator(extraName = "GeoSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static double processGeoSourceAndConstant(BytesRef leftValue, @Fixed Point rightValue) throws IOException {
        return GEO.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static double processGeoSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return GEO.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static double processGeoPointDocValuesAndConstant(long leftValue, @Fixed Point rightValue) {
        return GEO.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "GeoPointDocValuesAndSource", warnExceptions = { IllegalArgumentException.class })
    static double processGeoPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.distance(leftValue, geometry);
    }

    @Evaluator(extraName = "CartesianSourceAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static double processCartesianSourceAndConstant(BytesRef leftValue, @Fixed Point rightValue) throws IOException {
        return CARTESIAN.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianSourceAndSource", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static double processCartesianSourceAndSource(BytesRef leftValue, BytesRef rightValue) throws IOException {
        return CARTESIAN.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstant", warnExceptions = { IllegalArgumentException.class })
    static double processCartesianPointDocValuesAndConstant(long leftValue, @Fixed Point rightValue) {
        return CARTESIAN.distance(leftValue, rightValue);
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndSource")
    static double processCartesianPointDocValuesAndSource(long leftValue, BytesRef rightValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.distance(leftValue, geometry);
    }
}
