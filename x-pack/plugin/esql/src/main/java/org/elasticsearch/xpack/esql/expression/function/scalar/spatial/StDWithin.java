/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.planner.ExpressionTranslators.valueOf;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.makeGeometryFromLiteral;

/**
 * Computes the distance between two points.
 * For cartesian geometries, this is the pythagorean distance in the same units as the original coordinates.
 * For geographic geometries, this is the circular distance along the great circle in meters.
 * The function `st_distance` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is described in PostGIS documentation at <a href="https://postgis.net/docs/ST_Distance.html">PostGIS:ST_Distance</a>.
 */
public class StDWithin extends TernarySpatialFunction implements EvaluatorMapper, SpatialEvaluatorFactory.SpatialSourceSupplier {
    // public for test access with reflection
    public static final StDistance.DistanceCalculator GEO = new StDistance.GeoDistanceCalculator();
    // public for test access with reflection
    public static final StDistance.DistanceCalculator CARTESIAN = new StDistance.CartesianDistanceCalculator();

    @FunctionInfo(
        returnType = "boolean",
        description = """
            Returns whether two geometries are within a specified distance of each other.
            For cartesian geometries, this is the pythagorean distance in the same units as the original coordinates.
            For geographic geometries, this is the circular distance along the great circle in meters.""",
        examples = @Example(file = "spatial", tag = "st_dwithin-airports")
    )
    public StDWithin(
        Source source,
        @Param(name = "geomA", type = { "geo_point", "cartesian_point" }, description = """
            Expression of type `geo_point` or `cartesian_point`.
            If `null`, the function returns `null`.""") Expression left,
        @Param(name = "geomB", type = { "geo_point", "cartesian_point" }, description = """
            Expression of type `geo_point` or `cartesian_point`.
            If `null`, the function returns `null`.
            The second parameter must also have the same coordinate system as the first.
            This means it is not possible to combine `geo_point` and `cartesian_point` parameters.""") Expression right,
        @Param(name = "distance", type = "double", description = """
            The distance in meters for geographic geometries or the distance
            in the same units as the original coordinates for cartesian geometries.
            If `null`, the function returns `null`.""") Expression distance
    ) {
        super(source, left, right, distance, false, false, true);
    }

    private StDWithin(
        Source source,
        Expression left,
        Expression right,
        Expression distance,
        boolean leftDocValues,
        boolean rightDocValues
    ) {
        super(source, left, right, distance, leftDocValues, rightDocValues, true);
    }

    @Override
    public DataType dataType() {
        return BOOLEAN;
    }

    @Override
    protected TypeResolution resolveArgType() {
        return TypeResolutions.isType(arg(), DataType::isRational, sourceText(), TypeResolutions.ParamOrdinal.THIRD, "double");
    }

    @Override
    protected StDWithin replaceChildren(Expression newLeft, Expression newRight, Expression newArg) {
        return new StDWithin(source(), newLeft, newRight, newArg, leftDocValues, rightDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StDWithin::new, left(), right(), arg());
    }

    @Override
    public Object fold() {
        var leftGeom = makeGeometryFromLiteral(left());
        var rightGeom = makeGeometryFromLiteral(right());
        Object distValue = valueOf(arg());

        if (distValue instanceof Number distance) {
            double geomDistance = (crsType == BinarySpatialFunction.SpatialCrsType.GEO)
                ? GEO.distance(leftGeom, rightGeom)
                : CARTESIAN.distance(leftGeom, rightGeom);
            return geomDistance <= distance.doubleValue();
        } else {
            throw illegalDistance(distValue);
        }
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        if (right().foldable()) {
            return toEvaluator(toEvaluator, left(), makeGeometryFromLiteral(right()), leftDocValues);
        } else if (left().foldable()) {
            return toEvaluator(toEvaluator, right(), makeGeometryFromLiteral(left()), rightDocValues);
        } else {
            EvalOperator.ExpressionEvaluator.Factory leftE = toEvaluator.apply(left());
            EvalOperator.ExpressionEvaluator.Factory rightE = toEvaluator.apply(right());
            if (arg().foldable()) {
                Object distObj = valueOf(arg());
                if (distObj instanceof Number distValue) {
                    double dist = distValue.doubleValue();
                    if (crsType == BinarySpatialFunction.SpatialCrsType.GEO) {
                        if (leftDocValues) {
                            return new StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator.Factory(source(), leftE, rightE, dist);
                        } else if (rightDocValues) {
                            return new StDWithinGeoPointDocValuesAndFieldAndConstantEvaluator.Factory(source(), rightE, leftE, dist);
                        } else {
                            return new StDWithinGeoFieldAndFieldAndConstantEvaluator.Factory(source(), leftE, rightE, dist);
                        }
                    } else if (crsType == BinarySpatialFunction.SpatialCrsType.CARTESIAN) {
                        if (leftDocValues) {
                            return new StDWithinCartesianPointDocValuesAndFieldAndConstantEvaluator.Factory(source(), leftE, rightE, dist);
                        } else if (rightDocValues) {
                            return new StDWithinCartesianPointDocValuesAndFieldAndConstantEvaluator.Factory(source(), rightE, leftE, dist);
                        } else {
                            return new StDWithinCartesianFieldAndFieldAndConstantEvaluator.Factory(source(), leftE, rightE, dist);
                        }
                    }
                } else {
                    throw illegalDistance(distObj);
                }
            } else {
                EvalOperator.ExpressionEvaluator.Factory distanceE = toEvaluator.apply(arg());
                if (crsType == BinarySpatialFunction.SpatialCrsType.GEO) {
                    if (leftDocValues) {
                        return new StDWithinGeoPointDocValuesAndFieldAndFieldEvaluator.Factory(source(), leftE, rightE, distanceE);
                    } else if (rightDocValues) {
                        return new StDWithinGeoPointDocValuesAndFieldAndFieldEvaluator.Factory(source(), rightE, leftE, distanceE);
                    } else {
                        return new StDWithinGeoFieldAndFieldAndFieldEvaluator.Factory(source(), leftE, rightE, distanceE);
                    }
                } else if (crsType == BinarySpatialFunction.SpatialCrsType.CARTESIAN) {
                    if (leftDocValues) {
                        return new StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator.Factory(source(), leftE, rightE, distanceE);
                    } else if (rightDocValues) {
                        return new StDWithinCartesianPointDocValuesAndFieldAndFieldEvaluator.Factory(source(), rightE, leftE, distanceE);
                    } else {
                        return new StDWithinCartesianFieldAndFieldAndFieldEvaluator.Factory(source(), leftE, rightE, distanceE);
                    }
                }
            }
        }
        throw EsqlIllegalArgumentException.illegalDataType(crsType.name());
    }

    private IllegalArgumentException illegalDistance(Object value) {
        String cls = value.getClass().getSimpleName();
        return new IllegalArgumentException(
            "ST_DWITHIN third argument must be of type DOUBLE, but found literal [" + cls + "] of type [" + arg().dataType() + "]"
        );
    }

    private EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator,
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
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator,
        Expression field,
        Point point,
        boolean docValues
    ) {
        EvalOperator.ExpressionEvaluator.Factory fieldEvaluator = toEvaluator.apply(field);
        if (arg().foldable()) {
            Object distObj = valueOf(arg());
            if (distObj instanceof Number distValue) {
                double dist = distValue.doubleValue();

                if (crsType() == BinarySpatialFunction.SpatialCrsType.GEO) {
                    if (docValues) {
                        return new StDWithinGeoPointDocValuesAndConstantAndConstantEvaluator.Factory(source(), fieldEvaluator, point, dist);
                    } else {
                        return new StDWithinGeoFieldAndConstantAndConstantEvaluator.Factory(source(), fieldEvaluator, point, dist);
                    }
                } else if (crsType() == BinarySpatialFunction.SpatialCrsType.CARTESIAN) {
                    if (docValues) {
                        return new StDWithinCartesianPointDocValuesAndConstantAndConstantEvaluator.Factory(
                            source(),
                            fieldEvaluator,
                            point,
                            dist
                        );
                    } else {
                        return new StDWithinCartesianFieldAndConstantAndConstantEvaluator.Factory(source(), fieldEvaluator, point, dist);
                    }
                }
            }
        } else {

        }
        throw EsqlIllegalArgumentException.illegalDataType(crsType().name());
    }

    @Evaluator(extraName = "GeoFieldAndConstantAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoFieldAndConstant(BytesRef leftValue, @Fixed Point rightValue, @Fixed double argValue) throws IOException {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoFieldAndFieldAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoFieldAndField(BytesRef leftValue, BytesRef rightValue, @Fixed double argValue) throws IOException {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstantAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndConstant(long leftValue, @Fixed Point rightValue, @Fixed double argValue) {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoPointDocValuesAndFieldAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndField(long leftValue, BytesRef rightValue, @Fixed double argValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.distance(leftValue, geometry) < argValue;
    }

    @Evaluator(extraName = "CartesianFieldAndConstantAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianFieldAndConstant(BytesRef leftValue, @Fixed Point rightValue, @Fixed double argValue)
        throws IOException {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianFieldAndFieldAndConstant", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianFieldAndField(BytesRef leftValue, BytesRef rightValue, @Fixed double argValue) throws IOException {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstantAndConstant", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndConstant(long leftValue, @Fixed Point rightValue, @Fixed double argValue) {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndFieldAndConstant")
    static boolean processCartesianPointDocValuesAndField(long leftValue, BytesRef rightValue, @Fixed double argValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.distance(leftValue, geometry) < argValue;
    }

    @Evaluator(extraName = "GeoFieldAndConstantAndField", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoFieldAndConstantAndField(BytesRef leftValue, @Fixed Point rightValue, double argValue) throws IOException {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoFieldAndFieldAndField", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeoFieldAndFieldAndField(BytesRef leftValue, BytesRef rightValue, double argValue) throws IOException {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoPointDocValuesAndConstantAndField", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndConstantAndField(long leftValue, @Fixed Point rightValue, double argValue) {
        return GEO.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "GeoPointDocValuesAndFieldAndField", warnExceptions = { IllegalArgumentException.class })
    static boolean processGeoPointDocValuesAndFieldAndField(long leftValue, BytesRef rightValue, double argValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return GEO.distance(leftValue, geometry) < argValue;
    }

    @Evaluator(extraName = "CartesianFieldAndConstantAndField", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianFieldAndConstantAndField(BytesRef leftValue, @Fixed Point rightValue, double argValue)
        throws IOException {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianFieldAndFieldAndField", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesianFieldAndFieldAndField(BytesRef leftValue, BytesRef rightValue, double argValue) throws IOException {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndConstantAndField", warnExceptions = { IllegalArgumentException.class })
    static boolean processCartesianPointDocValuesAndConstantAndField(long leftValue, @Fixed Point rightValue, double argValue) {
        return CARTESIAN.distance(leftValue, rightValue) < argValue;
    }

    @Evaluator(extraName = "CartesianPointDocValuesAndFieldAndField")
    static boolean processCartesianPointDocValuesAndFieldAndField(long leftValue, BytesRef rightValue, double argValue) {
        Geometry geometry = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(rightValue);
        return CARTESIAN.distance(leftValue, geometry) < argValue;
    }
}
