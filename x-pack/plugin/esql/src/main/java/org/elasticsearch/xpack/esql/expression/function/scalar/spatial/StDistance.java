/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * Extracts the x-coordinate from a point geometry.
 * For cartesian geometries, the x-coordinate is the first coordinate.
 * For geographic geometries, the x-coordinate is the longitude.
 * The function `st_x` is defined in the <a href="https://www.ogc.org/standard/sfs/">OGC Simple Feature Access</a> standard.
 * Alternatively it is well described in PostGIS documentation at <a href="https://postgis.net/docs/ST_X.html">PostGIS:ST_X</a>.
 */
public class StDistance extends BinarySpatialFunction implements EvaluatorMapper, SpatialEvaluatorFactory.SpatialSourceSupplier {
    // public for test access with reflection
    public static final DistanceCalculator GEO = new DistanceCalculator(
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Distance")
    );
    // public for test access with reflection
    public static final DistanceCalculator CARTESIAN = new DistanceCalculator(
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Distance")
    );

    protected static class DistanceCalculator extends BinarySpatialComparator {

        protected DistanceCalculator(SpatialCoordinateTypes spatialCoordinateType, CoordinateEncoder encoder, ShapeIndexer shapeIndexer) {
            super(spatialCoordinateType, encoder, shapeIndexer);
        }

        protected double distance(Point left, Point right) {
            // TODO differentiate between GEO and CARTESIAN
            return Math.sqrt(Math.pow(left.getX() - right.getX(), 2) + Math.pow(left.getY() - right.getY(), 2));
        }

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
        description = "Extracts the `x` coordinate from the supplied point.\n"
            + "If the points is of type `geo_point` this is equivalent to extracting the `longitude` value.",
        examples = @Example(file = "spatial", tag = "st_x_y")
    )
    public StDistance(
        Source source,
        @Param(
            name = "left",
            type = { "geo_point", "cartesian_point" },
            description = "Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`."
        ) Expression left,
        @Param(
            name = "right",
            type = { "geo_point", "cartesian_point" },
            description = "Expression of type `geo_point` or `cartesian_point`. If `null`, the function returns `null`."
        ) Expression right
    ) {
        super(source, left, right, false, false);
    }

    private StDistance(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues);
    }

    @Override
    protected Expression.TypeResolution isSpatial(Expression e, TypeResolutions.ParamOrdinal paramOrd) {
        // We currently only support points for ST_DISTANCE
        return EsqlTypeResolutions.isSpatialPoint(e, sourceText(), paramOrd);
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

    @ConvertEvaluator(extraName = "FromWKB", warnExceptions = { IllegalArgumentException.class })
    static double fromWellKnownBinary(BytesRef in) {
        return UNSPECIFIED.wkbAsPoint(in).getX();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        DataType type = left().dataType();
        // if (leftDocValues) {
        // // When the points are read as doc-values (eg. from the index), feed them into the doc-values aggregator
        // if (crsType == SpatialCrsType.GEO) {
        // return new SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // if (type == EsqlDataTypes.CARTESIAN_POINT) {
        // return new SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // } else if (rightDocValues) {
        // // When the points are read as doc-values (eg. from the index), feed them into the doc-values aggregator
        // if (type == EsqlDataTypes.GEO_POINT) {
        // return new SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // if (type == EsqlDataTypes.CARTESIAN_POINT) {
        // return new SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // } else {
        // // When the points are read as WKB from source or as point literals, feed them into the source-values aggregator
        // if (type == EsqlDataTypes.GEO_POINT) {
        // return new SpatialCentroidGeoPointSourceValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // if (type == EsqlDataTypes.CARTESIAN_POINT) {
        // return new SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier(inputChannels);
        // }
        // }
        throw EsqlIllegalArgumentException.illegalDataType(type);
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
