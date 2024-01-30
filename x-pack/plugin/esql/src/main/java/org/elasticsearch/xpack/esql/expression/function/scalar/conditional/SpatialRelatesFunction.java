/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.geo.LuceneGeometriesUtils;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.lucene.spatial.GeometryDocValueWriter;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.lucene.document.ShapeField.QueryRelation.DISJOINT;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.ql.expression.Expressions.name;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.type.DataTypes.isString;

abstract class SpatialRelatesFunction extends BinaryScalarFunction implements EvaluatorMapper {
    protected DataType spatialDataType;
    protected final boolean useDocValues;

    protected SpatialRelatesFunction(Source source, Expression left, Expression right, boolean useDocValues) {
        super(source, left, right);
        this.useDocValues = useDocValues;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        // We determine the spatial data type first, then check if the other expression is of the same type or a string.
        TypeResolution leftResolution = isSpatial(left(), sourceText(), FIRST);
        TypeResolution rightResolution = isSpatial(right(), sourceText(), SECOND);
        if (leftResolution.unresolved() && rightResolution.unresolved()) {
            TypeResolution resolution = bothAreStringTypes(left(), right(), sourceText());
            if (resolution.unresolved() == false) {
                spatialDataType = CARTESIAN_SHAPE;
            }
            return resolution;
        }
        Expression spatialExpression = leftResolution.unresolved() ? right() : left();
        Expression otherExpression = leftResolution.unresolved() ? left() : right();
        TypeResolutions.ParamOrdinal otherParamOrdinal = leftResolution.unresolved() ? FIRST : SECOND;
        spatialDataType = spatialExpression.dataType();
        TypeResolution resolution = isSameSpatialTypeOrString(spatialDataType, otherExpression, sourceText(), otherParamOrdinal);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isSameSpatialTypeOrString(
        DataType spatialDataType,
        Expression expression,
        String operationName,
        TypeResolutions.ParamOrdinal paramOrd
    ) {
        return isType(
            expression,
            dt -> isString(dt) || EsqlDataTypes.isSpatial(dt) && spatialDataType.equals(dt),
            operationName,
            paramOrd,
            spatialDataType.esType(),
            "keyword",
            "text"
        );
    }

    public static TypeResolution bothAreStringTypes(Expression left, Expression right, String operationName) {
        return DataTypes.isString(left.dataType()) && DataTypes.isString(right.dataType())
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution(
                format(
                    null,
                    "when neither arguments of [{}] are [{}], both must be [{}], found value [{}] type [{}] and value [{}] type [{}]",
                    operationName,
                    "geo_point or geo_shape or cartesian_point or cartesian_shape",
                    "keyword or text",
                    name(left),
                    left.dataType().typeName(),
                    name(right),
                    right.dataType().typeName()
                )
            );
    }

    protected static Component2D asLuceneComponent2D(DataType spatialDataType, Expression expression) {
        Object result = expression.fold();
        if (result instanceof BytesRef bytesRef) {
            return asLuceneComponent2D(spatialDataType, SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(bytesRef.utf8ToString()));
        } else {
            throw new IllegalArgumentException("Invalid spatial constant string: " + result);
        }
    }

    protected static Component2D asLuceneComponent2D(DataType spatialDataType, Geometry geometry) {
        if (EsqlDataTypes.isSpatialGeo(spatialDataType)) {
            var luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, false, t -> {});
            return LatLonGeometry.create(luceneGeometries);
        } else {
            var luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            return XYGeometry.create(luceneGeometries);
        }
    }

    protected static GeometryDocValueReader asGeometryDocValueReader(DataType spatialDataType, Expression expression) throws IOException {
        Object result = expression.fold();
        if (result instanceof BytesRef bytesRef) {
            var geometry = SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(bytesRef.utf8ToString());
            if (EsqlDataTypes.isSpatialGeo(spatialDataType)) {
                return asGeometryDocValueReader(
                    CoordinateEncoder.GEO,
                    new GeoShapeIndexer(Orientation.CCW, "SpatialRelatesFunction"),
                    geometry
                );
            } else {
                return asGeometryDocValueReader(CoordinateEncoder.CARTESIAN, new CartesianShapeIndexer("SpatialRelatesFunction"), geometry);
            }
        } else {
            throw new IllegalArgumentException("Invalid spatial constant string: " + result);
        }
    }

    protected static GeometryDocValueReader asGeometryDocValueReader(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        Geometry geometry
    ) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        reader.reset(GeometryDocValueWriter.write(shapeIndexer.indexShape(geometry), encoder, centroidCalculator));
        return reader;
    }

    @Override
    public boolean foldable() {
        return left().foldable() && right().foldable();
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    public abstract SpatialRelatesFunction withDocValues();

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), useDocValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialRelatesFunction other = (SpatialRelatesFunction) obj;
            return Objects.equals(other.children(), children()) && Objects.equals(other.useDocValues, useDocValues);
        }
        return false;
    }

    public boolean useDocValues() {
        return useDocValues;
    }

    /**
     * Produce a map of rules defining combinations of incoming types to the evaluator factory that should be used.
     */
    protected abstract Map<SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules();

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        if (spatialDataType == null) {
            resolveType();
        }
        SpatialEvaluatorFactory<?, ?> factory = getSpatialEvaluatorFactory();
        return factory.get(toEvaluator);
    }

    protected SpatialEvaluatorFactory<?, ?> getSpatialEvaluatorFactory() {
        if (spatialDataType == null) {
            resolveType();
        }
        var evaluatorKey = new SpatialEvaluatorKey(
            spatialDataType,
            useDocValues,
            isConstantExpression(left()),
            isConstantExpression(right())
        );
        SpatialEvaluatorFactory<?, ?> factory = evaluatorRules().get(evaluatorKey);
        if (factory == null) {
            throw evaluatorKey.unsupported();
        }
        return factory;
    }

    protected boolean isConstantExpression(Expression expression) {
        return isString(expression.dataType()) && expression.foldable();
    }

    protected abstract static class SpatialEvaluatorFactory<V extends Object, T extends Object> {
        protected final TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator;

        SpatialEvaluatorFactory(TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator) {
            this.factoryCreator = factoryCreator;
        }

        public abstract EvalOperator.ExpressionEvaluator.Factory get(
            Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
        );
    }

    protected class SpatialEvaluatorFactoryWithFields extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        EvalOperator.ExpressionEvaluator.Factory> {
        SpatialEvaluatorFactoryWithFields(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                EvalOperator.ExpressionEvaluator.Factory,
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
            return factoryCreator.apply(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
        }
    }

    protected class SpatialEvaluatorWithConstantFactory extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        Component2D> {
        private final boolean swapSides;

        SpatialEvaluatorWithConstantFactory(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                Component2D,
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator,
            boolean swapSides
        ) {
            super(factoryCreator);
            this.swapSides = swapSides;
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
            return swapSides
                ? factoryCreator.apply(source(), toEvaluator.apply(right()), asLuceneComponent2D(spatialDataType, left()))
                : factoryCreator.apply(source(), toEvaluator.apply(left()), asLuceneComponent2D(spatialDataType, right()));
        }
    }

    protected class SpatialEvaluatorWithConstantsFactory extends SpatialEvaluatorFactory<GeometryDocValueReader, Component2D> {

        SpatialEvaluatorWithConstantsFactory(
            TriFunction<Source, GeometryDocValueReader, Component2D, EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
            try {
                return factoryCreator.apply(
                    source(),
                    asGeometryDocValueReader(spatialDataType, left()),
                    asLuceneComponent2D(spatialDataType, right())
                );
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to process spatial constant", e);
            }
        }
    }

    protected record SpatialEvaluatorKey(DataType dataType, boolean useDocValues, boolean leftIsConstant, boolean rightIsConstant) {
        UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("Unsupported spatial relation combination: " + this);
        }
    }

    protected static class SpatialRelations {
        protected final ShapeField.QueryRelation queryRelation;
        protected final SpatialCoordinateTypes spatialCoordinateType;
        protected final CoordinateEncoder coordinateEncoder;
        protected final ShapeIndexer shapeIndexer;
        protected final DataType spatialDataType;

        protected SpatialRelations(
            ShapeField.QueryRelation queryRelation,
            SpatialCoordinateTypes spatialCoordinateType,
            CoordinateEncoder encoder,
            ShapeIndexer shapeIndexer
        ) {
            this.queryRelation = queryRelation;
            this.spatialCoordinateType = spatialCoordinateType;
            this.coordinateEncoder = encoder;
            this.shapeIndexer = shapeIndexer;
            this.spatialDataType = spatialCoordinateType.equals(SpatialCoordinateTypes.GEO) ? GEO_SHAPE : CARTESIAN_SHAPE;
        }

        protected boolean geometryRelatesGeometry(BytesRef left, BytesRef right) throws IOException {
            Geometry rightGeom = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(right);
            if (rightGeom instanceof Point point) {
                Geometry leftGeom = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(left);
                return pointRelatesGeometry(point, leftGeom);
            } else {
                // Convert one of the geometries to a doc-values byte array, and then visit the other geometry as a Component2D
                Component2D rightComponent2D = makeComponent2D(rightGeom);
                return geometryRelatesGeometry(left, rightComponent2D);
            }
        }

        protected boolean geometryRelatesGeometry(BytesRef left, Component2D rightComponent2D) throws IOException {
            Geometry leftGeom = SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(left);
            if (leftGeom instanceof Point point) {
                return geometryRelatesPoint(rightComponent2D, point);
            } else {
                // We already have a Component2D for the right geometry, so we need to convert the left geometry to a doc-values byte array
                return geometryRelatesGeometry(asGeometryDocValueReader(coordinateEncoder, shapeIndexer, leftGeom), rightComponent2D);
            }
        }

        protected boolean geometryRelatesGeometry(GeometryDocValueReader reader, Component2D rightComponent2D) throws IOException {
            var visitor = Component2DVisitor.getVisitor(rightComponent2D, queryRelation, coordinateEncoder);
            reader.visit(visitor);
            return visitor.matches();
        }

        protected boolean pointRelatesGeometry(long encoded, Geometry geometry) {
            if (geometry instanceof Point point) {
                long other = spatialCoordinateType.pointAsLong(point.getX(), point.getY());
                return pointRelatesPoint(encoded, other);
            } else {
                Point point = spatialCoordinateType.longAsPoint(encoded);
                return pointRelatesGeometry(point, geometry);
            }
        }

        protected boolean pointRelatesGeometry(long encoded, Component2D component2D) {
            Point point = spatialCoordinateType.longAsPoint(encoded);
            return geometryRelatesPoint(component2D, point);
        }

        protected Component2D makeComponent2D(Geometry geometry) {
            return asLuceneComponent2D(spatialDataType, geometry);
        }

        protected boolean pointRelatesGeometry(Point leftPoint, Geometry geometry) {
            if (geometry instanceof Point rightPoint) {
                long leftEncoded = spatialCoordinateType.pointAsLong(leftPoint.getX(), leftPoint.getY());
                long rightEncoded = spatialCoordinateType.pointAsLong(rightPoint.getX(), rightPoint.getY());
                return pointRelatesPoint(leftEncoded, rightEncoded);
            } else {
                Component2D component2D = makeComponent2D(geometry);
                return geometryRelatesPoint(component2D, leftPoint);
            }
        }

        private boolean pointRelatesPoint(long left, long right) {
            boolean intersects = left == right;
            return queryRelation == DISJOINT ? intersects == false : intersects;
        }

        private boolean geometryRelatesPoint(Component2D component2D, Point point) {
            boolean contains = component2D.contains(point.getX(), point.getY());
            return queryRelation == DISJOINT ? contains == false : contains;
        }
    }
}
