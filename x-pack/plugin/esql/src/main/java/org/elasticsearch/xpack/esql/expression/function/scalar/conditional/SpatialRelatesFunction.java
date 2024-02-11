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
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
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
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.lucene.document.ShapeField.QueryRelation.DISJOINT;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;
import static org.elasticsearch.xpack.ql.expression.Expressions.name;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.planner.ExpressionTranslators.valueOf;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

public abstract class SpatialRelatesFunction extends BinaryScalarFunction implements EvaluatorMapper {
    protected SpatialCrsTypes crsType;
    protected final boolean useDocValues;

    protected SpatialRelatesFunction(Source source, Expression left, Expression right, boolean useDocValues) {
        super(source, left, right);
        this.useDocValues = useDocValues;
    }

    public abstract ShapeField.QueryRelation queryRelation();

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        // We determine the spatial data type first, then check if the other expression is of the same type or a string.
        TypeResolution leftResolution = isSpatial(left(), sourceText(), FIRST);
        TypeResolution rightResolution = isSpatial(right(), sourceText(), SECOND);
        // Both are not spatial, then both could be string literals
        if ((leftResolution.unresolved() || left().dataType().equals(NULL))
            && (rightResolution.unresolved() || right().dataType().equals(NULL))) {
            TypeResolution resolution = bothAreStringTypes(left(), right(), sourceText());
            if (resolution.unresolved() == false) {
                crsType = SpatialCrsTypes.UNSPECIFIED;
            }
            return resolution;
        }
        // Both are spatial, but one could be a literal spatial function
        if (leftResolution.resolved() && rightResolution.resolved()) {
            if (left().foldable() && right().foldable() == false || DataTypes.isNull(left().dataType())) {
                // Left is literal, but right is not, check the left field's type against the right field
                return resolveType(right(), left(), FIRST);
            } else {
                // All other cases check the right against the left
                return resolveType(left(), right(), SECOND);
            }
        }
        // Only one is spatial, the other must be a literal string
        if (leftResolution.unresolved()) {
            return resolveType(right(), left(), FIRST);
        } else {
            return resolveType(left(), right(), SECOND);
        }
    }

    protected TypeResolution resolveType(
        Expression spatialExpression,
        Expression otherExpression,
        TypeResolutions.ParamOrdinal otherParamOrdinal
    ) {
        TypeResolution resolution = isSameSpatialTypeOrString(
            spatialExpression.dataType(),
            otherExpression,
            sourceText(),
            otherParamOrdinal
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        crsType = SpatialCrsTypes.fromDataType(spatialExpression.dataType());
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
            dt -> dt == KEYWORD || EsqlDataTypes.isSpatial(dt) && spatialCRSCompatible(spatialDataType, dt),
            operationName,
            paramOrd,
            spatialDataType.esType(),
            "keyword",
            "text"
        );
    }

    private static boolean spatialCRSCompatible(DataType spatialDataType, DataType otherDataType) {
        return EsqlDataTypes.isSpatialGeo(spatialDataType) && EsqlDataTypes.isSpatialGeo(otherDataType)
            || EsqlDataTypes.isSpatialGeo(spatialDataType) == false && EsqlDataTypes.isSpatialGeo(otherDataType) == false;
    }

    public static TypeResolution bothAreStringTypes(Expression left, Expression right, String operationName) {
        return atLeastOneKeywordAndNoNonNulls(left, right)
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

    private static boolean atLeastOneKeywordAndNoNonNulls(Expression... expressions) {
        int countKeywords = 0;
        int countNulls = 0;
        for (Expression expression : expressions) {
            if (expression.dataType() == KEYWORD) {
                countKeywords++;
            }
            if (expression.dataType() == NULL) {
                countNulls++;
            }
        }
        return countKeywords > 0 && (countNulls + countKeywords == expressions.length);
    }

    protected static Component2D asLuceneComponent2D(SpatialCrsTypes crsType, Expression expression) {
        Object result = expression.fold();
        if (result instanceof String string) {
            return asLuceneComponent2D(crsType, SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(string));
        } else if (result instanceof BytesRef bytesRef) {
            if (expression.dataType() == KEYWORD) {
                return asLuceneComponent2D(crsType, SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(bytesRef.utf8ToString()));
            } else {
                return asLuceneComponent2D(crsType, SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef));
            }
        } else {
            throw new IllegalArgumentException("Invalid spatial constant string: " + result);
        }
    }

    protected static Component2D asLuceneComponent2D(SpatialCrsTypes crsType, Geometry geometry) {
        if (crsType == SpatialCrsTypes.GEO) {
            var luceneGeometries = LuceneGeometriesUtils.toLatLonGeometry(geometry, false, t -> {});
            return LatLonGeometry.create(luceneGeometries);
        } else {
            var luceneGeometries = LuceneGeometriesUtils.toXYGeometry(geometry, t -> {});
            return XYGeometry.create(luceneGeometries);
        }
    }

    protected static GeometryDocValueReader asGeometryDocValueReader(SpatialCrsTypes crsType, Expression expression) throws IOException {
        Object result = expression.fold();
        if (result instanceof String string) {
            return asGeometryDocValueReader(crsType, SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(string));
        } else if (result instanceof BytesRef bytesRef) {
            if (expression.dataType() == KEYWORD) {
                return asGeometryDocValueReader(crsType, SpatialCoordinateTypes.UNSPECIFIED.wktToGeometry(bytesRef.utf8ToString()));
            } else {
                return asGeometryDocValueReader(crsType, SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef));
            }
        } else {
            throw new IllegalArgumentException("Invalid spatial constant string: " + result);
        }
    }

    protected static GeometryDocValueReader asGeometryDocValueReader(SpatialCrsTypes crsType, Geometry geometry) throws IOException {
        if (crsType == SpatialCrsTypes.GEO) {
            return asGeometryDocValueReader(
                CoordinateEncoder.GEO,
                new GeoShapeIndexer(Orientation.CCW, "SpatialRelatesFunction"),
                geometry
            );
        } else {
            return asGeometryDocValueReader(CoordinateEncoder.CARTESIAN, new CartesianShapeIndexer("SpatialRelatesFunction"), geometry);
        }

    }

    protected static GeometryDocValueReader asGeometryDocValueReader(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        Geometry geometry
    ) throws IOException {
        GeometryDocValueReader reader = new GeometryDocValueReader();
        CentroidCalculator centroidCalculator = new CentroidCalculator();
        if (geometry instanceof Circle circle) {
            // TODO: How should we deal with circles?
            centroidCalculator.add(new Point(circle.getX(), circle.getY()));
        } else {
            centroidCalculator.add(geometry);
        }
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

    /**
     * Push-down to Lucene is only possible if one field is an indexed spatial field, and the other is a constant spatial or string column.
     */
    public boolean canPushToSource(Predicate<FieldAttribute> isAggregatable) {
        // The use of foldable here instead of SpatialEvaluatorFieldKey.isConstant is intentional to match the behavior of the
        // Lucene pushdown code in EsqlTranslationHandler::SpatialRelatesTranslator
        // We could enhance both places to support ReferenceAttributes that refer to constants, but that is a larger change
        return isPushableFieldAttribute(left(), isAggregatable) && right().foldable()
            || isPushableFieldAttribute(right(), isAggregatable) && left().foldable();
    }

    private static boolean isPushableFieldAttribute(Expression exp, Predicate<FieldAttribute> isAggregatable) {
        return exp instanceof FieldAttribute fa
            && fa.getExactInfo().hasExact()
            && isAggregatable.test(fa)
            && EsqlDataTypes.isSpatial(fa.dataType());
    }

    public Geometry makeGeometryFromLiteral(Expression expr) throws IOException, ParseException {
        Object value = valueOf(expr);

        if (value instanceof BytesRef bytesRef) {
            if (EsqlDataTypes.isSpatial(expr.dataType())) {
                return WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, bytesRef.bytes, bytesRef.offset, bytesRef.length);
            } else {
                return WellKnownText.fromWKT(GeometryValidator.NOOP, false, bytesRef.utf8ToString());
            }
        } else if (value instanceof String string) {
            return WellKnownText.fromWKT(GeometryValidator.NOOP, false, string);
        } else {
            throw new IllegalArgumentException(
                "Unsupported combination of literal [" + value.getClass().getSimpleName() + "] of type [" + expr.dataType() + "]"
            );
        }
    }

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
        if (crsType == null) {
            resolveType();
        }
        SpatialEvaluatorFactory<?, ?> factory = getSpatialEvaluatorFactory();
        return factory.get(toEvaluator);
    }

    protected SpatialEvaluatorFactory<?, ?> getSpatialEvaluatorFactory() {
        if (crsType == null) {
            resolveType();
        }
        var evaluatorKey = new SpatialEvaluatorKey(crsType, useDocValues, fieldKey(left()), fieldKey(right()));
        SpatialEvaluatorFactory<?, ?> factory = evaluatorRules().get(evaluatorKey);
        if (factory == null) {
            evaluatorKey = evaluatorKey.swapSides();
            factory = evaluatorRules().get(evaluatorKey);
            if (factory == null) {
                throw evaluatorKey.unsupported();
            }
            factory.swapSides();
        }
        return factory;
    }

    protected SpatialEvaluatorFieldKey fieldKey(Expression expression) {
        return new SpatialEvaluatorFieldKey(expression.dataType(), expression.foldable());
    }

    public boolean hasFieldAttribute(Set<Attribute> foundAttributes) {
        return left() instanceof FieldAttribute leftField && foundAttributes.contains(leftField)
            || right() instanceof FieldAttribute rightField && foundAttributes.contains(rightField);
    }

    protected abstract static class SpatialEvaluatorFactory<V extends Object, T extends Object> {
        protected final TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator;
        protected boolean swapSides = false;

        SpatialEvaluatorFactory(TriFunction<Source, V, T, EvalOperator.ExpressionEvaluator.Factory> factoryCreator) {
            this.factoryCreator = factoryCreator;
        }

        public abstract EvalOperator.ExpressionEvaluator.Factory get(
            Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
        );

        public void swapSides() {
            this.swapSides = this.swapSides == false;
        }
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
            if (swapSides) {
                return factoryCreator.apply(source(), toEvaluator.apply(right()), toEvaluator.apply(left()));
            } else {
                return factoryCreator.apply(source(), toEvaluator.apply(left()), toEvaluator.apply(right()));
            }
        }
    }

    protected class SpatialEvaluatorWithConstantFactory extends SpatialEvaluatorFactory<
        EvalOperator.ExpressionEvaluator.Factory,
        Component2D> {

        SpatialEvaluatorWithConstantFactory(
            TriFunction<
                Source,
                EvalOperator.ExpressionEvaluator.Factory,
                Component2D,
                EvalOperator.ExpressionEvaluator.Factory> factoryCreator
        ) {
            super(factoryCreator);
        }

        @Override
        public EvalOperator.ExpressionEvaluator.Factory get(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
            if (swapSides) {
                return factoryCreator.apply(source(), toEvaluator.apply(right()), asLuceneComponent2D(crsType, left()));
            } else {
                return factoryCreator.apply(source(), toEvaluator.apply(left()), asLuceneComponent2D(crsType, right()));
            }
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
                if (swapSides) {
                    return factoryCreator.apply(source(), asGeometryDocValueReader(crsType, right()), asLuceneComponent2D(crsType, left()));
                } else {
                    return factoryCreator.apply(source(), asGeometryDocValueReader(crsType, left()), asLuceneComponent2D(crsType, right()));
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to process spatial constant", e);
            }
        }
    }

    protected enum SpatialCrsTypes {
        GEO,
        CARTESIAN,
        UNSPECIFIED;

        public static SpatialCrsTypes fromDataType(DataType dataType) {
            return EsqlDataTypes.isSpatialGeo(dataType) ? SpatialCrsTypes.GEO
                : EsqlDataTypes.isSpatial(dataType) ? SpatialCrsTypes.CARTESIAN
                : SpatialCrsTypes.UNSPECIFIED;
        }
    }

    protected record SpatialEvaluatorFieldKey(DataType dataType, boolean isConstant) {}

    protected record SpatialEvaluatorKey(
        SpatialCrsTypes crsType,
        boolean useDocValues,
        SpatialEvaluatorFieldKey left,
        SpatialEvaluatorFieldKey right
    ) {
        SpatialEvaluatorKey withDocValues() {
            return new SpatialEvaluatorKey(crsType, true, left, right);
        }

        SpatialEvaluatorKey swapSides() {
            return new SpatialEvaluatorKey(crsType, useDocValues, right, left);
        }

        SpatialEvaluatorKey withConstants(boolean leftConstant, boolean rightConstant) {
            return new SpatialEvaluatorKey(
                crsType,
                useDocValues,
                new SpatialEvaluatorFieldKey(left.dataType, leftConstant),
                new SpatialEvaluatorFieldKey(right.dataType, rightConstant)
            );
        }

        static SpatialEvaluatorKey fromSourceAndConstant(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialCrsTypes.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, true)
            );
        }

        static SpatialEvaluatorKey fromSources(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialCrsTypes.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, false),
                new SpatialEvaluatorFieldKey(right, false)
            );
        }

        static SpatialEvaluatorKey fromConstants(DataType left, DataType right) {
            return new SpatialEvaluatorKey(
                SpatialCrsTypes.fromDataType(left),
                false,
                new SpatialEvaluatorFieldKey(left, true),
                new SpatialEvaluatorFieldKey(right, true)
            );
        }

        UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("Unsupported spatial relation combination: " + this);
        }
    }

    protected static class SpatialRelations {
        protected final ShapeField.QueryRelation queryRelation;
        protected final SpatialCoordinateTypes spatialCoordinateType;
        protected final CoordinateEncoder coordinateEncoder;
        protected final ShapeIndexer shapeIndexer;
        protected final SpatialCrsTypes crsType;

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
            this.crsType = spatialCoordinateType.equals(SpatialCoordinateTypes.GEO) ? SpatialCrsTypes.GEO : SpatialCrsTypes.CARTESIAN;
        }

        protected boolean geometryRelatesGeometry(BytesRef left, BytesRef right) throws IOException {
            Geometry rightGeom = fromBytesRef(right);
            if (rightGeom instanceof Point point) {
                Geometry leftGeom = fromBytesRef(left);
                return pointRelatesGeometry(point, leftGeom);
            } else {
                // Convert one of the geometries to a doc-values byte array, and then visit the other geometry as a Component2D
                Component2D rightComponent2D = makeComponent2D(rightGeom);
                return geometryRelatesGeometry(left, rightComponent2D);
            }
        }

        protected Geometry fromBytesRef(BytesRef bytesRef) {
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
        }

        protected boolean geometryRelatesGeometry(BytesRef left, Component2D rightComponent2D) throws IOException {
            Geometry leftGeom = fromBytesRef(left);
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
            return asLuceneComponent2D(crsType, geometry);
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
