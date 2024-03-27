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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.lucene.document.ShapeField.QueryRelation.DISJOINT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.type.DataTypes.isNull;

public abstract class SpatialRelatesFunction extends BinaryScalarFunction
    implements
        EvaluatorMapper,
        SpatialEvaluatorFactory.SpatialSourceSupplier {
    protected SpatialCrsType crsType;
    protected final boolean leftDocValues;
    protected final boolean rightDocValues;

    protected SpatialRelatesFunction(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right);
        this.leftDocValues = leftDocValues;
        this.rightDocValues = rightDocValues;
    }

    public abstract ShapeField.QueryRelation queryRelation();

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public SpatialCrsType crsType() {
        if (crsType == null) {
            resolveType();
        }
        return crsType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (left().foldable() && right().foldable() == false || isNull(left().dataType())) {
            // Left is literal, but right is not, check the left field's type against the right field
            return resolveType(right(), left(), SECOND, FIRST);
        } else {
            // All other cases check the right against the left
            return resolveType(left(), right(), FIRST, SECOND);
        }
    }

    private TypeResolution resolveType(
        Expression leftExpression,
        Expression rightExpression,
        TypeResolutions.ParamOrdinal leftOrdinal,
        TypeResolutions.ParamOrdinal rightOrdinal
    ) {
        TypeResolution leftResolution = isSpatial(leftExpression, sourceText(), leftOrdinal);
        TypeResolution rightResolution = isSpatial(rightExpression, sourceText(), rightOrdinal);
        if (leftResolution.resolved()) {
            return resolveType(leftExpression, rightExpression, rightOrdinal);
        } else if (rightResolution.resolved()) {
            return resolveType(rightExpression, leftExpression, leftOrdinal);
        } else {
            return leftResolution;
        }
    }

    protected TypeResolution resolveType(
        Expression spatialExpression,
        Expression otherExpression,
        TypeResolutions.ParamOrdinal otherParamOrdinal
    ) {
        if (isNull(spatialExpression.dataType())) {
            return isSpatial(otherExpression, sourceText(), otherParamOrdinal);
        }
        TypeResolution resolution = isSameSpatialType(spatialExpression.dataType(), otherExpression, sourceText(), otherParamOrdinal);
        if (resolution.unresolved()) {
            return resolution;
        }
        crsType = SpatialCrsType.fromDataType(spatialExpression.dataType());
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isSameSpatialType(
        DataType spatialDataType,
        Expression expression,
        String operationName,
        TypeResolutions.ParamOrdinal paramOrd
    ) {
        return isType(
            expression,
            dt -> EsqlDataTypes.isSpatial(dt) && spatialCRSCompatible(spatialDataType, dt),
            operationName,
            paramOrd,
            compatibleTypeNames(spatialDataType)
        );
    }

    private static final String[] GEO_TYPE_NAMES = new String[] { GEO_POINT.typeName(), GEO_SHAPE.typeName() };
    private static final String[] CARTESIAN_TYPE_NAMES = new String[] { GEO_POINT.typeName(), GEO_SHAPE.typeName() };

    private static boolean spatialCRSCompatible(DataType spatialDataType, DataType otherDataType) {
        return EsqlDataTypes.isSpatialGeo(spatialDataType) && EsqlDataTypes.isSpatialGeo(otherDataType)
            || EsqlDataTypes.isSpatialGeo(spatialDataType) == false && EsqlDataTypes.isSpatialGeo(otherDataType) == false;
    }

    static String[] compatibleTypeNames(DataType spatialDataType) {
        return EsqlDataTypes.isSpatialGeo(spatialDataType) ? GEO_TYPE_NAMES : CARTESIAN_TYPE_NAMES;
    }

    @Override
    public boolean foldable() {
        return left().foldable() && right().foldable();
    }

    /**
     * Mark the function as expecting the specified fields to arrive as doc-values.
     */
    public abstract SpatialRelatesFunction withDocValues(Set<FieldAttribute> attributes);

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

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), leftDocValues, rightDocValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialRelatesFunction other = (SpatialRelatesFunction) obj;
            return Objects.equals(other.children(), children())
                && Objects.equals(other.leftDocValues, leftDocValues)
                && Objects.equals(other.rightDocValues, rightDocValues);
        }
        return false;
    }

    public boolean leftDocValues() {
        return leftDocValues;
    }

    public boolean rightDocValues() {
        return rightDocValues;
    }

    /**
     * Produce a map of rules defining combinations of incoming types to the evaluator factory that should be used.
     */
    protected abstract Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules();

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        return SpatialEvaluatorFactory.makeSpatialEvaluator(this, evaluatorRules(), toEvaluator);
    }

    /**
     * When performing local physical plan optimization, it is necessary to know if this function has a field attribute.
     * This is because the planner might push down a spatial aggregation to lucene, which results in the field being provided
     * as doc-values instead of source values, and this function needs to know if it should use doc-values or not.
     */
    public boolean hasFieldAttribute(Set<FieldAttribute> foundAttributes) {
        return foundField(left(), foundAttributes) || foundField(right(), foundAttributes);
    }

    protected boolean foundField(Expression expression, Set<FieldAttribute> foundAttributes) {
        return expression instanceof FieldAttribute field && foundAttributes.contains(field);
    }

    protected enum SpatialCrsType {
        GEO,
        CARTESIAN,
        UNSPECIFIED;

        public static SpatialCrsType fromDataType(DataType dataType) {
            return EsqlDataTypes.isSpatialGeo(dataType) ? SpatialCrsType.GEO
                : EsqlDataTypes.isSpatial(dataType) ? SpatialCrsType.CARTESIAN
                : SpatialCrsType.UNSPECIFIED;
        }
    }

    protected static class SpatialRelations {
        protected final ShapeField.QueryRelation queryRelation;
        protected final SpatialCoordinateTypes spatialCoordinateType;
        protected final CoordinateEncoder coordinateEncoder;
        protected final ShapeIndexer shapeIndexer;
        protected final SpatialCrsType crsType;

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
            this.crsType = spatialCoordinateType.equals(SpatialCoordinateTypes.GEO) ? SpatialCrsType.GEO : SpatialCrsType.CARTESIAN;
        }

        protected boolean geometryRelatesGeometry(BytesRef left, BytesRef right) throws IOException {
            Component2D rightComponent2D = asLuceneComponent2D(crsType, fromBytesRef(right));
            return geometryRelatesGeometry(left, rightComponent2D);
        }

        private Geometry fromBytesRef(BytesRef bytesRef) {
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
        }

        protected boolean geometryRelatesGeometry(BytesRef left, Component2D rightComponent2D) throws IOException {
            Geometry leftGeom = fromBytesRef(left);
            // We already have a Component2D for the right geometry, so we need to convert the left geometry to a doc-values byte array
            return geometryRelatesGeometry(asGeometryDocValueReader(coordinateEncoder, shapeIndexer, leftGeom), rightComponent2D);
        }

        protected boolean geometryRelatesGeometry(GeometryDocValueReader reader, Component2D rightComponent2D) throws IOException {
            var visitor = Component2DVisitor.getVisitor(rightComponent2D, queryRelation, coordinateEncoder);
            reader.visit(visitor);
            return visitor.matches();
        }

        protected boolean pointRelatesGeometry(long encoded, Geometry geometry) {
            Component2D component2D = asLuceneComponent2D(crsType, geometry);
            return pointRelatesGeometry(encoded, component2D);
        }

        protected boolean pointRelatesGeometry(long encoded, Component2D component2D) {
            // This code path exists for doc-values points, and we could consider re-using the point class to reduce garbage creation
            Point point = spatialCoordinateType.longAsPoint(encoded);
            return geometryRelatesPoint(component2D, point);
        }

        private boolean geometryRelatesPoint(Component2D component2D, Point point) {
            boolean contains = component2D.contains(point.getX(), point.getY());
            return queryRelation == DISJOINT ? contains == false : contains;
        }
    }
}
