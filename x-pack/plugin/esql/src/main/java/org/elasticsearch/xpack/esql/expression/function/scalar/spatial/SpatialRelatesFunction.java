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
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.lucene.document.ShapeField.QueryRelation.CONTAINS;
import static org.apache.lucene.document.ShapeField.QueryRelation.DISJOINT;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;

public abstract class SpatialRelatesFunction extends BinarySpatialFunction
    implements
        EvaluatorMapper,
        SpatialEvaluatorFactory.SpatialSourceSupplier {

    protected SpatialRelatesFunction(Source source, Expression left, Expression right, boolean leftDocValues, boolean rightDocValues) {
        super(source, left, right, leftDocValues, rightDocValues, false);
    }

    protected SpatialRelatesFunction(StreamInput in, boolean leftDocValues, boolean rightDocValues) throws IOException {
        super(in, leftDocValues, rightDocValues, false);
    }

    public abstract ShapeRelation queryRelation();

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
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
            && DataType.isSpatial(fa.dataType());
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

    /**
     * Produce a map of rules defining combinations of incoming types to the evaluator factory that should be used.
     */
    abstract Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules();

    /**
     * Some spatial functions can replace themselves with alternatives that are more efficient for certain cases.
     */
    public SpatialRelatesFunction surrogate() {
        return this;
    }

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

    protected static class SpatialRelations extends BinarySpatialComparator<Boolean> {
        protected final ShapeField.QueryRelation queryRelation;
        protected final ShapeIndexer shapeIndexer;

        protected SpatialRelations(
            ShapeField.QueryRelation queryRelation,
            SpatialCoordinateTypes spatialCoordinateType,
            CoordinateEncoder encoder,
            ShapeIndexer shapeIndexer
        ) {
            super(spatialCoordinateType, encoder);
            this.queryRelation = queryRelation;
            this.shapeIndexer = shapeIndexer;
        }

        @Override
        protected Boolean compare(BytesRef left, BytesRef right) throws IOException {
            return geometryRelatesGeometry(left, right);
        }

        protected boolean geometryRelatesGeometry(BytesRef left, BytesRef right) throws IOException {
            Component2D rightComponent2D = asLuceneComponent2D(crsType, fromBytesRef(right));
            return geometryRelatesGeometry(left, rightComponent2D);
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
            return pointRelatesGeometry(point, component2D);
        }

        protected boolean pointRelatesGeometry(Point point, Component2D component2D) {
            if (queryRelation == CONTAINS) {
                return component2D.withinPoint(point.getX(), point.getY()) == Component2D.WithinRelation.CANDIDATE;
            } else {
                boolean contains = component2D.contains(point.getX(), point.getY());
                return queryRelation == DISJOINT ? contains == false : contains;
            }
        }
    }
}
