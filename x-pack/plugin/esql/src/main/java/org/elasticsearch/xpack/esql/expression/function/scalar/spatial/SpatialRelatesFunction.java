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
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
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
            // We already have a Component2D for the right geometry, so we need to convert the left geometry to a doc-values byte array
            return geometryRelatesGeometry(asGeometryDocValueReader(coordinateEncoder, shapeIndexer, fromBytesRef(left)), rightComponent2D);
        }

        protected boolean geometryRelatesGeometry(GeometryDocValueReader reader, Component2D rightComponent2D) throws IOException {
            var visitor = Component2DVisitor.getVisitor(rightComponent2D, queryRelation, coordinateEncoder);
            reader.visit(visitor);
            return visitor.matches();
        }

        protected void processSourceAndConstant(BooleanBlock.Builder builder, int position, BytesRefBlock left, @Fixed Component2D right)
            throws IOException {
            if (left.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                final GeometryDocValueReader reader = asGeometryDocValueReader(coordinateEncoder, shapeIndexer, left, position);
                builder.appendBoolean(geometryRelatesGeometry(reader, right));
            }
        }

        protected void processSourceAndSource(BooleanBlock.Builder builder, int position, BytesRefBlock left, BytesRefBlock right)
            throws IOException {
            if (left.getValueCount(position) < 1 || right.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                final GeometryDocValueReader reader = asGeometryDocValueReader(coordinateEncoder, shapeIndexer, left, position);
                final Component2D component2D = asLuceneComponent2D(crsType, right, position);
                builder.appendBoolean(geometryRelatesGeometry(reader, component2D));
            }
        }

        protected void processPointDocValuesAndConstant(
            BooleanBlock.Builder builder,
            int position,
            LongBlock leftValue,
            @Fixed Component2D rightValue
        ) throws IOException {
            if (leftValue.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                final GeometryDocValueReader reader = asGeometryDocValueReader(
                    coordinateEncoder,
                    shapeIndexer,
                    leftValue,
                    position,
                    spatialCoordinateType::longAsPoint
                );
                builder.appendBoolean(geometryRelatesGeometry(reader, rightValue));
            }
        }

        protected void processPointDocValuesAndSource(
            BooleanBlock.Builder builder,
            int position,
            LongBlock leftValue,
            BytesRefBlock rightValue
        ) throws IOException {
            if (leftValue.getValueCount(position) < 1 || rightValue.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                final GeometryDocValueReader reader = asGeometryDocValueReader(
                    coordinateEncoder,
                    shapeIndexer,
                    leftValue,
                    position,
                    spatialCoordinateType::longAsPoint
                );
                final Component2D component2D = asLuceneComponent2D(crsType, rightValue, position);
                builder.appendBoolean(geometryRelatesGeometry(reader, component2D));
            }
        }
    }
}
