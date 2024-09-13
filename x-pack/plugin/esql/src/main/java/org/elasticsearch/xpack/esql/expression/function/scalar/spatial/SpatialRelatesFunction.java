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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.MultiPoint;
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
import java.util.ArrayList;
import java.util.List;
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

        protected boolean geometryRelatesGeometries(MultiValuesCombiner left, MultiValuesCombiner right) throws IOException {
            Component2D rightComponent2D = asLuceneComponent2D(crsType, right.combined());
            return geometryRelatesGeometry(left, rightComponent2D);
        }

        private boolean geometryRelatesGeometry(MultiValuesCombiner left, Component2D rightComponent2D) throws IOException {
            GeometryDocValueReader leftDocValueReader = asGeometryDocValueReader(coordinateEncoder, shapeIndexer, left.combined());
            return geometryRelatesGeometry(leftDocValueReader, rightComponent2D);
        }

        protected void processSourceAndConstant(BooleanBlock.Builder builder, int position, BytesRefBlock left, @Fixed Component2D right)
            throws IOException {
            if (left.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                MultiValuesBytesRef leftValues = new MultiValuesBytesRef(left, position);
                builder.appendBoolean(geometryRelatesGeometry(leftValues, right));
            }
        }

        protected void processSourceAndSource(BooleanBlock.Builder builder, int position, BytesRefBlock left, BytesRefBlock right)
            throws IOException {
            if (left.getValueCount(position) < 1 || right.getValueCount(position) < 1) {
                builder.appendNull();
            } else {
                MultiValuesBytesRef leftValues = new MultiValuesBytesRef(left, position);
                MultiValuesBytesRef rightValues = new MultiValuesBytesRef(right, position);
                builder.appendBoolean(geometryRelatesGeometries(leftValues, rightValues));
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
                MultiValuesLong leftValues = new MultiValuesLong(leftValue, position, spatialCoordinateType::longAsPoint);
                builder.appendBoolean(geometryRelatesGeometry(leftValues, rightValue));
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
                MultiValuesLong leftValues = new MultiValuesLong(leftValue, position, spatialCoordinateType::longAsPoint);
                MultiValuesBytesRef rightValues = new MultiValuesBytesRef(rightValue, position);
                builder.appendBoolean(geometryRelatesGeometries(leftValues, rightValues));
            }
        }
    }

    /**
     * When dealing with ST_CONTAINS and ST_WITHIN we need to pre-combine the field geometries for multi-values in order
     * to perform the relationship check. This means instead of relying on the generated evaluators to iterate over all
     * values in a multi-value field, the entire block is passed into the spatial function, and we combine the values into
     * a geometry collection or multipoint.
     */
    protected interface MultiValuesCombiner {
        Geometry combined();
    }

    /**
     * Values read from source will be encoded as WKB in BytesRefBlock. The block contains multiple rows, and within
     * each row multiple values, so we need to efficiently iterate over only the values required for the requested row.
     * This class works for point and shape fields, because both are extracted into the same block encoding.
     * However, we do detect if all values in the field are actually points and create a MultiPoint instead of a GeometryCollection.
     */
    protected static class MultiValuesBytesRef implements MultiValuesCombiner {
        private final BytesRefBlock valueBlock;
        private final int valueCount;
        private final BytesRef scratch = new BytesRef();
        private final int firstValue;

        MultiValuesBytesRef(BytesRefBlock valueBlock, int position) {
            this.valueBlock = valueBlock;
            this.firstValue = valueBlock.getFirstValueIndex(position);
            this.valueCount = valueBlock.getValueCount(position);
        }

        @Override
        public Geometry combined() {
            int valueIndex = firstValue;
            boolean allPoints = true;
            if (valueCount == 1) {
                return fromBytesRef(valueBlock.getBytesRef(valueIndex, scratch));
            }
            List<Geometry> geometries = new ArrayList<>();
            while (valueIndex < firstValue + valueCount) {
                geometries.add(fromBytesRef(valueBlock.getBytesRef(valueIndex++, scratch)));
                if (geometries.get(geometries.size() - 1) instanceof Point == false) {
                    allPoints = false;
                }
            }
            return allPoints ? new MultiPoint(asPointList(geometries)) : new GeometryCollection<>(geometries);
        }

        private List<Point> asPointList(List<Geometry> geometries) {
            List<Point> points = new ArrayList<>(geometries.size());
            for (Geometry geometry : geometries) {
                points.add((Point) geometry);
            }
            return points;
        }

        protected Geometry fromBytesRef(BytesRef bytesRef) {
            return SpatialCoordinateTypes.UNSPECIFIED.wkbToGeometry(bytesRef);
        }
    }

    /**
     * Point values read from doc-values will be encoded as in LogBlock. The block contains multiple rows, and within
     * each row multiple values, so we need to efficiently iterate over only the values required for the requested row.
     * Since the encoding differs for GEO and CARTESIAN, we need the decoder function to be passed in the constructor.
     */
    protected static class MultiValuesLong implements MultiValuesCombiner {
        private final LongBlock valueBlock;
        private final Function<Long, Point> decoder;
        private final int valueCount;
        private final int firstValue;

        MultiValuesLong(LongBlock valueBlock, int position, Function<Long, Point> decoder) {
            this.valueBlock = valueBlock;
            this.decoder = decoder;
            this.firstValue = valueBlock.getFirstValueIndex(position);
            this.valueCount = valueBlock.getValueCount(position);
        }

        @Override
        public Geometry combined() {
            int valueIndex = firstValue;
            if (valueCount == 1) {
                return decoder.apply(valueBlock.getLong(valueIndex));
            }
            List<Point> points = new ArrayList<>();
            while (valueIndex < firstValue + valueCount) {
                points.add(decoder.apply(valueBlock.getLong(valueIndex++)));
            }
            return new MultiPoint(points);
        }
    }
}
