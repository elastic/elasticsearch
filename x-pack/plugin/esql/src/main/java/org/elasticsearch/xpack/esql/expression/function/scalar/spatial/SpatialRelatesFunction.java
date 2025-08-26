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
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.SpatialRelatesQuery;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asGeometryDocValueReader;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils.asLuceneComponent2D;

public abstract class SpatialRelatesFunction extends BinarySpatialFunction
    implements
        EvaluatorMapper,
        SpatialEvaluatorFactory.SpatialSourceSupplier,
        TranslationAware,
        SurrogateExpression {

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
     * Produce a map of rules defining combinations of incoming types to the evaluator factory that should be used.
     */
    abstract Map<SpatialEvaluatorFactory.SpatialEvaluatorKey, SpatialEvaluatorFactory<?, ?>> evaluatorRules();

    /**
     * Some spatial functions can replace themselves with alternatives that are more efficient for certain cases.
     */
    @Override
    public SpatialRelatesFunction surrogate() {
        return this;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return SpatialEvaluatorFactory.makeSpatialEvaluator(this, evaluatorRules(), toEvaluator);
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

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return super.translatable(pushdownPredicates); // only for the explicit Override, as only this subclass implements TranslationAware
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        if (left().foldable()) {
            checkSpatialRelatesFunction(left(), queryRelation());
            return translate(handler, right(), left());
        } else {
            checkSpatialRelatesFunction(right(), queryRelation());
            return translate(handler, left(), right());
        }

    }

    private static void checkSpatialRelatesFunction(Expression constantExpression, ShapeRelation queryRelation) {
        Check.isTrue(
            constantExpression.foldable(),
            "Line {}:{}: Comparisons against fields are not (currently) supported; offender [{}] in [ST_{}]",
            constantExpression.sourceLocation().getLineNumber(),
            constantExpression.sourceLocation().getColumnNumber(),
            Expressions.name(constantExpression),
            queryRelation
        );
    }

    private Query translate(TranslatorHandler handler, Expression spatialExpression, Expression constantExpression) {
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(spatialExpression);
        String name = handler.nameOf(attribute);

        try {
            Geometry shape = SpatialRelatesUtils.makeGeometryFromLiteral(FoldContext.small() /* TODO remove me */, constantExpression);
            return new SpatialRelatesQuery(source(), name, queryRelation(), shape, attribute.dataType());
        } catch (IllegalArgumentException e) {
            throw new QlIllegalArgumentException(e.getMessage(), e);
        }
    }
}
