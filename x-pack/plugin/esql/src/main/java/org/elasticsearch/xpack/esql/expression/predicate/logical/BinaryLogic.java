/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ScoreOperator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.querydsl.query.BoolQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.score.ExpressionScoreMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;

public abstract class BinaryLogic extends BinaryOperator<Boolean, Boolean, Boolean, BinaryLogicOperation>
    implements
        TranslationAware,
        ExpressionScoreMapper {

    protected BinaryLogic(Source source, Expression left, Expression right, BinaryLogicOperation operation) {
        super(source, left, right, operation);
    }

    protected BinaryLogic(StreamInput in, BinaryLogicOperation op) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            op
        );
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, ParamOrdinal paramOrdinal) {
        return isBoolean(e, sourceText(), paramOrdinal);
    }

    @Override
    public Nullability nullable() {
        // Cannot fold null due to 3vl, constant folding will do any possible folding.
        return Nullability.UNKNOWN;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return TranslationAware.translatable(left(), pushdownPredicates).merge(TranslationAware.translatable(right(), pushdownPredicates));
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return boolQuery(
            source(),
            handler.asQuery(pushdownPredicates, left()),
            handler.asQuery(pushdownPredicates, right()),
            this instanceof And
        );
    }

    public static Query boolQuery(Source source, Query left, Query right, boolean isAnd) {
        Check.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<Query> queries;
        // check if either side is already a bool query to an extra bool query
        if (left instanceof BoolQuery leftBool && leftBool.isAnd() == isAnd) {
            if (right instanceof BoolQuery rightBool && rightBool.isAnd() == isAnd) {
                queries = CollectionUtils.combine(leftBool.queries(), rightBool.queries());
            } else {
                queries = CollectionUtils.combine(leftBool.queries(), right);
            }
        } else if (right instanceof BoolQuery bool && bool.isAnd() == isAnd) {
            queries = CollectionUtils.combine(bool.queries(), left);
        } else {
            queries = Arrays.asList(left, right);
        }
        return new BoolQuery(source, isAnd, queries);
    }

    @Override
    public ScoreOperator.ExpressionScorer.Factory toScorer(ToScorer toScorer) {
        return context -> new BinaryLogicScorer(context, toScorer.toScorer(left()).get(context), toScorer.toScorer(right()).get(context));
    }

    /**
     * Binary logic adds together scores coming from the left and right expressions, both for conjunctions and disjunctions
     */
    private record BinaryLogicScorer(DriverContext driverContext, ScoreOperator.ExpressionScorer left, ScoreOperator.ExpressionScorer right)
        implements
            ScoreOperator.ExpressionScorer {
        @Override
        public DoubleBlock score(Page page) {
            DoubleVector.Builder builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(page.getPositionCount());
            try (DoubleVector leftVector = left.score(page).asVector(); DoubleVector rightVector = right.score(page).asVector()) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    builder.appendDouble(leftVector.getDouble(i) + rightVector.getDouble(i));
                }
            }
            return builder.build().asBlock();
        }

        @Override
        public void close() {
            left.close();
            right.close();
        }
    }
}
