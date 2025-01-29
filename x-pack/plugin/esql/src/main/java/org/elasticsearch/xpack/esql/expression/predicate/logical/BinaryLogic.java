/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
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
import org.elasticsearch.xpack.esql.evaluator.mapper.BooleanToScoringExpressionEvaluator;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;

public abstract class BinaryLogic extends BinaryOperator<Boolean, Boolean, Boolean, BinaryLogicOperation> implements TranslationAware {

    // Provides a way to score the result of the logical operation in case scoring is used
    private final BinaryScoringLogicOperation scoringFunction;

    protected BinaryLogic(
        Source source,
        Expression left,
        Expression right,
        BinaryLogicOperation operation,
        BinaryScoringLogicOperation scoringFunction
    ) {
        super(source, left, right, operation);
        this.scoringFunction = scoringFunction;
    }

    protected BinaryLogic(StreamInput in, BinaryLogicOperation op, BinaryScoringLogicOperation scoringOp) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            op,
            scoringOp
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
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        return left() instanceof TranslationAware leftAware
            && leftAware.translatable(pushdownPredicates)
            && right() instanceof TranslationAware rightAware
            && rightAware.translatable(pushdownPredicates);
    }

    @Override
    public Query asQuery(TranslatorHandler handler) {
        return boolQuery(source(), handler.asQuery(left()), handler.asQuery(right()), this instanceof And);
    }

    public BinaryScoringLogicOperation scoringFunction() {
        return scoringFunction;
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

    public static class BinaryLogicEvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final boolean usesScoring;
        private final BinaryLogic operation;
        private final EvalOperator.ExpressionEvaluator.Factory leftFactory;
        private final EvalOperator.ExpressionEvaluator.Factory rightFactory;

        public BinaryLogicEvaluatorFactory(
            boolean usesScoring,
            BinaryLogic operation,
            EvalOperator.ExpressionEvaluator.Factory leftFactory,
            EvalOperator.ExpressionEvaluator.Factory rightFactory
        ) {
            this.usesScoring = usesScoring;
            this.operation = operation;
            this.leftFactory = leftFactory;
            this.rightFactory = rightFactory;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            if (usesScoring) {
                return new BooleanScoringLogicExpressionEvaluator.BooleanScoringLogicEvaluatorFactory(
                    operation.scoringFunction(),
                    new BooleanToScoringExpressionEvaluator(leftFactory.get(context), context),
                    new BooleanToScoringExpressionEvaluator(rightFactory.get(context), context)
                ).get(context);
            } else {
                return new BooleanLogicExpressionEvaluator.BooleanLogicEvaluatorFactory(
                    operation.function(),
                    leftFactory.get(context),
                    rightFactory.get(context)
                ).get(context);
            }
        }
    }
}
