/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.expression.function.vector.ExactNN;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.VECTOR_SIMILARITY_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

/**
 * Break TopN back into Limit + OrderBy to allow the order rules to kick in.
 */
public class ReplaceKnnWithNoPushedDownFiltersWithEvalTopN extends OptimizerRules.OptimizerRule<Filter> {

    public static final String EXACT_SCORE_ATTR_NAME = "knn_score";

    public ReplaceKnnWithNoPushedDownFiltersWithEvalTopN() {
        super(UP);
    }

    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition();

        Holder<List<Knn>> replaced = new Holder<>(new ArrayList<>());
        Expression conditionWithoutKnns = condition.transformDown(Knn.class, knn -> replaceNonPushableKnnByTrue(knn, replaced));
        if (conditionWithoutKnns.equals(condition)) {
            return filter;
        }

        // Replace knn with scoring expressions of exact queries
        List<Expression> exactQueries = replaced.get()
            .stream()
            .map(ReplaceKnnWithNoPushedDownFiltersWithEvalTopN::replaceKnnByExact)
            .toList();
        int numExactQueries = replaced.get().size();
        assert numExactQueries > 0;
        List<Alias> scoringAliases = new ArrayList<>(numExactQueries);
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        for (int i = 0; i < numExactQueries; i++) {
            String name = rawTemporaryName(EXACT_SCORE_ATTR_NAME, String.valueOf(i));
            Alias alias = new Alias(EMPTY, name, new Score(EMPTY, exactQueries.get(i)));
            scoringAliases.add(alias);
            aliasesBuilder.put(alias.toAttribute(), alias.child());
        }

        Eval scoreEval = new Eval(
            EMPTY,
            filter.with(conditionWithoutKnns),
            scoringAliases
        );

        // Filter for all exact scores > 0
        Expression scoreComparison = null;
        List<Attribute> scoringAttributes = new ArrayList<>(numExactQueries);
        for (int i = 0; i < numExactQueries; i++) {
            Attribute scoringAttr = scoringAliases.get(i).toAttribute();
            scoringAttributes.add(scoringAttr);
            GreaterThan gt = new GreaterThan(
                EMPTY,
                scoringAttr,
                new Literal(EMPTY, 0.0, DataType.DOUBLE)
            );
            if (scoreComparison == null) {
                scoreComparison = gt;
            } else {
                scoreComparison = new And(EMPTY, gt, scoreComparison);
            }
        }
        Filter scoreFilter = new Filter(EMPTY, scoreEval, scoreComparison);

        // Sort on the scores, limit on the minimum k
        List<Order> orders = new ArrayList<>(numExactQueries);
        for (int i = 0; i < numExactQueries; i++) {
            orders.add(
                new Order(
                    EMPTY,
                    scoringAttributes.get(i),
                    Order.OrderDirection.DESC,
                    Order.NullsPosition.LAST
                )
            );
        }
        int minimumK = replaced.get()
            .stream()
            .map(k -> ((KnnVectorQueryBuilder) k.queryBuilder()))
            .mapToInt(KnnVectorQueryBuilder::k)
            .min()
            .orElseThrow();
        TopN topK = new TopN(EMPTY, scoreFilter, orders, new Literal(EMPTY, minimumK, DataType.INTEGER));
        return topK;
//        // Aliases resolution function
//        AttributeMap<Expression> evalAliases = aliasesBuilder.build();
//        topK = (TopN)  topK.transformExpressionsOnly(ReferenceAttribute.class, r -> evalAliases.resolve(r, r));
//        return topK;
    }

    private static Expression replaceNonPushableKnnByTrue(Knn knn, Holder<List<Knn>> replaced) {
        if (knn.hasNonPushableFilters() == false) {
            return knn;
        }

        replaced.get().add(knn);

        return Literal.TRUE;
    }

    private static Expression replaceKnnByExact(Knn knn) {
        Expression minimumSimilarity = knn.options() == null
            ? null
            : ((MapExpression) knn.options()).get(VECTOR_SIMILARITY_FIELD.getPreferredName());
        ExactNN exact = new ExactNN(
            knn.source(),
            knn.field(),
            knn.query(),
            minimumSimilarity
        );
        // Replaces query builder as it was not resolved during post analysis phase
        return exact.replaceQueryBuilder(
            TranslatorHandler.TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, exact).toQueryBuilder()
        );
    }
}
