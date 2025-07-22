/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.expression.function.vector.ExactNN;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
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
import static org.elasticsearch.xpack.esql.planner.PlannerUtils.usesScoring;

/**
 * Break TopN back into Limit + OrderBy to allow the order rules to kick in.
 */
public class ReplaceKnnWithNoPushedDownFilters extends OptimizerRules.OptimizerRule<Filter> {

    public static final String EXACT_SCORE_ATTR_NAME = "knn_score";

    public ReplaceKnnWithNoPushedDownFilters() {
        super(UP);
    }

    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition();

        Holder<List<Knn>> knnQueries = new Holder<>(new ArrayList<>());
        Expression conditionWithoutKnns = condition.transformDown(Knn.class, knn -> replaceNonPushableKnnByTrue(knn, knnQueries));
        if (conditionWithoutKnns.equals(condition)) {
            return filter;
        }

        List<Attribute> scoreAttrs;
        LogicalPlan scoringPlan;
        if (usesScoring(filter)) {
            scoreAttrs = filter.output()
                .stream()
                .filter(attr -> attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE))
                .toList();
            // Use the original filter, changing knn to exact queries
            scoringPlan = filter.with(
                filter.condition().transformDown(Knn.class, ReplaceKnnWithNoPushedDownFilters::replaceKnnByExactQuery)
            );
        } else {
            // Replace knn with scoring expressions of exact queries
            List<Expression> exactQueries = knnQueries.get()
                .stream()
                .map(ReplaceKnnWithNoPushedDownFilters::replaceKnnByExactQuery)
                .toList();
            assert exactQueries.isEmpty() == false;

            // Create an Eval for scoring the exact queries
            List<Alias> exactScoreAliases = exactQueryScoreAliases(exactQueries);
            scoringPlan = new Eval(EMPTY, filter.with(conditionWithoutKnns), exactScoreAliases);
            scoreAttrs = exactScoreAliases.stream().map(Alias::toAttribute).toList();
        }

        // Sort on the scores, limit on the minimum k from the queries
        TopN topN = createTopN(scoreAttrs, knnQueries.get(), scoringPlan);

        // Filter on scores > 0. We could filter earlier, but could be combined with the existing filter and _score would not be updated
        return createScoreFilter(scoreAttrs, topN);
    }

    private static Expression replaceKnnByExactQuery(Knn knn) {
        Expression minimumSimilarity = knn.options() == null
            ? null
            : ((MapExpression) knn.options()).get(VECTOR_SIMILARITY_FIELD.getPreferredName());
        ExactNN exact = new ExactNN(knn.source(), knn.field(), knn.query(), minimumSimilarity);
        // Replaces query builder as it was not resolved during post analysis phase
        return exact.replaceQueryBuilder(
            TranslatorHandler.TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, exact).toQueryBuilder()
        );
    }

    private static List<Alias> exactQueryScoreAliases(List<Expression> exactQueries) {
        List<Alias> scoringAliases = new ArrayList<>();
        for (int i = 0; i < exactQueries.size(); i++) {
            String name = rawTemporaryName(EXACT_SCORE_ATTR_NAME, String.valueOf(i));
            Alias alias = new Alias(EMPTY, name, new Score(EMPTY, exactQueries.get(i)));
            scoringAliases.add(alias);
        }
        return scoringAliases;
    }

    private static Filter createScoreFilter(List<Attribute> scoreAttrs, LogicalPlan planToFilter) {
        Expression scoreComparison = null;
        for (Attribute scoringAttr : scoreAttrs) {
            GreaterThan gt = new GreaterThan(EMPTY, scoringAttr, new Literal(EMPTY, 0.0, DataType.DOUBLE));
            if (scoreComparison == null) {
                scoreComparison = gt;
            } else {
                scoreComparison = new And(EMPTY, gt, scoreComparison);
            }
        }

        return new Filter(EMPTY, planToFilter, scoreComparison);
    }

    private static Expression replaceNonPushableKnnByTrue(Knn knn, Holder<List<Knn>> replaced) {
        if (knn.nonPushableFilters().isEmpty()) {
            return knn;
        }
        replaced.get().add(knn);
        return Literal.TRUE;
    }

    private static TopN createTopN(List<Attribute> scoreAttrs, List<Knn> knnQueries, LogicalPlan scoringPlan) {
        List<Order> orders = scoreAttrs.stream()
            .map(a -> new Order(EMPTY, a, Order.OrderDirection.DESC, Order.NullsPosition.LAST))
            .toList();
        int minimumK = knnQueries.stream().mapToInt(knn -> (Integer) knn.k().fold(FoldContext.small())).min().orElseThrow();
        return new TopN(EMPTY, scoringPlan, orders, new Literal(EMPTY, minimumK, DataType.INTEGER));
    }
}
