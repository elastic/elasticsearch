/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * A runtime search that adds to {@code _score} only does so after the filter holding it has run (see
 * {@link FullTextFunction#containsRuntimeScorer}), so a {@code _score} predicate combined with it using {@code AND}
 * would see the score from before the search. This splits such predicates out into a filter above the search, where
 * they see its score; {@link PushDownAndCombineFilters} then keeps them there.
 * <p>
 * This has to run before anything substitutes {@code _score} for an alias or {@code RENAME} of it: a copy taken before
 * the search holds the score from before it, and once substituted it can't be told apart from a {@code _score} written
 * alongside the search. Whether a search is a runtime one isn't settled yet either, as a search on an alias of an
 * indexed field looks like one until push-down resolves the alias. Splitting such a filter does no harm: the search
 * scores at the source, so {@code _score} is final on both sides, and the two filters combine again once it is pushed
 * down.
 */
public final class SplitScorePredicatesFromRuntimeSearch extends OptimizerRules.OptimizerRule<Filter> {

    @Override
    protected LogicalPlan rule(Filter filter) {
        if (FullTextFunction.containsRuntimeScorer(filter.condition()) == false) {
            return filter;
        }
        List<Expression> scorePredicates = new ArrayList<>();
        List<Expression> rest = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            // A conjunct that both reads _score and holds a runtime search can't be split; it stays with the search.
            if (conjunct.anyMatch(MetadataAttribute::isScoreAttribute) && FullTextFunction.containsRuntimeScorer(conjunct) == false) {
                scorePredicates.add(conjunct);
            } else {
                rest.add(conjunct);
            }
        }
        if (scorePredicates.isEmpty()) {
            return filter;
        }
        return filter.with(filter.with(filter.child(), Predicates.combineAnd(rest)), Predicates.combineAnd(scorePredicates));
    }
}
