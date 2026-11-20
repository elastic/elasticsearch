/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicateSerializationTests.analyzedPredicate;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class NestedPredicateTests extends ESTestCase {
    private static final String QUERY = "FROM ne | WHERE NESTED_ANY(CLAMP(items.qty, 1, 5) > 2) | LIMIT 10";
    private static final String EVALUABLE_QUERY = "FROM ne | WHERE NESTED_ANY(items.qty > 2) | LIMIT 10";

    public void testBodyIsTheOnlyChild() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.children(), equalTo(List.of(predicate.body())));
    }

    public void testExpressionRewritesReachTheBody() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.body().anyMatch(SurrogateExpression.class::isInstance), is(true));

        Expression rewritten = predicate.transformDown(Expression.class, SubstituteSurrogateExpressions::rule);

        assertThat(rewritten, instanceOf(NestedPredicate.class));
        NestedPredicate rewrittenPredicate = (NestedPredicate) rewritten;
        assertThat(rewrittenPredicate.body().anyMatch(SurrogateExpression.class::isInstance), is(false));
        assertThat(rewrittenPredicate.body(), not(equalTo(predicate.body())));
        assertThat(rewrittenPredicate.path(), equalTo(predicate.path()));
        assertThat(rewrittenPredicate.inputs(), equalTo(predicate.inputs()));
        assertThat(rewrittenPredicate.relativePaths(), equalTo(predicate.relativePaths()));
        assertThat(rewrittenPredicate.configuration(), equalTo(predicate.configuration()));
    }

    public void testInputsAreNotReferencesOfTheParentRow() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.body().references(), is(not(empty())));
        assertThat(predicate.references(), is(empty()));
    }

    public void testReplacingTheChildKeepsEveryOtherArgument() {
        NestedPredicate predicate = analyzedPredicate(QUERY).withQueryBuilder(QueryBuilders.matchAllQuery());
        Expression newBody = new Literal(Source.EMPTY, true, DataType.BOOLEAN);

        NestedPredicate replaced = predicate.replaceChildren(List.of(newBody));

        assertThat(replaced.body(), equalTo(newBody));
        assertThat(replaced.path(), equalTo(predicate.path()));
        assertThat(replaced.inputs(), equalTo(predicate.inputs()));
        assertThat(replaced.relativePaths(), equalTo(predicate.relativePaths()));
        assertThat(replaced.queryBuilder(), equalTo(predicate.queryBuilder()));
        assertThat(replaced.configuration(), equalTo(predicate.configuration()));
    }

    public void testAQueryChangesWhatThePredicateEqualsAndHashes() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        NestedPredicate withQuery = predicate.withQueryBuilder(QueryBuilders.matchAllQuery());

        assertThat(withQuery, not(equalTo(predicate)));
        assertThat(withQuery.hashCode(), not(equalTo(predicate.hashCode())));
        assertThat(withQuery, equalTo(predicate.withQueryBuilder(QueryBuilders.matchAllQuery())));
        assertThat(withQuery.hashCode(), equalTo(predicate.withQueryBuilder(QueryBuilders.matchAllQuery()).hashCode()));
    }

    public void testRewritingTheChildrenToTheSameExpressionsChangesNothing() {
        NestedPredicate predicate = analyzedPredicate(QUERY);
        assertThat(predicate.replaceChildren(predicate.children()), equalTo(predicate));
    }

    public void testChildrenAreReadWithThePlannerSettingsOfTheContext() {
        NestedPredicate predicate = analyzedPredicate(EVALUABLE_QUERY);
        Holder<Integer> requested = new Holder<>(0);
        EvaluatorMapper.ToEvaluator context = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }

            @Override
            public IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts() {
                return EmptyIndexedByShardId.instance();
            }

            @Override
            public PlannerSettings plannerSettings() {
                requested.set(requested.get() + 1);
                return PlannerSettings.DEFAULTS;
            }
        };

        assertThat(predicate.toEvaluator(context), is(notNullValue()));
        assertThat(requested.get(), equalTo(1));
    }

    public void testAContextWithoutPlannerSettingsIsAnError() {
        NestedPredicate predicate = analyzedPredicate(EVALUABLE_QUERY);
        EvaluatorMapper.ToEvaluator context = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }

            @Override
            public IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts() {
                return EmptyIndexedByShardId.instance();
            }
        };

        expectThrows(UnsupportedOperationException.class, () -> predicate.toEvaluator(context));
    }

    public void testTheMapperPassesTheSettingsItWasGiven() {
        PlannerSettings settings = PlannerSettings.DEFAULTS;
        EvaluatorMapper.ToEvaluator withSettings = EvalMapper.toEvaluatorContext(
            FoldContext.small(),
            new Layout.Builder().build(),
            EmptyIndexedByShardId.instance(),
            null,
            settings
        );
        assertThat(withSettings.plannerSettings(), sameInstance(settings));

        EvaluatorMapper.ToEvaluator without = EvalMapper.toEvaluatorContext(
            FoldContext.small(),
            new Layout.Builder().build(),
            EmptyIndexedByShardId.instance(),
            null
        );
        expectThrows(UnsupportedOperationException.class, without::plannerSettings);
    }
}
