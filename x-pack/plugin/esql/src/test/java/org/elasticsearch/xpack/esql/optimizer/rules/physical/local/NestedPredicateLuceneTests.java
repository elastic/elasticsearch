/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicateSerializationTests.analyzedPredicate;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class NestedPredicateLuceneTests extends ESTestCase {
    private static final LucenePushdownPredicates CHILDREN = PushNestedPredicateToSource.childFields(new EsqlFlags(true));
    private static final String EQUALS_A = "items.sku == \"a\"";

    public void testAQueryableChildTranslatesToAnExactNestedQuery() {
        NestedPredicate predicate = predicate(EQUALS_A);

        PushNestedPredicateToSource.Translation translation = PushNestedPredicateToSource.translate(
            predicate,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(translation.exact(), is(true));
        assertIsTheNestedQueryForSkuA(translation.query());
    }

    public void testAChildTheShardsCannotQueryDoesNotTranslate() {
        assertThat(PushNestedPredicateToSource.translate(predicate(EQUALS_A), CHILDREN, stats(Map.of())), is(nullValue()));
    }

    public void testAChildQueryableOnlyAsAnotherTypeDoesNotTranslate() {
        NestedPredicate predicate = predicate(EQUALS_A);
        Map<String, DataType> asAnotherType = Map.of("items.sku", DataType.INTEGER);

        assertThat(PushNestedPredicateToSource.translate(predicate, CHILDREN, stats(asAnotherType)), is(nullValue()));
    }

    public void testAnUntranslatablePredicateDoesNotTranslate() {
        NestedPredicate predicate = predicate("items.qty + 1 > 2");
        assertThat(PushNestedPredicateToSource.translate(predicate, CHILDREN, stats(queryableAsRead(predicate))), is(nullValue()));
    }

    public void testANegatedExactTranslationIsPushedAsMustNot() {
        NestedPredicate predicate = predicate(EQUALS_A);

        PushNestedPredicateToSource.Pushed pushed = PushNestedPredicateToSource.push(
            predicate,
            true,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(pushed.exact(), is(true));
        assertThat(pushed.query(), instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder negated = (BoolQueryBuilder) pushed.query();
        assertThat(negated.mustNot(), hasSize(1));
        assertIsTheNestedQueryForSkuA(negated.mustNot().getFirst());
        assertThat(negated.must(), is(empty()));
        assertThat(negated.filter(), is(empty()));
    }

    public void testAnUnnegatedExactTranslationIsPushedAsTheNestedQuery() {
        NestedPredicate predicate = predicate(EQUALS_A);

        PushNestedPredicateToSource.Pushed pushed = PushNestedPredicateToSource.push(
            predicate,
            false,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(pushed.exact(), is(true));
        assertIsTheNestedQueryForSkuA(pushed.query());
    }

    public void testAPredicateTheShardsCannotQueryIsNotPushed() {
        assertThat(PushNestedPredicateToSource.push(predicate(EQUALS_A), false, CHILDREN, stats(Map.of())), is(nullValue()));
    }

    public void testEvalOfAnExactPredicateIsEvaluatedByLucene() {
        NestedPredicate predicate = predicate(EQUALS_A);
        assertEvaluatedByTheNestedQuery(eval(predicate), predicate);
    }

    public void testFilterOfAnExactPredicateIsEvaluatedByLucene() {
        NestedPredicate predicate = predicate(EQUALS_A);
        assertEvaluatedByTheNestedQuery(new FilterExec(Source.EMPTY, source(), predicate), predicate);
    }

    public void testAPredicateTheShardsCannotQueryIsLeftToTheChildren() {
        EvalExec eval = eval(predicate(EQUALS_A));
        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(Map.of())), equalTo(eval));
    }

    public void testAPredicateThatAlreadyHasAQueryKeepsIt() {
        NestedPredicate predicate = predicate(EQUALS_A);
        QueryBuilder existing = QueryBuilders.matchAllQuery();
        EvalExec eval = eval(predicate.withQueryBuilder(existing));

        PhysicalPlan planned = new EvaluateNestedPredicateInLucene().apply(eval, context(queryableAsRead(predicate)));

        assertThat(only(planned).queryBuilder(), sameInstance(existing));
    }

    private static void assertEvaluatedByTheNestedQuery(PhysicalPlan plan, NestedPredicate predicate) {
        PhysicalPlan planned = new EvaluateNestedPredicateInLucene().apply(plan, context(queryableAsRead(predicate)));

        NestedPredicate lucene = only(planned);
        assertIsTheNestedQueryForSkuA(lucene.queryBuilder());
        assertThat(lucene, not(equalTo(predicate)));
    }

    private static void assertIsTheNestedQueryForSkuA(QueryBuilder query) {
        assertThat(query, instanceOf(NestedQueryBuilder.class));
        NestedQueryBuilder nested = (NestedQueryBuilder) query;
        assertThat(nested.path(), equalTo("items"));
        assertThat(nested.scoreMode(), equalTo(ScoreMode.None));
        assertThat(nested.ignoreUnmapped(), is(true));
        assertThat(nested.boost(), equalTo(Query.NO_SCORE_BOOST));
        assertThat(nested.query(), instanceOf(SingleValueQuery.Builder.class));
        SingleValueQuery.Builder child = (SingleValueQuery.Builder) nested.query();
        assertThat(child.field(), equalTo("items.sku"));
        assertThat(child.next(), equalTo(QueryBuilders.termQuery("items.sku", "a").boost(Query.NO_SCORE_BOOST)));
    }

    private static NestedPredicate only(PhysicalPlan plan) {
        List<NestedPredicate> found = new ArrayList<>();
        plan.forEachExpression(NestedPredicate.class, found::add);
        assertThat(found, hasSize(1));
        return found.getFirst();
    }

    private static EvalExec eval(NestedPredicate predicate) {
        return new EvalExec(Source.EMPTY, source(), List.of(new Alias(Source.EMPTY, "h", predicate)));
    }

    private static LocalSourceExec source() {
        return new LocalSourceExec(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
    }

    private static LocalPhysicalOptimizerContext context(Map<String, DataType> queryable) {
        return new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            stats(queryable)
        );
    }

    /**
     * {@link SearchStats} that a real shard answers from its mappings, which this test does not need: only the answer to whether a
     * child field is queryable as a given type.
     */
    private static SearchStats stats(Map<String, DataType> queryable) {
        return new SearchStats.UnsupportedSearchStats() {
            @Override
            public boolean nestedFieldIsQueryable(FieldAttribute.FieldName field, DataType type) {
                return queryable.get(field.string()) == type;
            }
        };
    }

    private static Map<String, DataType> queryableAsRead(NestedPredicate predicate) {
        Map<String, DataType> queryable = new HashMap<>();
        for (int i = 0; i < predicate.inputs().size(); i++) {
            queryable.put(predicate.fieldNames().get(i), predicate.inputs().get(i).dataType());
        }
        return queryable;
    }

    private static NestedPredicate predicate(String body) {
        return analyzedPredicate("FROM ne | WHERE NESTED_ANY(" + body + ") | LIMIT 10");
    }
}
