/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.IndexMode;
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
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
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

    public void testAnExactConjunctBesideAnUntranslatableOneIsPushedAsAParentPrefilter() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");

        PushNestedPredicateToSource.Pushed pushed = PushNestedPredicateToSource.push(
            predicate,
            false,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(pushed.exact(), is(false));
        assertIsTheNestedQueryForSkuA(pushed.query());
    }

    public void testTwoExactConjunctsBesideAnUntranslatableOneAreOneParentPrefilter() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty > 10 AND items.qty + 1 > 2");

        PushNestedPredicateToSource.Pushed pushed = PushNestedPredicateToSource.push(
            predicate,
            false,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(pushed.exact(), is(false));
        assertThat(pushed.query(), instanceOf(NestedQueryBuilder.class));
        NestedQueryBuilder nested = (NestedQueryBuilder) pushed.query();
        assertThat(nested.path(), equalTo("items"));
        assertThat(nested.query(), instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) nested.query()).must(), hasSize(2));
    }

    public void testAMixedPredicateIsNotPushedWhenNegated() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");

        assertThat(PushNestedPredicateToSource.push(predicate, true, CHILDREN, stats(queryableAsRead(predicate))), is(nullValue()));
    }

    public void testAPredicateWithNoExactConjunctIsNotPushed() {
        NestedPredicate predicate = predicate("items.qty + 1 > 2 AND items.qty + 2 > 3");

        assertThat(PushNestedPredicateToSource.push(predicate, false, CHILDREN, stats(queryableAsRead(predicate))), is(nullValue()));
    }

    public void testAnExactDisjunctiveConjunctBesideAnUntranslatableOneIsOneParentPrefilter() {
        NestedPredicate predicate = predicate("(items.sku == \"a\" OR items.qty > 10) AND items.qty + 1 > 2");

        PushNestedPredicateToSource.Pushed pushed = PushNestedPredicateToSource.push(
            predicate,
            false,
            CHILDREN,
            stats(queryableAsRead(predicate))
        );

        assertThat(pushed.exact(), is(false));
        assertThat(pushed.query(), instanceOf(NestedQueryBuilder.class));
        assertThat(((NestedQueryBuilder) pushed.query()).query(), instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) ((NestedQueryBuilder) pushed.query()).query()).should(), hasSize(2));
    }

    public void testADisjunctionWithAnUntranslatableBranchIsNotPushed() {
        NestedPredicate predicate = predicate("items.sku == \"a\" OR items.qty + 1 > 2");

        assertThat(PushNestedPredicateToSource.push(predicate, false, CHILDREN, stats(queryableAsRead(predicate))), is(nullValue()));
    }

    public void testAMixedFilterKeepsThePredicateAndPrefiltersTheScan() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");

        PhysicalPlan planned = new PushNestedPredicateToSource().apply(
            new FilterExec(Source.EMPTY, scan(), predicate),
            context(queryableAsRead(predicate))
        );

        assertThat(planned, instanceOf(FilterExec.class));
        FilterExec filter = (FilterExec) planned;
        assertThat(filter.condition(), equalTo(predicate));
        assertThat(filter.child(), instanceOf(EsQueryExec.class));
        List<QueryBuilder> nested = nestedQueries(((EsQueryExec) filter.child()).query());
        assertThat(nested, hasSize(1));
        assertIsTheNestedQueryForSkuA(nested.getFirst());
    }

    public void testANegatedMixedFilterIsLeftAlone() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");
        FilterExec filter = new FilterExec(Source.EMPTY, scan(), new Not(Source.EMPTY, predicate));

        assertThat(new PushNestedPredicateToSource().apply(filter, context(queryableAsRead(predicate))), equalTo(filter));
    }

    public void testAMixedPredicateIsNotPushedWhenTheShardsRefuseExpensiveQueries() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");

        assertThat(PushNestedPredicateToSource.push(predicate, false, CHILDREN, stats(queryableAsRead(predicate), false)), is(nullValue()));
    }

    public void testAMixedPredicateWithAnUnqueryableExactConjunctIsNotPushed() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");
        Map<String, DataType> withoutSku = new HashMap<>(queryableAsRead(predicate));
        withoutSku.remove("items.sku");

        assertThat(PushNestedPredicateToSource.push(predicate, false, CHILDREN, stats(withoutSku)), is(nullValue()));
    }

    public void testNothingTranslatesWhenTheShardsRefuseExpensiveQueries() {
        NestedPredicate onAChild = predicate(EQUALS_A);
        NestedPredicate onTheCollection = analyzedPredicate("FROM ne | WHERE NESTED_ANY(items, true) | LIMIT 10");
        SearchStats refusing = stats(queryableAsRead(onAChild), false);

        assertThat(PushNestedPredicateToSource.translate(onAChild, CHILDREN, refusing), is(nullValue()));
        assertThat(PushNestedPredicateToSource.translate(onTheCollection, CHILDREN, refusing), is(nullValue()));
        assertThat(PushNestedPredicateToSource.push(onAChild, false, CHILDREN, refusing), is(nullValue()));
    }

    public void testEvalKeepsReadingChildrenWhenTheShardsRefuseExpensiveQueries() {
        NestedPredicate predicate = predicate(EQUALS_A);
        EvalExec eval = eval(predicate);
        LocalPhysicalOptimizerContext refusing = context(stats(queryableAsRead(predicate), false));

        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, refusing), equalTo(eval));
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

    public void testAnExactConjunctFiltersChildrenAndTheRestStays() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");

        NestedPredicate planned = only(new EvaluateNestedPredicateInLucene().apply(eval(predicate), context(queryableAsRead(predicate))));

        assertThat(planned.queryBuilder(), is(nullValue()));
        assertThat(planned.childFilter(), instanceOf(SingleValueQuery.Builder.class));
        SingleValueQuery.Builder child = (SingleValueQuery.Builder) planned.childFilter();
        assertThat(child.field(), equalTo("items.sku"));
        assertThat(child.next(), equalTo(QueryBuilders.termQuery("items.sku", "a").boost(Query.NO_SCORE_BOOST)));
        assertThat(planned.relativePaths(), equalTo(List.of("qty")));
    }

    public void testTwoExactConjunctsAreOneChildFilter() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty > 10 AND items.qty + 1 > 2");

        NestedPredicate planned = only(new EvaluateNestedPredicateInLucene().apply(eval(predicate), context(queryableAsRead(predicate))));

        assertThat(planned.queryBuilder(), is(nullValue()));
        assertThat(planned.childFilter(), instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) planned.childFilter()).must(), hasSize(2));
        assertThat(planned.relativePaths(), equalTo(List.of("qty")));
    }

    public void testAFullyExactConjunctionStillMatchesTheParentInLucene() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty > 10");

        NestedPredicate planned = only(new EvaluateNestedPredicateInLucene().apply(eval(predicate), context(queryableAsRead(predicate))));

        assertThat(planned.queryBuilder(), instanceOf(NestedQueryBuilder.class));
        assertThat(planned.childFilter(), is(nullValue()));
    }

    public void testANotConjunctIsNotExtracted() {
        NestedPredicate predicate = predicate("NOT items.sku == \"a\" AND items.qty + 1 > 2");
        EvalExec eval = eval(predicate);

        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(queryableAsRead(predicate))), equalTo(eval));
    }

    public void testADisjunctionIsNotExtracted() {
        NestedPredicate predicate = predicate("items.sku == \"a\" OR items.qty + 1 > 2");
        EvalExec eval = eval(predicate);

        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(queryableAsRead(predicate))), equalTo(eval));
    }

    public void testAnUnqueryableExactConjunctStaysInThePredicate() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");
        Map<String, DataType> withoutSku = new HashMap<>(queryableAsRead(predicate));
        withoutSku.remove("items.sku");
        EvalExec eval = eval(predicate);

        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(withoutSku)), equalTo(eval));
    }

    public void testExpensiveQueriesLeaveTheConjunctionInPlace() {
        NestedPredicate predicate = predicate("items.sku == \"a\" AND items.qty + 1 > 2");
        EvalExec eval = eval(predicate);

        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(stats(queryableAsRead(predicate), false))), equalTo(eval));
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

    private static EsQueryExec scan() {
        return new EsQueryExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), null, null, null, List.of());
    }

    private static List<QueryBuilder> nestedQueries(QueryBuilder query) {
        List<QueryBuilder> found = new ArrayList<>();
        if (query instanceof NestedQueryBuilder) {
            found.add(query);
        } else if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                found.addAll(nestedQueries(clause));
            }
            for (QueryBuilder clause : bool.must()) {
                found.addAll(nestedQueries(clause));
            }
        }
        return found;
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
        return context(stats(queryable));
    }

    private static LocalPhysicalOptimizerContext context(SearchStats stats) {
        return new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            stats
        );
    }

    /**
     * {@link SearchStats} that a real shard answers from its mappings, which this test does not need: only the answer to whether a
     * child field is queryable as a given type.
     */
    private static SearchStats stats(Map<String, DataType> queryable) {
        return stats(queryable, true);
    }

    private static SearchStats stats(Map<String, DataType> queryable, boolean nestedQueriesAllowed) {
        return new SearchStats.UnsupportedSearchStats() {
            @Override
            public boolean nestedQueriesAllowed() {
                return nestedQueriesAllowed;
            }

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
