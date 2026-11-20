/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.Build;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NestedPredicateLuceneTests extends ESTestCase {
    private static final LucenePushdownPredicates CHILDREN = PushNestedPredicateToSource.childFields(new EsqlFlags(true));

    public void testAQueryableChildTranslatesToANestedQueryThatIsExact() {
        PushNestedPredicateToSource.Translation translation = PushNestedPredicateToSource.translate(
            predicate("items.sku == \"a\""),
            CHILDREN,
            stats(Set.of("items.sku"))
        );
        assertThat(translation, is(notNullValue()));
        assertThat(translation.exact(), is(true));
        assertThat(translation.query(), instanceOf(NestedQueryBuilder.class));
        NestedQueryBuilder nested = (NestedQueryBuilder) translation.query();
        assertThat(nested.path(), equalTo("items"));
        assertThat(nested.scoreMode(), equalTo(ScoreMode.None));
        assertThat(nested.ignoreUnmapped(), is(true));
    }

    public void testAChildTheShardsCannotQueryDoesNotTranslate() {
        assertThat(PushNestedPredicateToSource.translate(predicate("items.sku == \"a\""), CHILDREN, stats(Set.of())), is(nullValue()));
    }

    public void testAPredicateWithAnOuterValueDoesNotTranslate() {
        NestedPredicate predicate = predicate("items.sku == CONCAT(id, \"x\")");
        assertThat(PushNestedPredicateToSource.translate(predicate, CHILDREN, stats(Set.of("items.sku"))), is(nullValue()));
    }

    public void testAnUntranslatablePredicateDoesNotTranslate() {
        NestedPredicate predicate = predicate("items.qty + 1 > 2");
        assertThat(PushNestedPredicateToSource.translate(predicate, CHILDREN, stats(Set.of("items.qty"))), is(nullValue()));
    }

    public void testEvalOfAnExactPredicateIsEvaluatedByLucene() {
        NestedPredicate predicate = predicate("items.sku == \"a\"");
        EvalExec eval = eval(predicate);

        PhysicalPlan planned = new EvaluateNestedPredicateInLucene().apply(eval, context(Set.of("items.sku")));

        NestedPredicate lucene = only(planned);
        assertThat(lucene.queryBuilder(), is(notNullValue()));
        assertThat(lucene.queryBuilder(), instanceOf(NestedQueryBuilder.class));
    }

    public void testFilterOfAnExactPredicateIsEvaluatedByLucene() {
        NestedPredicate predicate = predicate("items.sku == \"a\"");
        FilterExec filter = new FilterExec(Source.EMPTY, source(), predicate);

        PhysicalPlan planned = new EvaluateNestedPredicateInLucene().apply(filter, context(Set.of("items.sku")));

        assertThat(only(planned).queryBuilder(), is(notNullValue()));
    }

    public void testAPredicateTheShardsCannotQueryIsLeftToTheChildren() {
        EvalExec eval = eval(predicate("items.sku == \"a\""));
        assertThat(new EvaluateNestedPredicateInLucene().apply(eval, context(Set.of())), equalTo(eval));
    }

    public void testPlanningTwiceChangesNothing() {
        EvaluateNestedPredicateInLucene rule = new EvaluateNestedPredicateInLucene();
        LocalPhysicalOptimizerContext context = context(Set.of("items.sku"));
        PhysicalPlan once = rule.apply(eval(predicate("items.sku == \"a\"")), context);
        assertThat(rule.apply(once, context), equalTo(once));
    }

    private static NestedPredicate only(PhysicalPlan plan) {
        List<NestedPredicate> found = new ArrayList<>();
        plan.forEachExpression(NestedPredicate.class, found::add);
        assertThat(found.size(), equalTo(1));
        return found.getFirst();
    }

    private static EvalExec eval(NestedPredicate predicate) {
        return new EvalExec(Source.EMPTY, source(), List.of(new Alias(Source.EMPTY, "h", predicate)));
    }

    private static LocalSourceExec source() {
        return new LocalSourceExec(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
    }

    private static LocalPhysicalOptimizerContext context(Set<String> queryable) {
        return new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            stats(queryable)
        );
    }

    private static SearchStats stats(Set<String> queryable) {
        return new SearchStats.UnsupportedSearchStats() {
            @Override
            public boolean nestedFieldIsQueryable(FieldAttribute.FieldName field, DataType type) {
                return queryable.contains(field.string());
            }
        };
    }

    private static NestedPredicate predicate(String body) {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
        List<NestedPredicate> found = new ArrayList<>();
        nestedAnalyzer(NestedPredicate.VERSION).query("FROM ne | WHERE NESTED_ANY(" + body + ") | LIMIT 10")
            .forEachExpressionDown(NestedPredicate.class, found::add);
        return found.getFirst();
    }
}
