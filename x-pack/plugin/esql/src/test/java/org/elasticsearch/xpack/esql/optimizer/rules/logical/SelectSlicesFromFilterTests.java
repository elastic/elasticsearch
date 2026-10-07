/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The slices a source reads are recorded on it from the filter that sits on it once the plan is optimized. These tests check
 * which queries select slices, that the filter always stays in the plan, and what a knn function receives as filters.
 */
public class SelectSlicesFromFilterTests extends AbstractLogicalPlanOptimizerTests {

    private static final SliceSelection NONE = SliceSelection.UNSPECIFIED;

    public SelectSlicesFromFilterTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void checkCapability() {
        assumeTrue("requires slice selection", EsqlCapabilities.Cap.SLICE_SELECTION_FROM_FILTER.isEnabled());
    }

    public void testEqualitySelectsSlice() {
        assertThat(slices("from test metadata _slice | where _slice == \"acme\""), contains(named("acme")));
        // the slices are part of the plan, and show in its description
        assertThat(plan("from test metadata _slice | where _slice == \"acme\"").toString(), containsString("[slice=acme]"));
        assertThat(slices("from test metadata _slice | where \"acme\" == _slice"), contains(named("acme")));
    }

    public void testInSelectsSlices() {
        assertThat(slices("from test metadata _slice | where _slice in (\"acme\", \"globex\")"), contains(named("acme", "globex")));
        assertThat(
            slices("from test metadata _slice | where _slice in (\"acme\", \"globex\", \"acme\")"),
            contains(named("acme", "globex"))
        );
    }

    public void testDisjunctionOfSlicesSelectsSlices() {
        assertThat(
            slices("from test metadata _slice | where _slice == \"acme\" or _slice == \"globex\""),
            contains(named("acme", "globex"))
        );
        assertThat(
            slices("from test metadata _slice | where _slice == \"acme\" or _slice in (\"globex\", \"initech\")"),
            contains(named("acme", "globex", "initech"))
        );
    }

    public void testConjunctionWithOtherConditions() {
        assertThat(slices("from test metadata _slice | where emp_no > 10 and _slice == \"acme\" and salary < 5"), contains(named("acme")));
        assertThat(slices("from test metadata _slice | where emp_no > 10 | where _slice == \"acme\""), contains(named("acme")));
        assertThat(slices("from test metadata _slice | where _slice == \"acme\" and (emp_no > 10 or salary < 5)"), contains(named("acme")));
    }

    /**
     * Several conditions on the slice restrict the source to the slices they have in common.
     */
    public void testConditionsOnSliceAreIntersected() {
        assertThat(
            slices("from test metadata _slice | where _slice in (\"a\", \"b\") | where _slice in (\"b\", \"c\") and emp_no > 1"),
            contains(named("b"))
        );
    }

    /**
     * A filter selects slices wherever it is written, as long as it applies to the rows of the source.
     */
    public void testFilterReachingTheSourceSelectsSlice() {
        for (String commands : List.of(
            "| eval x = emp_no + 1 | where _slice == \"acme\"",
            "| rename _slice as tenant | where tenant == \"acme\"",
            "| eval tenant = _slice | where tenant == \"acme\" | keep emp_no",
            "| sort emp_no | where _slice == \"acme\"",
            "| where _slice == \"acme\" | sort emp_no | limit 10",
            "| where _slice == \"acme\" | stats c = count(*) by gender",
            "| keep emp_no, _slice | where emp_no > 1 | where _slice == \"acme\""
        )) {
            assertThat(commands, slices("from test metadata _slice " + commands), contains(named("acme")));
        }
    }

    /**
     * A filter that applies to the rows another command produced does not say which slices the source reads.
     */
    public void testFilterAfterPipelineBreakerSelectsNothing() {
        for (String commands : List.of(
            "| limit 10 | where _slice == \"acme\"",
            "| sort emp_no | limit 10 | where _slice == \"acme\"",
            "| stats c = count(*) by _slice | where _slice == \"acme\"",
            "| stats c = count(*) by _slice | where _slice == \"acme\" | where c > 1"
        )) {
            assertThat(commands, slices("from test metadata _slice " + commands), contains(NONE));
        }
    }

    public void testOtherConditionsOnSliceSelectNothing() {
        for (String condition : List.of(
            "_slice != \"acme\"",
            "_slice like \"acme*\"",
            "_slice rlike \"acme.*\"",
            "starts_with(_slice, \"acme\")",
            "_slice not in (\"acme\", \"globex\")",
            "_slice > \"acme\"",
            "_slice is not null",
            "_slice == first_name",
            "_slice in (\"acme\", first_name)",
            "to_lower(_slice) == \"acme\"",
            "mv_intersects(_slice, [\"acme\", \"globex\"])",
            "mv_contains([\"acme\", \"globex\"], _slice)",
            "_slice == \"acme\" or emp_no > 10",
            "_slice == \"acme\" or (_slice == \"globex\" and emp_no > 10)",
            "not (_slice == \"acme\" and emp_no > 10)"
        )) {
            assertThat(condition, slices("from test metadata _slice | where " + condition), contains(NONE));
        }
    }

    /**
     * No document belongs to a slice whose name is invalid. Such a condition is left to the filter, which matches nothing.
     */
    public void testInvalidSliceNamesSelectNothing() {
        for (String condition : List.of(
            "_slice == \"acme,globex\"",
            "_slice == \"_all\"",
            "_slice == \"acme*\"",
            "_slice in (\"acme\", \"_all\")"
        )) {
            assertThat(condition, slices("from test metadata _slice | where " + condition), contains(NONE));
        }
    }

    public void testNoFilterSelectsNothing() {
        assertThat(slices("from test metadata _slice"), contains(NONE));
        assertThat(slices("from test | where emp_no > 10"), contains(NONE));
    }

    /**
     * The selection never replaces the filter it is derived from.
     */
    public void testFilterStaysInThePlan() {
        LogicalPlan plan = plan("from test metadata _slice | where _slice == \"acme\" and emp_no > 10");
        List<Filter> filters = new ArrayList<>();
        plan.forEachDown(Filter.class, filters::add);
        assertThat(filters, hasSize(1));
        assertThat(filters.getFirst().child(), equalTo(relations(plan).getFirst()));
        assertThat(SelectSlicesFromFilter.selectedSlices(filters.getFirst().condition()), equalTo(named("acme")));
        assertThat(
            filters.getFirst().condition().anyMatch(e -> e instanceof Expression x && x.sourceText().contains("emp_no > 10")),
            equalTo(true)
        );
    }

    /**
     * The selection belongs to the source the filter applies to. The index of a lookup join is another source.
     */
    public void testLookupJoinKeepsSelectionOnTheLeft() {
        LogicalPlan plan = plan("""
            from test metadata _slice
            | eval language_code = languages
            | lookup join languages_lookup on language_code
            | where _slice == "acme" and language_name == "English"
            """);
        assertThat(slices(plan), contains(named("acme")));
        assertThat(PlannerUtils.sliceSelection(new FragmentExec(plan)), equalTo(named("acme")));
    }

    /**
     * The shards of a plan are resolved together, so a plan only selects slices when all its sources agree on them.
     */
    public void testPlanSelectsSlicesWhenItsSourcesAgree() {
        assumeTrue("requires subqueries in FROM", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertThat(
            PlannerUtils.sliceSelection(new FragmentExec(plan("from test metadata _slice | where _slice == \"acme\" | limit 3"))),
            equalTo(named("acme"))
        );
        assertThat(PlannerUtils.sliceSelection(new FragmentExec(plan("from test metadata _slice | limit 3"))), equalTo(NONE));
        LogicalPlan disagree = planSubquery("""
            from (from test metadata _slice | where _slice == "acme"), (from test metadata _slice | where emp_no > 10)
            | keep emp_no, _slice
            """);
        assertThat(PlannerUtils.sliceSelection(new FragmentExec(disagree)), equalTo(NONE));
        LogicalPlan agree = planSubquery("""
            from (from test metadata _slice), (from test metadata _slice | where emp_no > 10)
            | where _slice == "acme"
            | keep emp_no, _slice
            """);
        assertThat(PlannerUtils.sliceSelection(new FragmentExec(agree)), equalTo(named("acme")));
    }

    /**
     * Each subquery is a source of its own: a filter inside one says nothing about the others.
     */
    public void testFilterInsideSubqueryOnlySelectsForThatSubquery() {
        assumeTrue("requires subqueries in FROM", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = planSubquery("""
            from (from test metadata _slice | where _slice == "acme"), (from test metadata _slice | where emp_no > 10)
            | keep emp_no, _slice
            """);
        assertThat(slices(plan), contains(named("acme"), NONE));

        plan = planSubquery("""
            from (from test metadata _slice | where _slice == "acme"), (from test metadata _slice | where _slice in ("globex", "initech"))
            | keep emp_no, _slice
            """);
        assertThat(slices(plan), contains(named("acme"), named("globex", "initech")));
    }

    /**
     * A filter over several subqueries applies to the rows of all of them, so it selects slices for each one it reaches. It
     * combines with the filter of a subquery like any two filters do.
     */
    public void testFilterOverSubqueriesSelectsForEachSubqueryItReaches() {
        assumeTrue("requires subqueries in FROM", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = planSubquery("""
            from (from test metadata _slice), (from test metadata _slice | where emp_no > 10)
            | where _slice == "acme"
            | keep emp_no, _slice
            """);
        assertThat(slices(plan), contains(named("acme"), named("acme")));

        plan = planSubquery("""
            from (from test metadata _slice | where _slice in ("acme", "globex")), (from test metadata _slice | limit 5)
            | where _slice == "acme"
            | keep emp_no, _slice
            """);
        // the filter cannot be applied before the limit of the second subquery
        assertThat(slices(plan), contains(named("acme"), NONE));
    }

    /**
     * A knn function searches the slices of its source, so the conditions that select them are not passed to it as filters.
     * Every other condition and-ed with the function still is.
     */
    public void testKnnDoesNotFilterOnSelectedSlices() {
        assertThat(knnFilters("knn(dense_vector, [0, 1, 2]) and _slice == \"acme\""), empty());
        assertThat(knnFilters("_slice in (\"acme\", \"globex\") and knn(dense_vector, [0, 1, 2])"), empty());
        assertThat(knnFilters("knn(dense_vector, [0, 1, 2]) and _slice == \"acme\" and integer > 10"), contains("integer > 10"));
        assertThat(
            knnFilters("integer > 10 and (_slice == \"acme\" and keyword == \"a\") and knn(dense_vector, [0, 1, 2])"),
            containsInAnyOrder("integer > 10", "keyword == \"a\"")
        );
        // the selection of the source and the filter stay in the plan
        LogicalPlan plan = planTypes("from types metadata _slice | where knn(dense_vector, [0, 1, 2]) and _slice == \"acme\"");
        assertThat(slices(plan), contains(named("acme")));
    }

    /**
     * A condition that does not select the slices of the source is a filter like any other for a knn function.
     */
    public void testKnnFiltersOnOtherSliceConditions() {
        assertThat(knnFilters("knn(dense_vector, [0, 1, 2]) and _slice != \"acme\""), contains("_slice != \"acme\""));
        assertThat(
            knnFilters("knn(dense_vector, [0, 1, 2]) and (_slice == \"acme\" or integer > 10)"),
            contains("_slice == \"acme\" or integer > 10")
        );
        // the function is not and-ed at the top level, so the source is not restricted to the slice
        assertThat(knnFilters("(knn(dense_vector, [0, 1, 2]) and _slice == \"acme\") or integer > 10"), contains("_slice == \"acme\""));
    }

    /** The source text of the filters of the knn function of {@code FROM types METADATA _slice | WHERE <condition>}. */
    private List<String> knnFilters(String condition) {
        LogicalPlan plan = planTypes("from types metadata _slice | where " + condition);
        List<Knn> knn = new ArrayList<>();
        plan.forEachExpressionDown(Knn.class, knn::add);
        assertThat(knn, hasSize(1));
        return knn.getFirst()
            .filterExpressions()
            .stream()
            .flatMap(filter -> Predicates.splitAnd(filter).stream())
            .map(Expression::sourceText)
            .toList();
    }

    private List<SliceSelection> slices(String query) {
        return slices(plan(query));
    }

    /** The slices recorded on each source of the plan, lookup indices excluded. */
    private static List<SliceSelection> slices(LogicalPlan plan) {
        return relations(plan).stream().map(EsRelation::slices).toList();
    }

    private static List<EsRelation> relations(LogicalPlan plan) {
        List<EsRelation> relations = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relation -> {
            if (relation.indexMode() != IndexMode.LOOKUP) {
                relations.add(relation);
            }
        });
        return relations;
    }

    private static SliceSelection named(String... slices) {
        return SliceSelection.of(List.of(slices));
    }
}
