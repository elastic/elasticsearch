/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.view.InMemoryViewResolver;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.anEmptyMap;

/**
 * Tests for the combined view + IN subquery resolution performed by {@code ViewResolver#replaceViews} followed by
 * {@code InSubqueryResolver#verify}, which expands view references and rewrites IN subqueries into Semi/Anti/MarkJoins in a single
 * traversal so that views referenced from inside IN subqueries and IN subqueries nested in view bodies are fully expanded.
 * Backed by the in-memory view fixture {@link InMemoryViewService}.
 */
public class ViewAndSubqueryResolverTests extends AbstractStatementParserTests {

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    private final QueryParams queryParams = new QueryParams();
    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryViewService viewService;
    private InMemoryViewResolver viewResolver;

    @Before
    public void setup() {
        assumeTrue("requires WHERE IN subquery capability", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        viewService = InMemoryViewService.makeViewService();
        viewResolver = viewService.getViewResolver();
        for (String index : List.of("employees", "departments", "teams")) {
            viewService.addIndex(projectId, index);
        }
    }

    @After
    public void teardown() {
        if (viewService != null) {
            viewService.close();
        }
    }

    // ---- resolution results ----

    /*
     * Filter[?emp_no > 1[INTEGER]]
     * \_UnresolvedRelation[employees]
     */
    public void testPlanWithoutViewsOrSubqueriesIsUnchanged() {
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE emp_no > 1");

        assertFalse(result.hasInSubquery());
        assertThat(result.viewQueries(), anEmptyMap());

        Filter filter = as(result.plan(), Filter.class);
        UnresolvedRelation relation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("employees", relation.indexPattern().indexPattern());
    }

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_NamedSubquery[dept_view]
     *     \_Keep[[?dept_id]]
     *       \_UnresolvedRelation[departments]
     */
    public void testViewReferencedInsideInSubqueryIsExpanded() {
        addView("dept_view", "FROM departments | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE dept_id IN (FROM dept_view | KEEP dept_id)");

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");
        Keep keep = as(semiJoin.right(), Keep.class);
        NamedSubquery namedSubquery = as(keep.child(), NamedSubquery.class);
        keep = as(namedSubquery.child(), Keep.class);
        assertUnresolvedRelation(keep.child(), "departments");
    }

    /*
     * AntiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_NamedSubquery[dept_view]
     *     \_Keep[[?dept_id]]
     *       \_UnresolvedRelation[departments]
     */
    public void testNotInSubqueryReferencingViewProducesAntiJoin() {
        addView("dept_view", "FROM departments | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE dept_id NOT IN (FROM dept_view | KEEP dept_id)");

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view"), result.viewQueries().keySet());

        AntiJoin antiJoin = as(result.plan(), AntiJoin.class);
        assertInSubqueryJoinKey(antiJoin, "dept_id");
        assertUnresolvedRelation(antiJoin.left(), "employees");
        Keep keep = as(antiJoin.right(), Keep.class);
        NamedSubquery namedSubquery = as(keep.child(), NamedSubquery.class);
        keep = as(namedSubquery.child(), Keep.class);
        assertUnresolvedRelation(keep.child(), "departments");
    }

    /*
     * NamedSubquery[v_with_in]
     * \_SemiJoin[[?emp_no],[]]
     *   |_UnresolvedRelation[employees]
     *   \_Keep[[?emp_no]]
     *     \_UnresolvedRelation[departments]
     */
    public void testInSubqueryNestedInViewBodyIsResolved() {
        addView("v_with_in", "FROM employees | WHERE emp_no IN (FROM departments | KEEP emp_no)");
        ViewResolver.ViewResolutionResult result = resolve("FROM v_with_in");

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("v_with_in"), result.viewQueries().keySet());

        NamedSubquery namedSubquery = as(result.plan(), NamedSubquery.class);
        SemiJoin semiJoin = as(namedSubquery.child(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "emp_no");
        assertUnresolvedRelation(semiJoin.left(), "employees");
        Keep keep = as(semiJoin.right(), Keep.class);
        assertUnresolvedRelation(keep.child(), "departments");
    }

    /*
     * SemiJoin[[?emp_no],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?emp_no]]
     *   \_NamedSubquery[in_layer_3]
     *     \_Keep[[?emp_no]]
     *       \_SemiJoin[[?emp_no],[]]
     *         |_UnresolvedRelation[employees]
     *         \_Keep[[?emp_no]]
     *           \_NamedSubquery[in_layer_2]
     *             \_Keep[[?emp_no]]
     *               \_SemiJoin[[?emp_no],[]]
     *                 |_UnresolvedRelation[employees]
     *                 \_Keep[[?emp_no]]
     *                   \_NamedSubquery[in_layer_1]
     *                     \_Keep[[?emp_no]]
     *                       \_SemiJoin[[?emp_no],[]]
     *                         |_UnresolvedRelation[employees]
     *                         \_Keep[[?emp_no]]
     *                           \_UnresolvedRelation[employees]
     */
    public void testChainedViewsResolveToFixedPoint() {
        addView("in_layer_1", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_2", "FROM employees | WHERE emp_no IN (FROM in_layer_1 | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_3", "FROM employees | WHERE emp_no IN (FROM in_layer_2 | KEEP emp_no) | KEEP emp_no");
        ViewResolver.ViewResolutionResult result = resolve("FROM employees | WHERE emp_no IN (FROM in_layer_3 | KEEP emp_no)");

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("in_layer_1", "in_layer_2", "in_layer_3"), result.viewQueries().keySet());

        // Each nested layer expands to SemiJoin(left=employees, right=Keep -> NamedSubquery[layer] -> Keep -> next SemiJoin).
        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        for (String layer : List.of("in_layer_3", "in_layer_2", "in_layer_1")) {
            assertInSubqueryJoinKey(semiJoin, "emp_no");
            assertUnresolvedRelation(semiJoin.left(), "employees");
            Keep keep = as(semiJoin.right(), Keep.class);
            NamedSubquery namedSubquery = as(keep.child(), NamedSubquery.class);
            assertEquals(layer, namedSubquery.name());
            Keep viewBody = as(namedSubquery.child(), Keep.class);
            semiJoin = as(viewBody.child(), SemiJoin.class);
        }
        // Innermost layer (in_layer_1's body) reads the concrete `employees` index, so its right side is a plain subquery.
        assertInSubqueryJoinKey(semiJoin, "emp_no");
        assertUnresolvedRelation(semiJoin.left(), "employees");
        Keep innermostKeep = as(semiJoin.right(), Keep.class);
        assertUnresolvedRelation(innermostKeep.child(), "employees");
    }

    // ---- IN subqueries mixed with subqueries in the FROM command ----

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_ViewUnionAll[[dept_view_a, dept_view_b]]
     *     |_NamedSubquery[dept_view_a]
     *     | \_Keep[[?dept_id]]
     *     |   \_UnresolvedRelation[departments]
     *     \_NamedSubquery[dept_view_b]
     *       \_Keep[[?dept_id]]
     *         \_UnresolvedRelation[teams]
     */
    public void testInSubqueryReferencingFromWithMultipleViews() {
        addView("dept_view_a", "FROM departments | KEEP dept_id");
        addView("dept_view_b", "FROM teams | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id IN (FROM dept_view_a, dept_view_b | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_a", "dept_view_b"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");

        Keep keep = as(semiJoin.right(), Keep.class);
        ViewUnionAll union = as(keep.child(), ViewUnionAll.class);
        assertEquals(List.of("dept_view_a", "dept_view_b"), List.copyOf(union.namedSubqueries().keySet()));

        NamedSubquery viewA = as(union.children().get(0), NamedSubquery.class);
        assertEquals("dept_view_a", viewA.name());
        assertUnresolvedRelation(as(viewA.child(), Keep.class).child(), "departments");

        NamedSubquery viewB = as(union.children().get(1), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        assertUnresolvedRelation(as(viewB.child(), Keep.class).child(), "teams");
    }

    /*
     * AntiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_ViewUnionAll[[dept_view_a, dept_view_b]]
     *     |_NamedSubquery[dept_view_a]
     *     | \_Keep[[?dept_id]]
     *     |   \_UnresolvedRelation[departments]
     *     \_NamedSubquery[dept_view_b]
     *       \_Keep[[?dept_id]]
     *         \_UnresolvedRelation[teams]
     */
    public void testNotInSubqueryReferencingFromWithMultipleViews() {
        addView("dept_view_a", "FROM departments | KEEP dept_id");
        addView("dept_view_b", "FROM teams | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id NOT IN (FROM dept_view_a, dept_view_b | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_a", "dept_view_b"), result.viewQueries().keySet());

        AntiJoin antiJoin = as(result.plan(), AntiJoin.class);
        assertInSubqueryJoinKey(antiJoin, "dept_id");
        assertUnresolvedRelation(antiJoin.left(), "employees");

        Keep keep = as(antiJoin.right(), Keep.class);
        ViewUnionAll union = as(keep.child(), ViewUnionAll.class);
        assertEquals(List.of("dept_view_a", "dept_view_b"), List.copyOf(union.namedSubqueries().keySet()));

        NamedSubquery viewA = as(union.children().get(0), NamedSubquery.class);
        assertEquals("dept_view_a", viewA.name());
        assertUnresolvedRelation(as(viewA.child(), Keep.class).child(), "departments");

        NamedSubquery viewB = as(union.children().get(1), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        assertUnresolvedRelation(as(viewB.child(), Keep.class).child(), "teams");
    }

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_UnionAll[[]]
     *     |_Subquery[]
     *     | \_Keep[[?dept_id]]
     *     |   \_NamedSubquery[dept_view_a]
     *     |     \_Keep[[?dept_id]]
     *     |       \_UnresolvedRelation[departments]
     *     \_Subquery[]
     *       \_Keep[[?dept_id]]
     *         \_NamedSubquery[dept_view_b]
     *           \_Keep[[?dept_id]]
     *             \_UnresolvedRelation[teams]
     */
    public void testInSubqueryReferencingMultipleFromSubqueriesEachWithAView() {
        addView("dept_view_a", "FROM departments | KEEP dept_id");
        addView("dept_view_b", "FROM teams | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id IN "
                + "(FROM (FROM dept_view_a | KEEP dept_id), (FROM dept_view_b | KEEP dept_id) | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_a", "dept_view_b"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");

        Keep keep = as(semiJoin.right(), Keep.class);
        // User-written FROM subqueries produce a plain UnionAll of Subquery branches, not a view-produced ViewUnionAll.
        UnionAll union = as(keep.child(), UnionAll.class);
        assertEquals(UnionAll.class, union.getClass());
        assertEquals(2, union.children().size());

        Subquery branchA = as(union.children().get(0), Subquery.class);
        assertEquals(Subquery.class, branchA.getClass());
        NamedSubquery viewA = as(as(branchA.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals("dept_view_a", viewA.name());
        assertUnresolvedRelation(as(viewA.child(), Keep.class).child(), "departments");

        Subquery branchB = as(union.children().get(1), Subquery.class);
        assertEquals(Subquery.class, branchB.getClass());
        NamedSubquery viewB = as(as(branchB.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        assertUnresolvedRelation(as(viewB.child(), Keep.class).child(), "teams");
    }

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[departments]
     *     \_Subquery[]
     *       \_Keep[[?dept_id]]
     *         \_NamedSubquery[dept_view_b]
     *           \_Keep[[?dept_id]]
     *             \_UnresolvedRelation[teams]
     */
    public void testInSubqueryReferencingMixedConcreteIndexAndViewInFrom() {
        addView("dept_view_b", "FROM teams | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id IN (FROM departments, (FROM dept_view_b | KEEP dept_id) | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_b"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");

        Keep keep = as(semiJoin.right(), Keep.class);
        UnionAll union = as(keep.child(), UnionAll.class);
        assertEquals(UnionAll.class, union.getClass());
        assertEquals(2, union.children().size());

        // The concrete index stays a bare UnresolvedRelation; only the view-referencing subquery is wrapped.
        assertUnresolvedRelation(union.children().get(0), "departments");

        Subquery branchB = as(union.children().get(1), Subquery.class);
        assertEquals(Subquery.class, branchB.getClass());
        NamedSubquery viewB = as(as(branchB.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        assertUnresolvedRelation(as(viewB.child(), Keep.class).child(), "teams");
    }

    // ---- IN subquery referencing multiple views, each view containing its own IN subquery ----

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_ViewUnionAll[[dept_view_a, dept_view_b]]
     *     |_NamedSubquery[dept_view_a]
     *     | \_Keep[[?dept_id]]
     *     |   \_SemiJoin[[?dept_id],[]]
     *     |     |_UnresolvedRelation[departments]
     *     |     \_Keep[[?dept_id]]
     *     |       \_UnresolvedRelation[departments]
     *     \_NamedSubquery[dept_view_b]
     *       \_Keep[[?dept_id]]
     *         \_SemiJoin[[?dept_id],[]]
     *           |_UnresolvedRelation[teams]
     *           \_Keep[[?dept_id]]
     *             \_UnresolvedRelation[teams]
     */
    public void testInSubqueryReferencingFromWithMultipleViewsEachContainingInSubquery() {
        addView("dept_view_a", "FROM departments | WHERE dept_id IN (FROM departments | KEEP dept_id) | KEEP dept_id");
        addView("dept_view_b", "FROM teams | WHERE dept_id IN (FROM teams | KEEP dept_id) | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id IN (FROM dept_view_a, dept_view_b | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_a", "dept_view_b"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");

        Keep keep = as(semiJoin.right(), Keep.class);
        ViewUnionAll union = as(keep.child(), ViewUnionAll.class);
        assertEquals(List.of("dept_view_a", "dept_view_b"), List.copyOf(union.namedSubqueries().keySet()));

        NamedSubquery viewA = as(union.children().get(0), NamedSubquery.class);
        assertEquals("dept_view_a", viewA.name());
        SemiJoin viewASemiJoin = as(as(viewA.child(), Keep.class).child(), SemiJoin.class);
        assertInSubqueryJoinKey(viewASemiJoin, "dept_id");
        assertUnresolvedRelation(viewASemiJoin.left(), "departments");
        assertUnresolvedRelation(as(viewASemiJoin.right(), Keep.class).child(), "departments");

        NamedSubquery viewB = as(union.children().get(1), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        SemiJoin viewBSemiJoin = as(as(viewB.child(), Keep.class).child(), SemiJoin.class);
        assertInSubqueryJoinKey(viewBSemiJoin, "dept_id");
        assertUnresolvedRelation(viewBSemiJoin.left(), "teams");
        assertUnresolvedRelation(as(viewBSemiJoin.right(), Keep.class).child(), "teams");
    }

    /*
     * SemiJoin[[?dept_id],[]]
     * |_UnresolvedRelation[employees]
     * \_Keep[[?dept_id]]
     *   \_UnionAll[[]]
     *     |_Subquery[]
     *     | \_Keep[[?dept_id]]
     *     |   \_NamedSubquery[dept_view_a]
     *     |     \_Keep[[?dept_id]]
     *     |       \_SemiJoin[[?dept_id],[]]
     *     |         |_UnresolvedRelation[departments]
     *     |         \_Keep[[?dept_id]]
     *     |           \_UnresolvedRelation[departments]
     *     \_Subquery[]
     *       \_Keep[[?dept_id]]
     *         \_NamedSubquery[dept_view_b]
     *           \_Keep[[?dept_id]]
     *             \_SemiJoin[[?dept_id],[]]
     *               |_UnresolvedRelation[teams]
     *               \_Keep[[?dept_id]]
     *                 \_UnresolvedRelation[teams]
     */
    public void testInSubqueryReferencingMultipleFromSubqueriesEachWithAViewContainingInSubquery() {
        addView("dept_view_a", "FROM departments | WHERE dept_id IN (FROM departments | KEEP dept_id) | KEEP dept_id");
        addView("dept_view_b", "FROM teams | WHERE dept_id IN (FROM teams | KEEP dept_id) | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM employees | WHERE dept_id IN "
                + "(FROM (FROM dept_view_a | KEEP dept_id), (FROM dept_view_b | KEEP dept_id) | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("dept_view_a", "dept_view_b"), result.viewQueries().keySet());

        SemiJoin semiJoin = as(result.plan(), SemiJoin.class);
        assertInSubqueryJoinKey(semiJoin, "dept_id");
        assertUnresolvedRelation(semiJoin.left(), "employees");

        Keep keep = as(semiJoin.right(), Keep.class);
        UnionAll union = as(keep.child(), UnionAll.class);
        assertEquals(UnionAll.class, union.getClass());
        assertEquals(2, union.children().size());

        Subquery branchA = as(union.children().get(0), Subquery.class);
        assertEquals(Subquery.class, branchA.getClass());
        NamedSubquery viewA = as(as(branchA.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals("dept_view_a", viewA.name());
        SemiJoin viewASemiJoin = as(as(viewA.child(), Keep.class).child(), SemiJoin.class);
        assertInSubqueryJoinKey(viewASemiJoin, "dept_id");
        assertUnresolvedRelation(viewASemiJoin.left(), "departments");
        assertUnresolvedRelation(as(viewASemiJoin.right(), Keep.class).child(), "departments");

        Subquery branchB = as(union.children().get(1), Subquery.class);
        assertEquals(Subquery.class, branchB.getClass());
        NamedSubquery viewB = as(as(branchB.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals("dept_view_b", viewB.name());
        SemiJoin viewBSemiJoin = as(as(viewB.child(), Keep.class).child(), SemiJoin.class);
        assertInSubqueryJoinKey(viewBSemiJoin, "dept_id");
        assertUnresolvedRelation(viewBSemiJoin.left(), "teams");
        assertUnresolvedRelation(as(viewBSemiJoin.right(), Keep.class).child(), "teams");
    }

    // ---- main FROM and IN subquery both reference multiple view subqueries ----

    /*
     * AntiJoin[[?dept_id],[]]
     * |_UnionAll[[]]
     * | |_Subquery[]
     * | | \_Keep[[?dept_id]]
     * | |   \_NamedSubquery[main_view_a]
     * | |     \_Keep[[?dept_id]]
     * | |       \_UnresolvedRelation[departments]
     * | \_Subquery[]
     * |   \_Keep[[?dept_id]]
     * |     \_NamedSubquery[main_view_b]
     * |       \_Keep[[?dept_id]]
     * |         \_UnresolvedRelation[teams]
     * \_Keep[[?dept_id]]
     *   \_UnionAll[[]]
     *     |_Subquery[]
     *     | \_Keep[[?dept_id]]
     *     |   \_NamedSubquery[in_view_a]
     *     |     \_Keep[[?dept_id]]
     *     |       \_UnresolvedRelation[departments]
     *     \_Subquery[]
     *       \_Keep[[?dept_id]]
     *         \_NamedSubquery[in_view_b]
     *           \_Keep[[?dept_id]]
     *             \_UnresolvedRelation[teams]
     */
    public void testMainFromAndNotInSubqueryEachReferenceMultipleViewSubqueries() {
        addView("main_view_a", "FROM departments | KEEP dept_id");
        addView("main_view_b", "FROM teams | KEEP dept_id");
        addView("in_view_a", "FROM departments | KEEP dept_id");
        addView("in_view_b", "FROM teams | KEEP dept_id");
        ViewResolver.ViewResolutionResult result = resolve(
            "FROM (FROM main_view_a | KEEP dept_id), (FROM main_view_b | KEEP dept_id) "
                + "| WHERE dept_id NOT IN (FROM (FROM in_view_a | KEEP dept_id), (FROM in_view_b | KEEP dept_id) | KEEP dept_id)"
        );

        assertTrue(result.hasInSubquery());
        assertEquals(Set.of("main_view_a", "main_view_b", "in_view_a", "in_view_b"), result.viewQueries().keySet());

        AntiJoin antiJoin = as(result.plan(), AntiJoin.class);
        assertInSubqueryJoinKey(antiJoin, "dept_id");

        // Main FROM: a plain UnionAll of the two view-referencing subqueries forms the join's left side.
        UnionAll mainFrom = as(antiJoin.left(), UnionAll.class);
        assertEquals(UnionAll.class, mainFrom.getClass());
        assertEquals(2, mainFrom.children().size());
        assertViewSubqueryBranch(mainFrom.children().get(0), "main_view_a", "departments");
        assertViewSubqueryBranch(mainFrom.children().get(1), "main_view_b", "teams");

        // NOT IN subquery: the join's right side is the subquery's KEEP over a plain UnionAll of two view-referencing subqueries.
        Keep keep = as(antiJoin.right(), Keep.class);
        UnionAll inSubquery = as(keep.child(), UnionAll.class);
        assertEquals(UnionAll.class, inSubquery.getClass());
        assertEquals(2, inSubquery.children().size());
        assertViewSubqueryBranch(inSubquery.children().get(0), "in_view_a", "departments");
        assertViewSubqueryBranch(inSubquery.children().get(1), "in_view_b", "teams");
    }

    // ---- IN_SUBQUERY telemetry ----

    public void testInSubqueryMetricCountedOncePerQuery() {
        addView("in_layer_1", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_2", "FROM employees | WHERE emp_no IN (FROM in_layer_1 | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_3", "FROM employees | WHERE emp_no IN (FROM in_layer_2 | KEEP emp_no) | KEEP emp_no");

        long inSubqueryCount = inSubqueryMetric("FROM employees | WHERE emp_no IN (FROM in_layer_3 | KEEP emp_no)");
        assertEquals("IN_SUBQUERY must be counted once per query", 1L, inSubqueryCount);
    }

    public void testInSubqueryMetricCountedOnceForSingleSubquery() {
        long inSubqueryCount = inSubqueryMetric("FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no)");
        assertEquals("IN_SUBQUERY must be counted once per query", 1L, inSubqueryCount);
    }

    public void testInSubqueryMetricCountedForInSubqueryInsideViews() {
        addView("in_layer_1", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_2", "FROM employees | WHERE emp_no IN (FROM in_layer_1 | KEEP emp_no) | KEEP emp_no");
        addView("in_layer_3", "FROM employees | WHERE emp_no IN (FROM in_layer_2 | KEEP emp_no) | KEEP emp_no");

        long inSubqueryCount = inSubqueryMetric("FROM in_layer_1, in_layer_2, in_layer_3");
        assertEquals("IN_SUBQUERY inside a view definition must be counted once per query", 1L, inSubqueryCount);
    }

    public void testInSubqueryReferencingMultipleViewsCountedOnce() {
        addView("dept_view_a", "FROM departments | KEEP dept_id");
        addView("dept_view_b", "FROM teams | KEEP dept_id");
        long inSubqueryCount = inSubqueryMetric(
            "FROM employees | WHERE dept_id IN "
                + "(FROM (FROM dept_view_a | KEEP dept_id), (FROM dept_view_b | KEEP dept_id) | KEEP dept_id)"
        );
        assertEquals("IN_SUBQUERY must be counted once per query", 1L, inSubqueryCount);
    }

    // ---- helpers ----

    private static void assertInSubqueryJoinKey(AbstractSubqueryJoin join, String expectedKey) {
        JoinConfig joinConfig = as(join.config(), JoinConfig.class);
        assertEquals(1, joinConfig.leftFields().size());
        UnresolvedAttribute joinKey = as(joinConfig.leftFields().get(0), UnresolvedAttribute.class);
        assertEquals(expectedKey, joinKey.name());
        assertEquals(0, joinConfig.rightFields().size());
    }

    private static void assertUnresolvedRelation(LogicalPlan plan, String indexPattern) {
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(indexPattern, relation.indexPattern().indexPattern());
    }

    /**
     * Asserts {@code plan} is a user-written FROM subquery branch of shape
     * {@code Subquery -> Keep -> NamedSubquery[viewName] -> Keep -> UnresolvedRelation[index]} — the shape a FROM subquery such as
     * {@code (FROM <view> | KEEP <field>)} expands to once {@code <view>} is resolved. The branch is a plain {@link Subquery}, not a
     * view-produced {@link NamedSubquery}, because it was written by the user.
     */
    private static void assertViewSubqueryBranch(LogicalPlan plan, String viewName, String index) {
        Subquery branch = as(plan, Subquery.class);
        assertEquals(Subquery.class, branch.getClass());
        NamedSubquery view = as(as(branch.child(), Keep.class).child(), NamedSubquery.class);
        assertEquals(viewName, view.name());
        assertUnresolvedRelation(as(view.child(), Keep.class).child(), index);
    }

    private ViewResolver.ViewResolutionResult resolve(String query) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        viewResolver.replaceViews(query(query), null, this::parse, future.delegateFailureAndWrap((l, viewResult) -> {
            // Validate: no InSubquery expressions should survive view+subquery resolution.
            InSubqueryResolver.verify(viewResult.plan());
            l.onResponse(viewResult);
        }));
        return future.actionGet();
    }

    /**
     * Collects the {@code IN_SUBQUERY} feature metric exactly as {@code EsqlSession} does: increment once per query when the resolver
     * reports it rewrote any IN subquery — whether the IN subquery appeared directly in the query or only inside a view definition.
     * Returns the resulting {@code IN_SUBQUERY} counter value.
     */
    private long inSubqueryMetric(String query) {
        Metrics metrics = new Metrics(TEST_FUNCTION_REGISTRY, true, true);
        if (resolve(query).hasInSubquery()) {
            metrics.inc(FeatureMetric.IN_SUBQUERY);
        }
        return metrics.stats().get("features." + FeatureMetric.IN_SUBQUERY);
    }

    private LogicalPlan parse(String query, String viewName) {
        return TEST_PARSER.parseView(query, queryParams, new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    private void addView(String name, String query) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        viewService.putView(projectId, request, future);
        future.actionGet();
    }
}
