/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The two halves of "the raw request filter must not be pushed into a view's source scan".
 *
 * <p>Neither half was covered before: the behaviour was only observable through wrong row counts in
 * {@code ViewRequestFilterIT}, which is how a marking bug survived once already — {@code Mapper} set the marker and
 * {@code Node.transformDown} silently discarded it, because {@code FragmentExec.equals} ignored the field and
 * {@code transformDown} keeps the original node when the rule's result compares equal.
 */
public class ViewBranchFilterPushdownTests extends ESTestCase {

    private static final QueryBuilder FILTER = QueryBuilders.termQuery("region", "eu");

    /**
     * {@code Mapper} must mark the fragments under a view branch and leave the fragments of a bare-index branch alone,
     * so that the two take different filter paths downstream. This is the test that fails if the marker is dropped
     * anywhere between being set and being read.
     */
    public void testMapperMarksOnlyViewBranchFragments() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("a_view", EsqlTestUtils.relation());
        branches.put("main", EsqlTestUtils.relation());
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, branches, Set.of("a_view"), List.of());

        PhysicalPlan physical = new Mapper().map(new Versioned<>(vua, TransportVersion.current()));

        List<Boolean> marks = collectFragmentMarks(physical);
        assertThat("one fragment per branch", marks.size(), equalTo(2));
        assertThat("exactly the view branch is marked", marks, containsInAnyOrder(true, false));
    }

    /** A plain {@code UnionAll} has no view branches, so nothing may be marked. */
    public void testMapperMarksNothingWithoutViewBranches() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("main", EsqlTestUtils.relation());
        branches.put("other", EsqlTestUtils.relation());
        ViewUnionAll vua = new ViewUnionAll(Source.EMPTY, branches, Set.of(), List.of());

        PhysicalPlan physical = new Mapper().map(new Versioned<>(vua, TransportVersion.current()));

        assertThat(collectFragmentMarks(physical), contains(false, false));
    }

    /**
     * The reading half: {@code integrateEsFilterIntoFragment} stamps the raw DSL onto ordinary fragments and skips
     * marked ones. A marked fragment keeps a {@code null} filter — the request filter reaches it as a logical
     * {@code Filter} above the view's output instead.
     */
    public void testIntegrateEsFilterSkipsMarkedFragmentsOnly() {
        FragmentExec plain = new FragmentExec(EsqlTestUtils.relation());
        FragmentExec viewBranch = new FragmentExec(EsqlTestUtils.relation()).asFromViewBranch();
        assertTrue("precondition: the marker survived construction", viewBranch.isFromViewBranch());

        FragmentExec stampedPlain = (FragmentExec) PlannerUtils.integrateEsFilterIntoFragment(plain, FILTER);
        FragmentExec stampedView = (FragmentExec) PlannerUtils.integrateEsFilterIntoFragment(viewBranch, FILTER);

        assertThat("a bare-index fragment takes the Lucene push-in path", stampedPlain.esFilter(), equalTo(FILTER));
        assertThat("a view-branch fragment must not receive the raw DSL", stampedView.esFilter(), nullValue());
    }

    /** A null request filter leaves every fragment alone, marked or not. */
    public void testIntegrateEsFilterWithNoFilterIsANoop() {
        FragmentExec viewBranch = new FragmentExec(EsqlTestUtils.relation()).asFromViewBranch();
        assertThat(PlannerUtils.integrateEsFilterIntoFragment(viewBranch, null), equalTo(viewBranch));
    }

    private static List<Boolean> collectFragmentMarks(PhysicalPlan plan) {
        List<Boolean> marks = new ArrayList<>();
        plan.forEachDown(FragmentExec.class, f -> marks.add(f.isFromViewBranch()));
        return marks;
    }
}
