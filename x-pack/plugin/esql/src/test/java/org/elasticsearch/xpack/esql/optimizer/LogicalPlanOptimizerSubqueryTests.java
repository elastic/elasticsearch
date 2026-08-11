/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Negative tests for FROM subqueries at the logical-optimizer stage; the positive coverage is in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    public void testUnboundedSortInsideInSubqueryInUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM test
                  | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
                 (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testUnboundedSortInsideInSubqueryInNestedUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM (FROM test
                        | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
                       (FROM test)
                 ),
                 (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testTotalBranchCountAtLimitPasses() {
        LogicalPlan plan = planSubquery("""
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """);

        assertThat(branchCountFailures(plan, 3), empty());
        List<Failure> failures = branchCountFailures(plan, 2);
        assertThat(failures, hasSize(1));
        assertThat(failures.getFirst().message(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testTotalBranchCountSumsNestedUnions() {
        LogicalPlan plan = planSubquery("""
            FROM test, (FROM test, (FROM languages))
            | STATS c = COUNT(*)
            """);

        assertThat(branchCountFailures(plan, 4), empty());
        List<Failure> failures = branchCountFailures(plan, 3);
        assertThat(failures, hasSize(1));
        assertThat(failures.getFirst().message(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
        // The pragma is named so the user can find and raise the limit, and the advice that does not depend on having pragma access is
        // there too.
        assertThat(failures.getFirst().message(), containsString("[max_query_branches] query pragma"));
        assertThat(failures.getFirst().message(), containsString("Reduce the number of sources"));
    }

    public void testTotalBranchCountFailureNamesOuterMostUnion() {
        LogicalPlan plan = planSubquery("""
            FROM test, (FROM test, (FROM languages))
            | STATS c = COUNT(*)
            """);

        List<UnionAll> unions = new ArrayList<>();
        plan.forEachDown(UnionAll.class, unions::add);
        assertThat(unions, hasSize(2));

        List<Failure> failures = branchCountFailures(plan, 3);
        assertThat(failures, hasSize(1));
        assertThat(failures.getFirst().node(), sameInstance(unions.getFirst()));
    }

    public void testTotalBranchCountCountsViewUnionAll() {
        LinkedHashMap<String, LogicalPlan> namedSubqueries = LinkedHashMap.newLinkedHashMap(2);
        namedSubqueries.put("view_0", relation("index1"));
        namedSubqueries.put("view_1", relation("index2"));
        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, namedSubqueries, List.of());
        UnionAll outer = new UnionAll(Source.EMPTY, List.of(relation("index3"), viewUnion), List.of());

        assertThat(branchCountFailures(outer, 4), empty());
        List<Failure> failures = branchCountFailures(outer, 3);
        assertThat(failures, hasSize(1));
        assertThat(failures.getFirst().message(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
    }

    public void testTotalBranchCountIgnoresPlansWithoutUnions() {
        LogicalPlan plan = planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """);
        assertThat(branchCountFailures(plan, 1), empty());
    }

    private static List<Failure> branchCountFailures(LogicalPlan plan, int maxBranches) {
        Failures failures = new Failures();
        UnionAll.checkTotalBranchCount(plan, maxBranches, failures);
        return List.copyOf(failures.failures());
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }
}
