/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit coverage for {@link FlattenUnionAll} in isolation. Pins the lifting behaviour the post-optimization
 * "no nested UnionAll" invariant ({@code UnionAll#checkNestedUnionAlls}) relies on: an inner {@code UnionAll} that
 * reaches the outer union through view-folding (directly, or through a {@code Project}/{@code Eval} alignment wrapper)
 * is lifted; a {@code Subquery} boundary stops the lift; and a non-nested union is returned untouched.
 *
 * <p>Branches are built with matching output schemas (same names, different ids) so the unions are valid and the
 * by-name attribute remap has something to bite on.
 */
public class FlattenUnionAllTests extends ESTestCase {

    /** A leaf branch whose single output attribute is {@code a:INTEGER} with a fresh id. */
    private static LogicalPlan branch() {
        // EsRelation built by relation() has empty output; wrap a fresh "a" attribute in a Project so each branch
        // carries the same name but a distinct id, matching how union branches differ only by id.
        FieldAttribute a = fieldAttribute("a", DataType.INTEGER);
        return new Project(Source.EMPTY, relation(), List.of(a));
    }

    private static Attribute soleOutput(LogicalPlan plan) {
        assertThat(plan.output().size(), equalTo(1));
        return plan.output().getFirst();
    }

    private static UnionAll union(List<LogicalPlan> children) {
        // Union output: same name, fresh id (a union renames branch attributes to its own ids).
        return new UnionAll(Source.EMPTY, children, List.of(fieldAttribute("a", DataType.INTEGER)));
    }

    /** (a) A directly nested {@code UnionAll} child is lifted: its branches replace it verbatim in the outer union. */
    public void testDirectNestedUnionIsLifted() {
        LogicalPlan b1 = branch();
        LogicalPlan b2 = branch();
        LogicalPlan b3 = branch();
        UnionAll inner = union(List.of(b2, b3));
        UnionAll outer = union(List.of(b1, inner));

        UnionAll flattened = as(new FlattenUnionAll().apply(outer), UnionAll.class);

        assertThat(flattened.children(), contains(b1, b2, b3));
        // Outer output is preserved.
        assertThat(flattened.output(), equalTo(outer.output()));
    }

    /**
     * (b) A {@code Project}-wrapped inner {@code UnionAll} (the alignment-wrapper case) is distributed over the inner
     * branches and lifted; each lifted wrapper's reference is remapped by name onto its own inner branch's attribute.
     */
    public void testProjectWrappedInnerUnionIsDistributedAndRemapped() {
        LogicalPlan b1 = branch();
        LogicalPlan i1 = branch();
        LogicalPlan i2 = branch();
        UnionAll inner = union(List.of(i1, i2));

        // Alignment Project: references the inner union's output attribute.
        Attribute innerUnionAttr = soleOutput(inner);
        Project wrapper = new Project(Source.EMPTY, inner, List.of(innerUnionAttr));

        UnionAll outer = union(List.of(b1, wrapper));

        UnionAll flattened = as(new FlattenUnionAll().apply(outer), UnionAll.class);

        // b1, then one wrapper per inner branch.
        assertThat(flattened.children().size(), equalTo(3));
        assertThat(flattened.children().get(0), sameInstance(b1));

        Project lifted1 = as(flattened.children().get(1), Project.class);
        assertThat(lifted1.child(), sameInstance(i1));
        // The wrapper's projection now points at i1's attribute, not the inner union's.
        assertThat(onlyProjection(lifted1).toAttribute(), equalTo(soleOutput(i1)));

        Project lifted2 = as(flattened.children().get(2), Project.class);
        assertThat(lifted2.child(), sameInstance(i2));
        assertThat(onlyProjection(lifted2).toAttribute(), equalTo(soleOutput(i2)));
    }

    /** (c) An {@code Eval}-over-{@code Project}-over-{@code UnionAll} alignment wrapper also flattens. */
    public void testEvalOverProjectWrappedInnerUnionFlattens() {
        LogicalPlan b1 = branch();
        LogicalPlan i1 = branch();
        LogicalPlan i2 = branch();
        UnionAll inner = union(List.of(i1, i2));

        Attribute innerUnionAttr = soleOutput(inner);
        Project innerProject = new Project(Source.EMPTY, inner, List.of(innerUnionAttr));
        // Eval on top of the alignment Project; references the attribute below it (preserved across the rebuild).
        Eval eval = new Eval(Source.EMPTY, innerProject, List.of(new Alias(Source.EMPTY, "e", fieldAttribute("e", DataType.INTEGER))));

        UnionAll outer = union(List.of(b1, eval));

        UnionAll flattened = as(new FlattenUnionAll().apply(outer), UnionAll.class);

        assertThat(flattened.children().size(), equalTo(3));
        assertThat(flattened.children().get(0), sameInstance(b1));

        // Each lifted branch keeps the Eval-over-Project wrapper, now bottoming out in an inner branch.
        Eval liftedEval1 = as(flattened.children().get(1), Eval.class);
        Project liftedProject1 = as(liftedEval1.child(), Project.class);
        assertThat(liftedProject1.child(), sameInstance(i1));

        Eval liftedEval2 = as(flattened.children().get(2), Eval.class);
        Project liftedProject2 = as(liftedEval2.child(), Project.class);
        assertThat(liftedProject2.child(), sameInstance(i2));
    }

    /**
     * (d) A {@code Subquery} boundary above the inner {@code UnionAll} marks user-written nesting; the lift stops at it
     * and leaves the nesting in place for the post-optimization verifier to reject.
     */
    public void testSubqueryBoundaryIsNotFlattened() {
        LogicalPlan b1 = branch();
        LogicalPlan i1 = branch();
        LogicalPlan i2 = branch();
        UnionAll inner = union(List.of(i1, i2));

        Subquery subquery = new Subquery(Source.EMPTY, inner);
        Project wrapper = new Project(Source.EMPTY, subquery, List.of(soleOutput(subquery)));

        UnionAll outer = union(List.of(b1, wrapper));

        LogicalPlan result = new FlattenUnionAll().apply(outer);

        // Returned unchanged — the inner UnionAll still hides behind the Subquery boundary.
        assertThat(result, sameInstance((LogicalPlan) outer));
    }

    /** (e) A non-nested {@code UnionAll} (plain branches, no inner union) is returned as the same instance. */
    public void testNonNestedUnionIsUnchanged() {
        UnionAll outer = union(List.of(branch(), branch()));

        LogicalPlan result = new FlattenUnionAll().apply(outer);

        assertThat(result, sameInstance((LogicalPlan) outer));
    }

    private static NamedExpression onlyProjection(Project project) {
        assertThat(project.projections().size(), equalTo(1));
        return project.projections().getFirst();
    }
}
