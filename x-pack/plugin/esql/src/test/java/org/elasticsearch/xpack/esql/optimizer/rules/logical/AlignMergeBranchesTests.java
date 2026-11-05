/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link AlignMergeBranches} on hand-built merges whose branches do not line up with the merge's output, the shape the
 * analyzer is free to leave them in.
 */
public class AlignMergeBranchesTests extends ESTestCase {

    /**
     * Each branch null-fills the columns it lacks and is projected to the merge's columns in the merge's order, whatever order it
     * produces them in.
     */
    public void testFillsMissingColumnsAndReorders() {
        Row first = row("a", "b");
        Row second = row("c", "a");
        UnionAll union = new UnionAll(Source.EMPTY, List.of(first, second), output("a", "b", "c"));

        MergePlan aligned = align(union);

        assertThat(aligned.output(), equalTo(union.output()));
        assertBranchColumns(aligned, "a", "b", "c");

        Eval firstFill = as(as(aligned.children().get(0), Project.class).child(), Eval.class);
        assertNullFills(firstFill, Map.of("c", INTEGER));
        assertThat(firstFill.child(), sameInstance(first));

        Eval secondFill = as(as(aligned.children().get(1), Project.class).child(), Eval.class);
        assertNullFills(secondFill, Map.of("b", INTEGER));
        assertThat(secondFill.child(), sameInstance(second));
    }

    /**
     * A branch that already produces the merge's columns, but not in its order, only needs the {@link Project}.
     */
    public void testReordersWithoutFilling() {
        Row branch = row("b", "a");
        Fork fork = new Fork(Source.EMPTY, List.of(branch, row("a", "b")), output("a", "b"));

        MergePlan aligned = align(fork);

        assertThat(aligned, instanceOf(Fork.class));
        assertBranchColumns(aligned, "a", "b");
        assertThat(as(aligned.children().get(0), Project.class).child(), sameInstance(branch));
    }

    /**
     * A branch ending in a {@link Project} over exactly the merge's columns is left alone, and so is a merge all of whose branches
     * are, which also makes the rule idempotent.
     */
    public void testAlignedBranchesAreLeftAlone() {
        UnionAll union = new UnionAll(Source.EMPTY, List.of(row("a", "b"), row("b", "a")), output("a", "b"));
        MergePlan aligned = align(union);

        assertThat(align(aligned), sameInstance(aligned));
    }

    /**
     * A null fill is typed like the column, except for types an {@code EVAL} cannot return: {@code UNSUPPORTED} is filled as
     * {@code KEYWORD} and a counter as its non-counter type.
     */
    public void testNullFillTypes() {
        List<Attribute> output = List.of(
            new ReferenceAttribute(Source.EMPTY, "a", INTEGER),
            new ReferenceAttribute(Source.EMPTY, "unsupported", UNSUPPORTED),
            new ReferenceAttribute(Source.EMPTY, "counter", COUNTER_LONG)
        );
        UnionAll union = new UnionAll(Source.EMPTY, List.of(row("a"), row("a")), output);

        MergePlan aligned = align(union);

        for (LogicalPlan branch : aligned.children()) {
            Eval fill = as(as(branch, Project.class).child(), Eval.class);
            assertNullFills(fill, Map.of("unsupported", KEYWORD, "counter", LONG));
        }
    }

    /**
     * A fill of a synthetic column is synthetic too, so that it is removed from the query's output like the column itself.
     */
    public void testNullFillOfSyntheticColumnIsSynthetic() {
        Attribute synthetic = new ReferenceAttribute(Source.EMPTY, null, "$$a$converted_to$long", LONG, null, null, true);
        UnionAll union = new UnionAll(Source.EMPTY, List.of(row("a"), row("a")), List.of(attr("a"), synthetic));

        MergePlan aligned = align(union);

        Alias fill = as(as(aligned.children().get(0), Project.class).child(), Eval.class).fields().getFirst();
        assertTrue(fill.synthetic());
    }

    /**
     * A branch over an index without any mapping only outputs the no-fields marker, which is not a merge column. Projecting it to the
     * merge's columns drops the marker, also when the merge has no columns at all.
     */
    public void testNoFieldsBranch() {
        LogicalPlan noFields = new LocalRelation(Source.EMPTY, NO_FIELDS, EmptyLocalSupplier.EMPTY);
        UnionAll withColumns = new UnionAll(Source.EMPTY, List.of(noFields, row("a")), output("a"));
        MergePlan aligned = align(withColumns);
        assertBranchColumns(aligned, "a");
        assertThat(as(as(aligned.children().get(0), Project.class).child(), Eval.class).child(), sameInstance(noFields));

        UnionAll withoutColumns = new UnionAll(Source.EMPTY, List.of(noFields, noFields), List.of());
        MergePlan alignedWithoutColumns = align(withoutColumns);
        for (LogicalPlan branch : alignedWithoutColumns.children()) {
            assertThat(as(branch, Project.class).projections(), empty());
        }

        // Aligning is idempotent: a branch already projected to nothing is left alone.
        assertThat(align(alignedWithoutColumns), sameInstance(alignedWithoutColumns));
    }

    /**
     * A single pass aligns nested merges too: the inner merge is aligned with its own output, which the outer merge's alignment
     * reads, and neither alignment changes a branch's output.
     */
    public void testNestedMerges() {
        UnionAll inner = new UnionAll(Source.EMPTY, List.of(row("a"), row("b")), output("a", "b"));
        UnionAll outer = new UnionAll(Source.EMPTY, List.of(inner, row("c")), output("a", "b", "c"));

        MergePlan aligned = align(outer);

        assertBranchColumns(aligned, "a", "b", "c");
        MergePlan alignedInner = as(as(as(aligned.children().get(0), Project.class).child(), Eval.class).child(), UnionAll.class);
        assertBranchColumns(alignedInner, "a", "b");
    }

    /**
     * A {@link BranchPlan} keeps its branch keys and identities.
     */
    public void testBranchPlanKeepsIdentities() {
        LinkedHashMap<String, LogicalPlan> branches = new LinkedHashMap<>();
        branches.put("my_view", row("a"));
        branches.put("main", row("b"));
        Map<String, BranchIdentity> identities = Map.of("my_view", BranchIdentity.view("my_view"));
        BranchPlan branchPlan = new BranchPlan(Source.EMPTY, branches, identities, output("a", "b"));

        BranchPlan aligned = as(align(branchPlan), BranchPlan.class);

        assertThat(aligned.namedSubqueries().keySet(), contains("my_view", "main"));
        assertThat(aligned.branchIdentities(), equalTo(branchPlan.branchIdentities()));
        assertBranchColumns(aligned, "a", "b");
    }

    /**
     * The analyzer leaves branches unaligned, so an analyzed plan has to be aligned before it is checked for consistency; the
     * check has to catch a merge whose branches do not line up with its output, whether a branch lacks a merge column or has one
     * the merge does not, and accept it once the branches are aligned.
     */
    public void testConsistencyCheckRequiresAlignedBranches() {
        UnionAll missingColumn = new UnionAll(Source.EMPTY, List.of(row("a", "b"), row("a")), output("a", "b"));
        assertTrue(hasConsistencyFailures(missingColumn));
        assertFalse(hasConsistencyFailures(align(missingColumn)));

        UnionAll extraColumn = new UnionAll(Source.EMPTY, List.of(row("a", "b"), row("a", "c")), output("a", "b"));
        assertTrue(hasConsistencyFailures(extraColumn));
        assertFalse(hasConsistencyFailures(align(extraColumn)));
    }

    private static boolean hasConsistencyFailures(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachDown(p -> PlanConsistencyChecker.checkPlan(p, failures));
        return failures.hasFailures();
    }

    private static MergePlan align(MergePlan merge) {
        return as(new AlignMergeBranches().apply(merge), MergePlan.class);
    }

    private static void assertBranchColumns(MergePlan merge, String... names) {
        assertThat(Expressions.names(merge.output()), contains(names));
        for (LogicalPlan branch : merge.children()) {
            assertThat(Expressions.names(as(branch, Project.class).output()), contains(names));
        }
    }

    private static void assertNullFills(Eval eval, Map<String, DataType> expectedTypes) {
        assertThat(eval.fields().size(), equalTo(expectedTypes.size()));
        for (Alias fill : eval.fields()) {
            Literal value = as(fill.child(), Literal.class);
            assertNull(value.value());
            assertThat(fill.name(), value.dataType(), equalTo(expectedTypes.get(fill.name())));
        }
    }

    private static Row row(String... names) {
        return new Row(
            Source.EMPTY,
            Arrays.stream(names).map(name -> new Alias(Source.EMPTY, name, Literal.integer(Source.EMPTY, 1))).toList()
        );
    }

    private static List<Attribute> output(String... names) {
        return Arrays.stream(names).map(AlignMergeBranchesTests::attr).toList();
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, INTEGER);
    }
}
