/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * What {@link Approximation}'s constructor does to a materialized relation in the plan it is given.
 * <p>
 * Asserted here and not only of {@link LocalRelation#withRepeatedReads()} itself, because the method being right
 * says nothing about this caller still calling it: the line is one {@code transformUp} in a constructor, and with
 * it deleted every approximation assertion in this package stays green while an approximated plan carrying rows
 * reads a released page on its second execution. Both arms, since the method answers differently per relation and
 * a test of one arm leaves the other free to regress.
 */
public class ApproximationRepeatedReadsTests extends ESTestCase {

    private static final List<Attribute> OUTPUT = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));

    /** An approximated plan is executed once per subplan, so a relation holding rows has to survive being read again. */
    public void testARelationCarryingRowsIsMadeToCopyPerRead() {
        LocalRelation leaf = leafOfTheApproximatedPlan(relation(onePage()));

        assertThat(leaf.supplier(), instanceOf(CopyingLocalSupplier.class));
    }

    /**
     * And an empty one is left as it is. A copy of nothing still reads as nothing, so no result would differ --
     * what is lost is {@code hasEmptySupplier}, which three optimizer rules prune on, and a plan that loses it
     * runs correctly and does more work than it needs to.
     */
    public void testAnEmptyRelationIsLeftRecognisablyEmpty() {
        LocalRelation empty = relation(EmptyLocalSupplier.EMPTY);

        LocalRelation leaf = leafOfTheApproximatedPlan(empty);

        assertSame(empty, leaf);
        assertTrue("an approximated empty relation is no longer recognisable as empty", leaf.hasEmptySupplier());
    }

    /**
     * The plan {@link Approximation} holds is private, and its first subplan is a count over the plan's leftmost
     * leaf -- so the leaf of that subplan is this relation after the constructor is done with it.
     */
    private static LocalRelation leafOfTheApproximatedPlan(LocalRelation relation) {
        Approximation approximation = new Approximation(
            relation,
            new ApproximationVerifier.QueryProperties(false, true, null),
            new ApproximationSettings(100, null)
        );
        LogicalPlan subPlan = approximation.firstSubPlan();
        assertThat(subPlan, instanceOf(Aggregate.class));
        LogicalPlan leaf = ((Aggregate) subPlan).child();
        assertThat(leaf, instanceOf(LocalRelation.class));
        return (LocalRelation) leaf;
    }

    private static LocalRelation relation(LocalSupplier supplier) {
        return new LocalRelation(Source.EMPTY, OUTPUT, supplier);
    }

    private static LocalSupplier onePage() {
        return LocalSupplier.of(new Page(BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 1)));
    }
}
