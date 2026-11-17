/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.hamcrest.Matchers.instanceOf;

/**
 * What {@link InlineJoin#firstSubPlan} does to a materialized right-hand side.
 * <p>
 * Pinned at this caller rather than only at {@link LocalRelation#withRepeatedReads()}, because the method being
 * right is not the same as this caller still calling it: delete the one {@code transformUp} here and the subplan
 * is handed out holding the original page, which the second read of it finds released. Both arms, since the method
 * answers differently per relation and a test of one leaves the other free to regress.
 */
public class InlineJoinRepeatedReadsTests extends ESTestCase {

    private static final List<Attribute> OUTPUT = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.INTEGER));

    /** The subplan's rows are read once per branch that ends up holding them, so they have to survive a second read. */
    public void testAMaterializedRightSideIsMadeToCopyPerRead() {
        LocalRelation right = withSupplier(onePage());

        LocalRelation subPlan = subPlanOf(right);

        assertThat(subPlan.supplier(), instanceOf(CopyingLocalSupplier.class));
    }

    /**
     * And an empty right side comes back as it was. Copying nothing reads as nothing, so no result would differ --
     * what is lost is {@code hasEmptySupplier}, which the rules that prune an empty branch test, so the query
     * answers correctly and prunes nothing.
     */
    public void testAnEmptyRightSideIsLeftRecognisablyEmpty() {
        LocalRelation empty = withSupplier(EmptyLocalSupplier.EMPTY);

        LocalRelation subPlan = subPlanOf(empty);

        assertSame(empty, subPlan);
        assertTrue("an inline join's empty right side is no longer recognisable as empty", subPlan.hasEmptySupplier());
    }

    /**
     * An inline join whose right side is already materialized, which is the arm that reaches the transform: the
     * stub-replacing arm above it needs a {@code StubRelation}, and there is none here.
     */
    private static LocalRelation subPlanOf(LocalRelation right) {
        InlineJoin join = new InlineJoin(Source.EMPTY, relation(), right, new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), null));
        InlineJoin.LogicalPlanTuple tuple = InlineJoin.firstSubPlan(join, Set.of());
        assertNotNull("no subplan was taken, so this test asserts nothing about the transform", tuple);
        LogicalPlan subPlan = tuple.stubReplacedSubPlan();
        assertThat(subPlan, instanceOf(LocalRelation.class));
        return (LocalRelation) subPlan;
    }

    private static LocalRelation withSupplier(LocalSupplier supplier) {
        return new LocalRelation(Source.EMPTY, OUTPUT, supplier);
    }

    private static LocalSupplier onePage() {
        return LocalSupplier.of(new Page(BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 1)));
    }
}
