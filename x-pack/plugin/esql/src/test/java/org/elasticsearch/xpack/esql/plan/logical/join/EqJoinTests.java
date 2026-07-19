/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link EqJoin}: it is an INNER {@link Join} subtype, and it participates in the coordinator phase loop via the shared
 * {@link AbstractHashJoin#firstSubPlan} machinery. Unlike the SEMI family ({@link AbstractSubqueryJoin}), its
 * {@link EqJoin#newMainPlan} path simply replaces the right child with the materialized {@link LocalRelation} so the mapper can
 * lower the node to an {@code EqJoinExec} - it does not rewrite into an {@code IN} list or hash join.
 */
public class EqJoinTests extends ESTestCase {

    public void testIsInnerJoinWithEqJoinShape() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        EqJoin eqJoin = eqJoin(leftKey, rightKey, value, true);

        assertThat(eqJoin, instanceOf(Join.class));
        assertThat(eqJoin.config().type(), sameInstance(JoinTypes.INNER));
        assertThat(eqJoin.leftFields(), equalTo(List.of(leftKey)));
        assertThat(eqJoin.rightFields(), equalTo(List.of(rightKey)));
        assertThat(eqJoin.addedFields(), equalTo(List.of(value)));
        assertThat(eqJoin.unique(), is(true));
        // INNER equi-join output: the copied build column(s) plus the (probe) join key, as the physical EqJoinExec produces.
        assertThat(eqJoin.output(), equalTo(List.of(value, leftKey)));
    }

    public void testNotSerialized() {
        EqJoin eqJoin = eqJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, false);
        expectThrows(UnsupportedOperationException.class, () -> eqJoin.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, eqJoin::getWriteableName);
    }

    public void testFirstSubPlanFindsEqJoinRight() {
        EqJoin eqJoin = eqJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, true);

        AbstractHashJoin.LogicalPlanTuple tuple = AbstractHashJoin.firstSubPlan(eqJoin, new HashSet<>());
        assertThat(tuple, notNullValue());
        // The right subquery is the very instance held on the join, doubling as the identity key for newMainPlan.
        assertThat(tuple.subPlan(), sameInstance(eqJoin.right()));
        assertThat(tuple.originalSubPlan(), sameInstance(eqJoin.right()));
    }

    public void testFirstSubPlanSkipsAlreadyMaterializedRight() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        LocalRelation materializedRight = localRelation(List.of(rightKey));
        EqJoin eqJoin = new EqJoin(
            Source.EMPTY,
            localRelation(List.of(leftKey)),
            materializedRight,
            List.of(leftKey),
            List.of(rightKey),
            List.of(),
            false
        );

        Set<LocalRelation> processed = new HashSet<>();
        processed.add(materializedRight);
        assertThat(AbstractHashJoin.firstSubPlan(eqJoin, processed), nullValue());
    }

    public void testNewMainPlanReplacesRightWithLocalRelation() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        EqJoin eqJoin = eqJoin(leftKey, rightKey, value, true);

        AbstractHashJoin.LogicalPlanTuple tuple = AbstractHashJoin.firstSubPlan(eqJoin, new HashSet<>());
        assertThat(tuple, notNullValue());

        LocalRelation materialized = localRelation(List.of(rightKey, value));
        LogicalPlan newMain = EqJoin.newMainPlan(eqJoin, tuple, materialized);

        // Still an EqJoin (not rewritten into a Filter/HashJoin like the SEMI family), now with the materialized build side on its right.
        EqJoin rebuilt = as(newMain, EqJoin.class);
        assertThat(rebuilt.right(), sameInstance(materialized));
        assertThat(rebuilt.left(), sameInstance(eqJoin.left()));
        assertThat(rebuilt.leftFields(), equalTo(eqJoin.leftFields()));
        assertThat(rebuilt.rightFields(), equalTo(eqJoin.rightFields()));
        assertThat(rebuilt.addedFields(), equalTo(eqJoin.addedFields()));
        assertThat(rebuilt.unique(), is(eqJoin.unique()));
    }

    private static EqJoin eqJoin(FieldAttribute leftKey, FieldAttribute rightKey, FieldAttribute addedValue, boolean unique) {
        List<Attribute> rightOutput = addedValue == null ? List.of(rightKey) : List.of(rightKey, addedValue);
        List<Attribute> added = addedValue == null ? List.of() : List.of(addedValue);
        return new EqJoin(
            Source.EMPTY,
            localRelation(List.of(leftKey)),
            localRelation(rightOutput),
            List.of(leftKey),
            List.of(rightKey),
            added,
            unique
        );
    }

    private static LocalRelation localRelation(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }
}
