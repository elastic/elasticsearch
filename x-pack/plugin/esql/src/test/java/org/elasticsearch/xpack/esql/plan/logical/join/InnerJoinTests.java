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
 * Unit tests for {@link InnerJoin}: it is an INNER {@link Join} subtype, and it participates in the coordinator phase loop via its own
 * {@link InnerJoin#firstSubPlan} / {@link InnerJoin#newMainPlan} hooks. Unlike the SEMI family ({@link AbstractSubqueryJoin}), its
 * {@link InnerJoin#newMainPlan} path simply replaces the right child with the materialized {@link LocalRelation} so the mapper can
 * lower the node to LEFT {@code HashJoinExec} + lookup-ordinal filter - it does not rewrite into an {@code IN} list or hash join.
 */
public class InnerJoinTests extends ESTestCase {

    public void testIsInnerJoinWithInnerJoinShape() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        InnerJoin innerJoin = innerJoin(leftKey, rightKey, value, true);

        assertThat(innerJoin, instanceOf(Join.class));
        assertThat(innerJoin.config().type(), sameInstance(JoinTypes.INNER));
        assertThat(innerJoin.leftFields(), equalTo(List.of(leftKey)));
        assertThat(innerJoin.rightFields(), equalTo(List.of(rightKey)));
        assertThat(innerJoin.addedFields(), equalTo(List.of(value)));
        assertThat(innerJoin.unique(), is(true));
        // INNER equi-join output: the copied build column(s) plus the (probe) join key, as the physical HashJoinExec produces.
        assertThat(innerJoin.output(), equalTo(List.of(value, leftKey)));
    }

    public void testNotSerialized() {
        InnerJoin innerJoin = innerJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, false);
        expectThrows(UnsupportedOperationException.class, () -> innerJoin.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, innerJoin::getWriteableName);
    }

    public void testFirstSubPlanFindsInnerJoinRight() {
        InnerJoin innerJoin = innerJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, true);

        InnerJoin.LogicalPlanTuple tuple = InnerJoin.firstSubPlan(innerJoin, new HashSet<>());
        assertThat(tuple, notNullValue());
        // The right subquery is the very instance held on the join, doubling as the identity key for newMainPlan.
        assertThat(tuple.subPlan(), sameInstance(innerJoin.right()));
        assertThat(tuple.originalSubPlan(), sameInstance(innerJoin.right()));
    }

    public void testFirstSubPlanSkipsAlreadyMaterializedRight() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        LocalRelation materializedRight = localRelation(List.of(rightKey));
        InnerJoin innerJoin = new InnerJoin(
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
        assertThat(InnerJoin.firstSubPlan(innerJoin, processed), nullValue());
    }

    public void testNewMainPlanReplacesRightWithLocalRelation() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        InnerJoin innerJoin = innerJoin(leftKey, rightKey, value, true);

        InnerJoin.LogicalPlanTuple tuple = InnerJoin.firstSubPlan(innerJoin, new HashSet<>());
        assertThat(tuple, notNullValue());

        LocalRelation materialized = localRelation(List.of(rightKey, value));
        LogicalPlan newMain = InnerJoin.newMainPlan(innerJoin, tuple, materialized);

        // Still an InnerJoin (not rewritten into a Filter/HashJoin like the SEMI family), now with the materialized build side on its
        // right.
        InnerJoin rebuilt = as(newMain, InnerJoin.class);
        assertThat(rebuilt.right(), sameInstance(materialized));
        assertThat(rebuilt.left(), sameInstance(innerJoin.left()));
        assertThat(rebuilt.leftFields(), equalTo(innerJoin.leftFields()));
        assertThat(rebuilt.rightFields(), equalTo(innerJoin.rightFields()));
        assertThat(rebuilt.addedFields(), equalTo(innerJoin.addedFields()));
        assertThat(rebuilt.unique(), is(innerJoin.unique()));
    }

    private static InnerJoin innerJoin(FieldAttribute leftKey, FieldAttribute rightKey, FieldAttribute addedValue, boolean unique) {
        List<Attribute> rightOutput = addedValue == null ? List.of(rightKey) : List.of(rightKey, addedValue);
        List<Attribute> added = addedValue == null ? List.of() : List.of(addedValue);
        return new InnerJoin(
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
