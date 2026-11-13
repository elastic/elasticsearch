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
 * Unit tests for {@link HashJoin}: coordinator hash join ({@link Join} subtype), and it participates in the coordinator phase loop
 * via its own {@link HashJoin#firstSubPlan} / {@link HashJoin#newMainPlan} hooks. Unlike the SEMI family ({@link AbstractSubqueryJoin}),
 * its {@link HashJoin#newMainPlan} path simply replaces the right child with the materialized {@link LocalRelation} so the mapper can
 * lower the node to {@code HashJoinExec} (+ lookup-ordinal filter for INNER) - it does not rewrite into an {@code IN} list.
 */
public class HashJoinTests extends ESTestCase {

    public void testIsHashJoinWithHashJoinShape() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        HashJoin hashJoin = hashJoin(leftKey, rightKey, value, true);

        assertThat(hashJoin, instanceOf(Join.class));
        assertThat(hashJoin.config().type(), sameInstance(JoinTypes.INNER));
        assertThat(hashJoin.leftFields(), equalTo(List.of(leftKey)));
        assertThat(hashJoin.rightFields(), equalTo(List.of(rightKey)));
        assertThat(hashJoin.addedFields(), equalTo(List.of(value)));
        assertThat(hashJoin.unique(), is(true));
        // INNER equi-join output: the copied build column(s) plus the (probe) join key, as the physical HashJoinExec produces.
        assertThat(hashJoin.output(), equalTo(List.of(value, leftKey)));
    }

    public void testLeftJoinType() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        HashJoin hashJoin = new HashJoin(
            Source.EMPTY,
            localRelation(List.of(leftKey)),
            localRelation(List.of(rightKey, value)),
            List.of(leftKey),
            List.of(rightKey),
            List.of(value),
            false,
            JoinTypes.LEFT
        );
        assertThat(hashJoin.config().type(), sameInstance(JoinTypes.LEFT));
    }

    public void testNotSerialized() {
        HashJoin hashJoin = hashJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, false);
        expectThrows(UnsupportedOperationException.class, () -> hashJoin.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, hashJoin::getWriteableName);
    }

    public void testFirstSubPlanFindsHashJoinRight() {
        HashJoin hashJoin = hashJoin(getFieldAttribute("k", DataType.LONG), getFieldAttribute("k", DataType.LONG), null, true);

        HashJoin.LogicalPlanTuple tuple = HashJoin.firstSubPlan(hashJoin, new HashSet<>());
        assertThat(tuple, notNullValue());
        // The right subquery is the very instance held on the join, doubling as the identity key for newMainPlan.
        assertThat(tuple.subPlan(), sameInstance(hashJoin.right()));
        assertThat(tuple.originalSubPlan(), sameInstance(hashJoin.right()));
    }

    public void testFirstSubPlanSkipsAlreadyMaterializedRight() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        LocalRelation materializedRight = localRelation(List.of(rightKey));
        HashJoin hashJoin = new HashJoin(
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
        assertThat(HashJoin.firstSubPlan(hashJoin, processed), nullValue());
    }

    public void testNewMainPlanReplacesRightWithLocalRelation() {
        FieldAttribute leftKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute rightKey = getFieldAttribute("k", DataType.LONG);
        FieldAttribute value = getFieldAttribute("v", DataType.LONG);
        HashJoin hashJoin = hashJoin(leftKey, rightKey, value, true);

        HashJoin.LogicalPlanTuple tuple = HashJoin.firstSubPlan(hashJoin, new HashSet<>());
        assertThat(tuple, notNullValue());

        LocalRelation materialized = localRelation(List.of(rightKey, value));
        LogicalPlan newMain = HashJoin.newMainPlan(hashJoin, tuple, materialized);

        // Still a HashJoin (not rewritten into a Filter/HashJoin like the SEMI family), now with the materialized build side on its
        // right.
        HashJoin rebuilt = as(newMain, HashJoin.class);
        assertThat(rebuilt.right(), sameInstance(materialized));
        assertThat(rebuilt.left(), sameInstance(hashJoin.left()));
        assertThat(rebuilt.leftFields(), equalTo(hashJoin.leftFields()));
        assertThat(rebuilt.rightFields(), equalTo(hashJoin.rightFields()));
        assertThat(rebuilt.addedFields(), equalTo(hashJoin.addedFields()));
        assertThat(rebuilt.unique(), is(hashJoin.unique()));
    }

    private static HashJoin hashJoin(FieldAttribute leftKey, FieldAttribute rightKey, FieldAttribute addedValue, boolean unique) {
        List<Attribute> rightOutput = addedValue == null ? List.of(rightKey) : List.of(rightKey, addedValue);
        List<Attribute> added = addedValue == null ? List.of() : List.of(addedValue);
        return new HashJoin(
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
