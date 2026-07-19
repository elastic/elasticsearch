/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.EqJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EqJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies the {@link Mapper} lowers a logical {@link EqJoin} (over a materialized build side) to a
 * physical {@link EqJoinExec}, carrying the join fields and the {@code unique} flag through unchanged.
 */
public class EqJoinMapperTests extends ESTestCase {

    public void testMapsEqJoinToEqJoinExec() {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();

        ReferenceAttribute probeKey = new ReferenceAttribute(Source.EMPTY, "k", DataType.LONG);
        LocalRelation probe = new LocalRelation(
            Source.EMPTY,
            List.of(probeKey),
            LocalSupplier.of(new Page(blockFactory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock()))
        );

        ReferenceAttribute buildKey = new ReferenceAttribute(Source.EMPTY, "k", DataType.LONG);
        ReferenceAttribute buildValue = new ReferenceAttribute(Source.EMPTY, "bval", DataType.LONG);
        LocalRelation build = new LocalRelation(
            Source.EMPTY,
            List.of(buildKey, buildValue),
            LocalSupplier.of(
                new Page(
                    blockFactory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock(),
                    blockFactory.newLongArrayVector(new long[] { 100, 200 }, 2).asBlock()
                )
            )
        );

        EqJoin eqJoin = new EqJoin(Source.EMPTY, probe, build, List.of(probeKey), List.of(buildKey), List.of(buildValue), true);

        PhysicalPlan physical = new Mapper().map(new Versioned<>(eqJoin, TransportVersion.current()));

        EqJoinExec exec = as(physical, EqJoinExec.class);
        assertThat(exec.leftFields(), equalTo(List.of(probeKey)));
        assertThat(exec.rightFields(), equalTo(List.of(buildKey)));
        assertThat(exec.addedFields().size(), equalTo(1));
        assertThat(exec.addedFields().contains(buildValue), equalTo(true));
        assertThat(exec.unique(), equalTo(true));
        assertThat(exec.joinData(), instanceOf(LocalSourceExec.class));
    }
}
