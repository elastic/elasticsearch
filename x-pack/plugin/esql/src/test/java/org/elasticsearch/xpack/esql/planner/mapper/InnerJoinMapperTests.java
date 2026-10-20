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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MaterializedSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.DistinctByExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.planner.mapper.Mapper.JOIN_MARKER_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class InnerJoinMapperTests extends ESTestCase {

    public void testMapsInnerJoinToLeftHashJoinFilterOnLookupOrdinal() {
        InnerJoin innerJoin = innerJoin(false);
        PhysicalPlan physical = new Mapper().map(new Versioned<>(innerJoin, TransportVersion.current()));

        ProjectExec project = as(physical, ProjectExec.class);
        assertThat(project.projections(), equalTo(innerJoin.output()));

        FilterExec filter = as(project.child(), FilterExec.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        Attribute ordinal = as(isNotNull.field(), Attribute.class);
        assertThat(ordinal.name(), startsWith(JOIN_MARKER_PREFIX));

        HashJoinExec join = as(filter.child(), HashJoinExec.class);
        assertThat(join.addedFields(), hasItem(sameInstance(ordinal)));
        // The ordinal is emitted by the join itself for the downstream filter...
        assertThat(join.output(), hasItem(sameInstance(ordinal)));
        // ...but is not a build column: the planner binds it to the RowInTableLookup positions channel.
        LocalSourceExec build = as(join.joinData(), LocalSourceExec.class);
        assertThat(build.outputSet().contains(ordinal), equalTo(false));
    }

    public void testMapsUniqueInnerJoinGuardsOutputWithDistinctBy() {
        InnerJoin innerJoin = innerJoin(true, new long[] { 10, 10 }, new long[] { 100, 200 });
        PhysicalPlan physical = new Mapper().map(new Versioned<>(innerJoin, TransportVersion.current()));

        ProjectExec project = as(physical, ProjectExec.class);
        DistinctByExec distinctBy = as(project.child(), DistinctByExec.class);
        FilterExec filter = as(distinctBy.child(), FilterExec.class);
        Attribute ordinal = as(as(filter.condition(), IsNotNull.class).field(), Attribute.class);
        assertThat(ordinal.name(), startsWith(JOIN_MARKER_PREFIX));

        // Guard keys on the lookup ordinal alone (unique per build row), not on the join key columns.
        assertThat(distinctBy.key(), sameInstance(ordinal));
        assertThat(distinctBy.failOnDuplicate(), equalTo(true));

        HashJoinExec join = as(filter.child(), HashJoinExec.class);
        assertThat(join.addedFields(), hasItem(sameInstance(ordinal)));
    }

    public void testOnlyMaterializedSubqueryJoinEnablesRepeatedKeyMode() {
        var blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = new ReferenceAttribute(Source.EMPTY, "k", DataType.LONG);
        ReferenceAttribute rightKey = new ReferenceAttribute(Source.EMPTY, "k", DataType.LONG);
        ReferenceAttribute value = new ReferenceAttribute(Source.EMPTY, "v", DataType.LONG);
        LocalRelation left = new LocalRelation(
            Source.EMPTY,
            List.of(leftKey),
            LocalSupplier.of(new Page(blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock()))
        );
        LocalRelation right = new LocalRelation(
            Source.EMPTY,
            List.of(rightKey, value),
            LocalSupplier.of(
                new Page(
                    blockFactory.newLongArrayVector(new long[] { 1, 1 }, 2).asBlock(),
                    blockFactory.newLongArrayVector(new long[] { 10, 11 }, 2).asBlock()
                )
            )
        );
        MaterializedSubqueryJoin materialized = new MaterializedSubqueryJoin(
            Source.EMPTY,
            left,
            right,
            JoinTypes.LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );

        HashJoinExec mapped = as(new Mapper().map(new Versioned<>(materialized, TransportVersion.current())), HashJoinExec.class);
        HashJoinExec localMapped = as(LocalMapper.INSTANCE.map(materialized), HashJoinExec.class);
        assertTrue(mapped.repeatedKeys());
        assertTrue(localMapped.repeatedKeys());

        Join ordinary = new Join(Source.EMPTY, left, right, JoinTypes.LEFT, List.of(leftKey), List.of(rightKey), null, ExecuteLocation.ANY);
        assertFalse(as(new Mapper().map(new Versioned<>(ordinary, TransportVersion.current())), HashJoinExec.class).repeatedKeys());
        assertFalse(as(LocalMapper.INSTANCE.map(ordinary), HashJoinExec.class).repeatedKeys());
    }

    private static InnerJoin innerJoin(boolean unique) {
        return innerJoin(unique, new long[] { 10, 20 }, new long[] { 100, 200 });
    }

    private static InnerJoin innerJoin(boolean unique, long[] buildKeys, long[] buildValues) {
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
                    blockFactory.newLongArrayVector(buildKeys, buildKeys.length).asBlock(),
                    blockFactory.newLongArrayVector(buildValues, buildValues.length).asBlock()
                )
            )
        );
        return new InnerJoin(Source.EMPTY, probe, build, List.of(probeKey), List.of(buildKey), List.of(buildValue), unique);
    }
}
