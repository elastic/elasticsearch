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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.DistinctByExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.planner.mapper.Mapper.JOIN_MARKER_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class HashJoinMapperTests extends ESTestCase {

    public void testMapsHashJoinToLeftHashJoinFilterOnLookupOrdinal() {
        HashJoin hashJoin = hashJoin(false);
        PhysicalPlan physical = new Mapper().map(new Versioned<>(hashJoin, TransportVersion.current()));

        ProjectExec project = as(physical, ProjectExec.class);
        assertThat(project.projections(), equalTo(hashJoin.output()));

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

    public void testMapsUniqueHashJoinGuardsOutputWithDistinctBy() {
        HashJoin hashJoin = hashJoin(true, JoinTypes.INNER, new long[] { 10, 10 }, new long[] { 100, 200 });
        PhysicalPlan physical = new Mapper().map(new Versioned<>(hashJoin, TransportVersion.current()));

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

    public void testLeftJoinSkipsMarkerFilter() {
        HashJoin hashJoin = hashJoin(false, JoinTypes.LEFT, new long[] { 10, 20 }, new long[] { 100, 200 });
        PhysicalPlan physical = new Mapper().map(new Versioned<>(hashJoin, TransportVersion.current()));

        ProjectExec project = as(physical, ProjectExec.class);
        // Left-outer: HashJoinExec directly under Project - no IsNotNull(marker) filter.
        HashJoinExec join = as(project.child(), HashJoinExec.class);
        assertThat(join.addedFields().stream().anyMatch(a -> a.name().startsWith(JOIN_MARKER_PREFIX)), equalTo(true));
    }

    public void testMapsRepeatedKeysToHashJoinExec() {
        HashJoin template = hashJoin(false, JoinTypes.LEFT, new long[] { 10, 10 }, new long[] { 100, 200 });
        assertThat(as(as(map(template), ProjectExec.class).child(), HashJoinExec.class).repeatedKeys(), equalTo(false));

        HashJoin hashJoin = withLeft(template, template.left(), template.leftFields());
        HashJoinExec join = as(as(map(hashJoin), ProjectExec.class).child(), HashJoinExec.class);
        assertThat(join.repeatedKeys(), equalTo(true));
        // Local data on the left needs no exchange.
        as(join.left(), LocalSourceExec.class);
    }

    /**
     * A unique join matches a probe row to at most one build row, which repeated build keys would break.
     */
    public void testUniqueWithRepeatedKeysNotSupported() {
        HashJoin template = hashJoin(true, JoinTypes.LEFT, new long[] { 10, 20 }, new long[] { 100, 200 });
        HashJoin hashJoin = withLeft(template, template.left(), template.leftFields());
        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> map(hashJoin));
        assertThat(e.getMessage(), equalTo("a unique hash join cannot allow repeated keys"));
    }

    /**
     * The join runs on the coordinator, so a left side that reads an index comes through an exchange, as for a coordinator
     * {@code LOOKUP JOIN}. PromQL never needs one: its left side is always an aggregation.
     */
    public void testHashJoinOverIndexExchangesTheLeftSide() {
        HashJoin template = hashJoin(false, JoinTypes.LEFT, new long[] { 10, 10 }, new long[] { 100, 200 });
        FieldAttribute indexKey = getFieldAttribute("k", DataType.LONG);
        EsRelation index = relation().withAttributes(List.of(indexKey));
        HashJoin hashJoin = withLeft(template, index, List.of(indexKey));

        HashJoinExec join = as(as(map(hashJoin), ProjectExec.class).child(), HashJoinExec.class);
        ExchangeExec exchange = as(join.left(), ExchangeExec.class);
        FragmentExec fragment = as(exchange.child(), FragmentExec.class);
        assertThat(fragment.fragment(), sameInstance(index));
        as(join.joinData(), LocalSourceExec.class);
    }

    private static PhysicalPlan map(HashJoin hashJoin) {
        return new Mapper().map(new Versioned<>(hashJoin, TransportVersion.current()));
    }

    /**
     * {@code template} with repeated keys and the given left side.
     */
    private static HashJoin withLeft(HashJoin template, LogicalPlan left, List<Attribute> leftFields) {
        return new HashJoin(
            Source.EMPTY,
            left,
            template.right(),
            leftFields,
            template.rightFields(),
            template.addedFields(),
            template.unique(),
            template.config().type(),
            true
        );
    }

    private static HashJoin hashJoin(boolean unique) {
        return hashJoin(unique, JoinTypes.INNER, new long[] { 10, 20 }, new long[] { 100, 200 });
    }

    private static HashJoin hashJoin(boolean unique, JoinType type, long[] buildKeys, long[] buildValues) {
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
        return new HashJoin(Source.EMPTY, probe, build, List.of(probeKey), List.of(buildKey), List.of(buildValue), unique, type);
    }
}
