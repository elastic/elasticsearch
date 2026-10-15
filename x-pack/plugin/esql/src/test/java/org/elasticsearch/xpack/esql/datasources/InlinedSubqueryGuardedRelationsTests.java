/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * Pins that {@link AbstractSubqueryJoin#inlineData} output is a guarded ancestor of the Hive
 * {@link ExternalRelation} — the missing composition between subquery inlining and L1 partition
 * pruning. {@code SubqueryJoinTests} already proves {@code Filter(In)} / {@code Filter(Not(In))}
 * over a dummy left; {@code SplitDiscoveryPhaseTests} already proves {@code guardedRelations} on a
 * hand-built {@code Filter}. This file is those two steps in sequence.
 *
 * <p>Does not run {@code FileSplitProvider}; the matcher already has {@code testInFilterPrunes}.
 */
public class InlinedSubqueryGuardedRelationsTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final int HASH_JOIN_THRESHOLD = PlannerSettings.IN_SUBQUERY_HASH_JOIN_THRESHOLD.getDefault(Settings.EMPTY);
    private static final BlockFactory BLOCK_FACTORY = TestBlockFactory.getNonBreakingInstance();

    /**
     * Small-list SEMI {@code inlineData} becomes {@code Filter(In)} over the same Hive relation, and
     * {@code guardedRelations} seeds that {@code In}.
     */
    public void testSemiFilterPathSeedsIn() {
        ExternalRelation relation = hiveRelation();
        Attribute year = relation.output().get(0);
        Attribute right = rightYear();
        SemiJoin semiJoin = semiJoin(relation, year, right);

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, rightPage(right, 2025), HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        Filter filter = as(inlined, Filter.class);
        assertSame("filter path must keep the Hive relation as the child", relation, filter.child());
        In in = as(filter.condition(), In.class);
        assertSame("In.value() must be the relation's year attr (same NameId)", year, in.value());

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(inlined);
        assertEquals(1, guarded.size());
        assertSame(relation, guarded.get(0).relation());
        assertTrue("the inlined In must seed L1", guarded.get(0).filters().contains(in));
    }

    /**
     * Small-list ANTI {@code inlineData} becomes {@code Filter(Not(In))} over the same Hive relation,
     * and {@code guardedRelations} seeds that {@code Not}.
     */
    public void testAntiFilterPathSeedsNotIn() {
        ExternalRelation relation = hiveRelation();
        Attribute year = relation.output().get(0);
        Attribute right = rightYear();
        AntiJoin antiJoin = antiJoin(relation, year, right);

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(antiJoin, rightPage(right, 2025), HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        Filter filter = as(inlined, Filter.class);
        assertSame("filter path must keep the Hive relation as the child", relation, filter.child());
        Not not = as(filter.condition(), Not.class);
        In in = as(not.field(), In.class);
        assertSame("In.value() must be the relation's year attr (same NameId)", year, in.value());

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(inlined);
        assertEquals(1, guarded.size());
        assertSame(relation, guarded.get(0).relation());
        assertTrue("the inlined Not(In) must seed L1", guarded.get(0).filters().contains(not));
    }

    /**
     * Empty SEMI replaces the left with an empty {@link LocalRelation}; the Hive relation is gone, so
     * {@code guardedRelations} is empty. Stronger than prune — there is nothing to scan.
     */
    public void testEmptySemiDropsExternalRelation() {
        ExternalRelation relation = hiveRelation();
        Attribute right = rightYear();
        SemiJoin semiJoin = semiJoin(relation, relation.output().get(0), right);

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, dummyRight(right), HASH_JOIN_THRESHOLD, BLOCK_FACTORY, null);

        as(inlined, LocalRelation.class);
        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(inlined);
        assertTrue("empty SEMI drops the Hive relation; there is nothing to seed", guarded.isEmpty());
    }

    /**
     * {@code hashJoinThreshold = 0} forces the LEFT hash-join path. {@code Join} is not
     * {@code rowPreserving}, so filters above it reset; the rewrite never emits {@code In}. A leftover
     * seed may exist ({@code IsNotNull($$year$sv)} below the join) but it does not bind to the Hive
     * year's {@code NameId}, so L1 cannot prune. Full scan is today's contract, not a goal.
     */
    public void testHashJoinPathSeedHasNoIn() {
        ExternalRelation relation = hiveRelation();
        Attribute year = relation.output().get(0);
        Attribute right = rightYear();
        SemiJoin semiJoin = semiJoin(relation, year, right);

        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(semiJoin, rightPage(right, 2025), 0, BLOCK_FACTORY, null);

        var project = as(inlined, Project.class);
        var join = as(as(project.child(), Filter.class).child(), Join.class);
        assertEquals(JoinTypes.LEFT, join.config().type());

        List<SplitDiscoveryPhase.GuardedRelation> guarded = SplitDiscoveryPhase.guardedRelations(inlined);
        assertEquals(1, guarded.size());
        assertSame(relation, guarded.get(0).relation());
        for (Expression f : guarded.get(0).filters()) {
            assertFalse(
                "leftover hash-join seed must not bind to Hive year (NameId); $$year$sv is synthetic",
                f.references().contains(year)
            );
            assertFalse(isInOrNotIn(f));
        }
    }

    private static boolean isInOrNotIn(Expression e) {
        if (e instanceof In) {
            return true;
        }
        return e instanceof Not not && not.field() instanceof In;
    }

    /** An {@code ExternalRelation} with {@code year}/{@code month} INTEGER attrs, matching {@code SplitDiscoveryPhaseTests}. */
    private static ExternalRelation hiveRelation() {
        List<Attribute> output = List.of(fieldAttr("year", DataType.INTEGER), fieldAttr("month", DataType.INTEGER));
        SimpleSourceMetadata metadata = new SimpleSourceMetadata(
            output,
            "parquet",
            "s3://bucket/data/*.parquet",
            null,
            null,
            Map.of(),
            Map.of()
        );
        return new ExternalRelation(SRC, "s3://bucket/data/*.parquet", metadata, output, FileList.UNRESOLVED, Map.of());
    }

    private static SemiJoin semiJoin(ExternalRelation left, Attribute year, Attribute right) {
        return new SemiJoin(SRC, left, dummyRight(right), List.of(year), List.of(right));
    }

    private static AntiJoin antiJoin(ExternalRelation left, Attribute year, Attribute right) {
        return new AntiJoin(SRC, left, dummyRight(right), List.of(year), List.of(right));
    }

    private static LocalRelation dummyRight(Attribute right) {
        return new LocalRelation(SRC, List.of(right), LocalSupplier.of(new Page(0)));
    }

    private static LocalRelation rightPage(Attribute right, int... years) {
        return new LocalRelation(SRC, List.of(right), LocalSupplier.of(new Page(intBlock(years))));
    }

    private static FieldAttribute rightYear() {
        return fieldAttr("year", DataType.INTEGER);
    }

    private static IntBlock intBlock(int... values) {
        try (IntBlock.Builder builder = BLOCK_FACTORY.newIntBlockBuilder(values.length)) {
            for (int v : values) {
                builder.appendInt(v);
            }
            return builder.build();
        }
    }

    private static FieldAttribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
