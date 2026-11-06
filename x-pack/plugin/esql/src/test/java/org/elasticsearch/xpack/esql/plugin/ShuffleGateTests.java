/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.Map;

public class ShuffleGateTests extends ESTestCase {

    private static final Configuration ENABLED = EsqlTestUtils.configuration(
        new QueryPragmas(Settings.builder().put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2).build())
    );

    private static final Configuration DISABLED = EsqlTestUtils.configuration(QueryPragmas.EMPTY);

    public void testLogicalGateRejectsUnsupportedShapes() {
        LogicalPlan relation = EsqlTestUtils.relation();
        LogicalPlan external = externalRelation();
        LogicalPlan timeSeries = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            List.of(),
            List.of(),
            null,
            null,
            TimeSeriesAggregate.Origin.TS_COMMAND
        );
        LogicalPlan fork = new Fork(Source.EMPTY, List.of(relation, relation), List.of());
        LogicalPlan union = new UnionAll(Source.EMPTY, List.of(relation, relation), List.of());
        LogicalPlan crossCluster = new EsRelation(
            Source.EMPTY,
            "remote:events",
            IndexMode.STANDARD,
            Map.of("remote", List.of("events")),
            Map.of("remote", List.of("events")),
            Map.of(),
            List.of()
        );

        assertTrue(ShuffleGate.allowed(ENABLED, relation));
        assertFalse(ShuffleGate.allowed(ENABLED, fork));
        assertTrue(ShuffleGate.allowed(ENABLED, union));
        assertTrue(ShuffleGate.allowed(ENABLED, external));
        assertTrue(ShuffleGate.allowed(ENABLED, new UnionAll(Source.EMPTY, List.of(externalRelation(), externalRelation()), List.of())));
        assertFalse(ShuffleGate.allowed(ENABLED, timeSeries));
        assertFalse(ShuffleGate.allowed(ENABLED, crossCluster));
    }

    public void testLogicalGateAllowsJoinsBetweenExternalSources() {
        LogicalPlan join = new Join(
            Source.EMPTY,
            externalRelation(),
            externalRelation(),
            JoinTypes.LEFT,
            List.of(),
            List.of(),
            null,
            ExecuteLocation.ANY
        );

        assertTrue(ShuffleGate.allowed(ENABLED, join));
        assertTrue(ShuffleGate.allowed(ENABLED, new FragmentExec(join)));
    }

    public void testPhysicalGateRejectsUnsupportedShapes() {
        LogicalPlan relation = EsqlTestUtils.relation();
        PhysicalPlan fragment = new FragmentExec(relation);
        PhysicalPlan fork = new MergeExec(Source.EMPTY, List.of(), List.of(), MergeExec.Kind.FORK);
        PhysicalPlan union = new MergeExec(
            Source.EMPTY,
            List.of(fragment, new FragmentExec(relation)),
            fragment.output(),
            MergeExec.Kind.UNION
        );
        PhysicalPlan externalSource = new ExternalSourceExec(
            Source.EMPTY,
            "file:///events.csv",
            "csv",
            List.of(),
            Map.of(),
            Map.of(),
            null
        );
        PhysicalPlan unsupportedFragment = new FragmentExec(
            new TimeSeriesAggregate(Source.EMPTY, relation, List.of(), List.of(), null, null, TimeSeriesAggregate.Origin.TS_COMMAND)
        );

        assertTrue(ShuffleGate.allowed(ENABLED, fragment));
        assertFalse(ShuffleGate.allowed(ENABLED, fork));
        assertTrue(ShuffleGate.allowed(ENABLED, union));
        assertTrue(ShuffleGate.allowed(ENABLED, externalSource));
        assertTrue(ShuffleGate.allowed(ENABLED, new FragmentExec(externalRelation())));
        assertFalse(ShuffleGate.allowed(ENABLED, unsupportedFragment));
    }

    public void testDisabledPragmaRejectsOtherwiseSupportedPlans() {
        LogicalPlan logical = EsqlTestUtils.relation();
        PhysicalPlan physical = new FragmentExec(logical);

        assertFalse(ShuffleGate.allowed(DISABLED, logical));
        assertFalse(ShuffleGate.allowed(DISABLED, physical));
    }

    private static ExternalRelation externalRelation() {
        String path = "file:///events.csv";
        return new ExternalRelation(
            Source.EMPTY,
            path,
            new SimpleSourceMetadata(List.of(), "csv", path),
            List.of(),
            FileList.UNRESOLVED,
            Map.of()
        );
    }
}
