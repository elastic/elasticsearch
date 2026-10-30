/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.generator.ShuffleQueryGenerator;
import org.elasticsearch.xpack.esql.optimizer.PlanShape;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

/** Checks distribution properties from the resulting operators rather than invoking the optimizer's requirement walker. */
public class EnsureDistributionInvariantTests extends ESTestCase {
    public void testGeneratedPlanDistributions() {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        assumeTrue("subquery in FROM required", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        ShuffleQueryGenerator generator = new ShuffleQueryGenerator(random(), List.of("test"));
        for (int joins : new int[] { 1, 2, 5, 10, 20 }) {
            String query = generator.query(joins, joins == 20 ? 4 : randomIntBetween(0, 4));
            for (String strategy : List.of("partitioned", "broadcast")) {
                for (boolean shuffleAgg : List.of(false, true)) {
                    QueryPragmas pragmas = new QueryPragmas(
                        Settings.builder()
                            .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), randomIntBetween(1, 3))
                            .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
                            .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), strategy)
                            .put(QueryPragmas.SHUFFLE_AGG.getKey(), shuffleAgg)
                            .build()
                    );
                    var configuration = configuration(pragmas, query);
                    var analyzer = analyzer().minimumTransportVersion(TransportVersion.current())
                        .configuration(configuration)
                        .addIndex("test", "mapping-basic.json")
                        .addNoFieldsIndex()
                        .buildAnalyzer();
                    String context = "pragmas=" + pragmas.getSettings() + "\n" + query;
                    try {
                        PhysicalPlan plan = new TestPlannerOptimizer(configuration, analyzer).distributedPlan(query);
                        assertTrue(context, StagedPlanner.hasShuffle(plan));
                        assertTrue(context + "\n" + PlanShape.of(plan), check(plan, false).single());
                        StagedPlanner.StagedPlan staged = StagedPlanner.split(plan);
                        assertEquals(context, StagedPlanner.StagePlacement.COORDINATOR, staged.root().placement());
                        for (StagedPlanner.Stage stage : staged.stages()) {
                            assertFalse(context, StagedPlanner.hasShuffle(stage.plan()));
                            assertFalse(context, stage.plan().anyMatch(MergeExec.class::isInstance));
                        }
                    } catch (Exception | AssertionError e) {
                        throw new AssertionError(context, e);
                    }
                }
            }
        }
    }

    private record Distribution(boolean single, List<NameId> keys) {
        private static final Distribution UNKNOWN = new Distribution(false, List.of());
        private static final Distribution SINGLE = new Distribution(true, List.of());
    }

    private static Distribution check(PhysicalPlan plan, boolean unionBranch) {
        assertFalse("legacy exchange remains:\n" + PlanShape.of(plan), plan instanceof ExchangeExec);
        List<Distribution> children = plan.children().stream().map(child -> check(child, plan instanceof MergeExec)).toList();
        if (plan instanceof ShuffleExchangeExec exchange) {
            Distribution child = children.getFirst();
            return switch (exchange.distribution()) {
                case GATHER -> {
                    assertFalse("redundant gather:\n" + PlanShape.of(plan), unionBranch == false && child.single());
                    yield Distribution.SINGLE;
                }
                case HASH -> {
                    List<NameId> keys = ids(exchange.keys());
                    assertFalse("redundant hash exchange:\n" + PlanShape.of(plan), unionBranch == false && child.keys().equals(keys));
                    yield new Distribution(false, keys);
                }
                case BROADCAST -> Distribution.UNKNOWN;
            };
        }
        if (plan instanceof LocalSourceExec) {
            return Distribution.SINGLE;
        }
        if (plan instanceof MergeExec merge) {
            Distribution first = byPosition(children.getFirst(), merge.children().getFirst().output(), merge.output());
            for (int i = 1; i < children.size(); i++) {
                assertEquals(first, byPosition(children.get(i), merge.children().get(i).output(), merge.output()));
            }
            return first;
        }
        if (plan instanceof DistributedHashJoinExec join) {
            Distribution probe = children.getFirst();
            if (join.mode() == DistributedHashJoinExec.Mode.PARTITIONED) {
                assertEquals("probe partitioning:\n" + PlanShape.of(plan), ids(join.leftFields()), probe.keys());
                assertEquals("build partitioning:\n" + PlanShape.of(plan), ids(join.rightFields()), children.get(1).keys());
            }
            return retain(probe, plan.output());
        }
        if (children.size() == 1) {
            Distribution child = children.getFirst();
            if (child.single()) {
                return child;
            }
            if (plan instanceof AggregateExec aggregate) {
                if (aggregate.getMode() == AggregatorMode.INITIAL || aggregate.getMode() == AggregatorMode.INTERMEDIATE) {
                    // Partial grouping can expand multivalued keys into groups owned by other lanes.
                    return Distribution.UNKNOWN;
                }
                if (aggregate.child() instanceof ShuffleExchangeExec exchange && exchange.inBetweenAggs()) {
                    List<Attribute> groups = aggregate.groupings().stream().map(Expressions::attribute).toList();
                    return retain(byPosition(child, exchange.output().subList(0, groups.size()), groups), plan.output());
                }
            }
            if (plan instanceof ProjectExec project && child.keys().isEmpty() == false) {
                List<NameId> keys = new ArrayList<>();
                for (NameId key : child.keys()) {
                    for (var projection : project.projections()) {
                        Attribute input = projection instanceof Alias alias && alias.child() instanceof Attribute attribute
                            ? attribute
                            : projection.toAttribute();
                        if (input.id().equals(key)) {
                            keys.add(projection.id());
                            break;
                        }
                    }
                }
                return keys.size() == child.keys().size() ? new Distribution(false, keys) : Distribution.UNKNOWN;
            }
            return retain(child, plan.output());
        }
        return Distribution.UNKNOWN;
    }

    private static Distribution byPosition(Distribution distribution, List<Attribute> input, List<Attribute> output) {
        List<NameId> inputs = ids(input);
        List<NameId> keys = new ArrayList<>();
        for (NameId key : distribution.keys()) {
            int position = inputs.indexOf(key);
            assertTrue("partition key missing from input", position >= 0);
            keys.add(output.get(position).id());
        }
        return new Distribution(distribution.single(), keys);
    }

    private static Distribution retain(Distribution distribution, List<Attribute> output) {
        return ids(output).containsAll(distribution.keys()) ? distribution : Distribution.UNKNOWN;
    }

    private static List<NameId> ids(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::id).toList();
    }
}
