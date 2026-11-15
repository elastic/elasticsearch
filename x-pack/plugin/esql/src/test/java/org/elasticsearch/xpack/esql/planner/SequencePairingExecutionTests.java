/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.execution.sequence.SequenceSourceOperator;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;

/** Physical planning selects the guarded source and hands off its existing native analysis registry. */
public class SequencePairingExecutionTests extends ESTestCase {
    /** The real registry is borrowed from planning; evaluator creation remains per source rather than per plan. */
    public void testLatestValidSelectsPreparedFactoryAndReceivesThePlanningRegistry() throws Exception {
        try (
            var registry = new AnalysisRegistry(
                TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build()),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of()
            )
        ) {
            for (Integer bound : new Integer[] { null, 1, 3, 1024 }) {
                AtomicInteger factories = new AtomicInteger();
                var selected = SequenceSourceOperator.sourceFactoryProvider((exec, stagePlanner) -> {
                    factories.incrementAndGet();
                    return factory(exec, stagePlanner);
                });
                SequenceSourceOperator.FactoryProvider observed = (exec, stagePlanner, suppliedRegistry) -> {
                    assertSame(registry, suppliedRegistry);
                    return selected.create(exec, stagePlanner, suppliedRegistry);
                };
                Sequence sequence = sequence(2, Sequence.Pairing.LATEST_VALID, bound);
                var exec = new SequenceExec(Source.EMPTY, sequence);
                assertEquals(sequence.output(), exec.output());
                planner(observed, registry).plan(
                    "sequence-limits",
                    FoldContext.small(),
                    PlannerSettings.DEFAULTS,
                    exec,
                    EmptyIndexedByShardId.instance(),
                    true
                );
                assertEquals(1, factories.get());
            }
        }
    }

    /** Candidate ceilings are applied before even constructing the request dependency factory. */
    public void testExcessivePlanBoundIsRefusedBeforeFactoryCreation() {
        AtomicInteger factories = new AtomicInteger();
        var selected = SequenceSourceOperator.sourceFactoryProvider((exec, stagePlanner) -> {
            factories.incrementAndGet();
            return factory(exec, stagePlanner);
        });
        var exec = new SequenceExec(Source.EMPTY, sequence(4, Sequence.Pairing.LATEST_VALID, 65));
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> planner(selected, null).plan(
                    "sequence-limits",
                    FoldContext.small(),
                    PlannerSettings.DEFAULTS,
                    exec,
                    EmptyIndexedByShardId.instance(),
                    true
                )
            ).getMessage(),
            containsString("max_candidates [65] for [4] stages must be between [1] and [64]")
        );
        assertEquals(0, factories.get());
    }

    /** Large stage counts use a capped default rather than admitting the ordinary default16. */
    public void testCappedDefaultsAndArbitraryLegalBoundsReachTheFactory() {
        for (int[] shape : new int[][] { { 3, 380 }, { 4, 63 }, { 7, 0 }, { 22, 0 }, { 32, 0 } }) {
            AtomicInteger factories = new AtomicInteger();
            var selected = SequenceSourceOperator.sourceFactoryProvider((exec, stagePlanner) -> {
                factories.incrementAndGet();
                return factory(exec, stagePlanner);
            });
            var exec = new SequenceExec(Source.EMPTY, sequence(shape[0], Sequence.Pairing.LATEST_VALID, shape[1] == 0 ? null : shape[1]));
            planner(selected, null).plan(
                "sequence-limits",
                FoldContext.small(),
                PlannerSettings.DEFAULTS,
                exec,
                EmptyIndexedByShardId.instance(),
                true
            );
            assertEquals(1, factories.get());
        }
    }

    /** Today still returns the existing public nine-component factory, preserving default record behavior. */
    public void testTodayProviderKeepsTheExistingFactory() {
        var exec = new SequenceExec(Source.EMPTY, sequence(2, Sequence.Pairing.TODAY, null));
        SequenceSourceOperator.StagePlanner stages = (physical, output) -> List::of;
        var original = factory(exec, stages);
        var selected = SequenceSourceOperator.sourceFactoryProvider((ignored, unused) -> original);
        assertSame(original, selected.create(exec, stages, null));
        assertEquals(9, SequenceSourceOperator.Factory.class.getRecordComponents().length);
    }

    /** Missing production dependencies are still refused instead of silently using another pairing path. */
    public void testTodayKeepsTheExistingExecutionFactoryPath() {
        var exec = new SequenceExec(Source.EMPTY, sequence(2, Sequence.Pairing.TODAY, null));
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> planner().plan(
                    "sequence-limits",
                    FoldContext.small(),
                    PlannerSettings.DEFAULTS,
                    exec,
                    EmptyIndexedByShardId.instance(),
                    true
                )
            ).getMessage(),
            containsString("native SEQUENCE execution is not available in this planner")
        );
    }

    private static Sequence sequence(int stages, Sequence.Pairing pairing, Integer bound) {
        Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
        var child = new LocalRelation(Source.EMPTY, List.of(key), EmptyLocalSupplier.EMPTY);
        var stage = new SequenceStage(Source.EMPTY, child, List.of(key));
        return new Sequence(Source.EMPTY, Collections.nCopies(stages, stage), 1, pairing, bound).resolveSchema();
    }

    private static LocalExecutionPlanner planner() {
        return planner(null, null);
    }

    private static SequenceSourceOperator.Factory factory(SequenceExec exec, SequenceSourceOperator.StagePlanner stages) {
        // Physical planning does not use request clients/tasks. Native source tests own actual transport and driver dependencies.
        return new SequenceSourceOperator.Factory(exec, null, null, null, EsqlTestUtils.TEST_CFG, stages, null, null, null);
    }

    private static LocalExecutionPlanner planner(SequenceSourceOperator.FactoryProvider sequenceFactory, AnalysisRegistry registry) {
        BlockFactory blocks = TestBlockFactory.getNonBreakingInstance();
        return new LocalExecutionPlanner(
            "sequence-limits",
            "",
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            blocks,
            Settings.EMPTY,
            EsqlTestUtils.TEST_CFG,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ProjectMetadata.builder(ProjectId.DEFAULT).build(),
            new EsPhysicalOperationProviders(
                FoldContext.small(),
                EmptyIndexedByShardId.instance(),
                registry,
                PlannerSettings.DEFAULTS,
                () -> 0L,
                QueryWarnings.EMIT
            ),
            null,
            false,
            null,
            0,
            MatcherWatchdog.noop(),
            TransportVersion.current(),
            sequenceFactory
        );
    }
}
