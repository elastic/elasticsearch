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
    public void testEverySequenceSelectsPreparedFactoryAndReceivesThePlanningRegistry() throws Exception {
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
            for (int stageCount : new int[] { 2, 4, 7, 22, 32 }) {
                AtomicInteger factories = new AtomicInteger();
                var selected = SequenceSourceOperator.sourceFactoryProvider((exec, stagePlanner) -> {
                    factories.incrementAndGet();
                    return factory(exec, stagePlanner);
                });
                SequenceSourceOperator.FactoryProvider observed = (exec, stagePlanner, suppliedRegistry) -> {
                    assertSame(registry, suppliedRegistry);
                    return selected.create(exec, stagePlanner, suppliedRegistry);
                };
                Sequence sequence = sequence(stageCount);
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

    /** Missing production dependencies are still refused instead of silently using another pairing path. */
    public void testMissingNativeDependenciesAreRefused() {
        var exec = new SequenceExec(Source.EMPTY, sequence(2));
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

    private static Sequence sequence(int stages) {
        Attribute key = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
        var child = new LocalRelation(Source.EMPTY, List.of(key), EmptyLocalSupplier.EMPTY);
        var stage = new SequenceStage(Source.EMPTY, child, List.of(key));
        return new Sequence(Source.EMPTY, Collections.nCopies(stages, stage), 1).resolveSchema();
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
