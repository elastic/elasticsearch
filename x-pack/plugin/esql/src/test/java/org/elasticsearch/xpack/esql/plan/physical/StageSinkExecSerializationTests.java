/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class StageSinkExecSerializationTests extends AbstractPhysicalPlanSerializationTests<StageSinkExec> {
    private static StageSinkExec randomStageSinkExec(int depth) {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean intermediateAgg = randomBoolean();
        String edgeKey = randomAlphaOfLength(5);
        ExchangeDistribution distribution = randomLegacyDistribution();
        List<Attribute> keys = distribution == ExchangeDistribution.HASH ? randomFieldAttributes(1, 5, false) : List.of();
        PhysicalPlan child = randomChild(depth);
        return new StageSinkExec(source, output, intermediateAgg, edgeKey, distribution, keys, child);
    }

    public void testBroadcastEdgeRequiresItsTransportVersion() throws IOException {
        StageSinkExec stageSink = randomStageSinkExec(0);
        StageSinkExec broadcastSink = new StageSinkExec(
            stageSink.source(),
            stageSink.output(),
            stageSink.isIntermediateAgg(),
            stageSink.edgeKey(),
            ExchangeDistribution.BROADCAST,
            List.of(),
            stageSink.child()
        );

        assertEquals(broadcastSink, copyInstance(broadcastSink, ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION));
        expectThrows(
            IllegalStateException.class,
            () -> copyInstance(broadcastSink, TransportVersionUtils.getPreviousVersion(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION))
        );
    }

    @Override
    protected StageSinkExec createTestInstance() {
        return randomStageSinkExec(0);
    }

    @Override
    protected StageSinkExec mutateInstance(StageSinkExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean intermediateAgg = instance.isIntermediateAgg();
        String edgeKey = instance.edgeKey();
        ExchangeDistribution distribution = instance.distribution();
        List<Attribute> keys = instance.keys();
        PhysicalPlan child = instance.child();
        switch (between(0, 4)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> intermediateAgg = intermediateAgg == false;
            case 2 -> edgeKey = randomValueOtherThan(edgeKey, () -> randomAlphaOfLength(5));
            case 3 -> {
                distribution = randomValueOtherThan(distribution, StageSinkExecSerializationTests::randomLegacyDistribution);
                keys = distribution == ExchangeDistribution.HASH ? randomFieldAttributes(1, 5, false) : List.of();
            }
            case 4 -> child = randomValueOtherThan(child, () -> randomChild(0));
            default -> throw new AssertionError();
        }
        return new StageSinkExec(instance.source(), output, intermediateAgg, edgeKey, distribution, keys, child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    private static ExchangeDistribution randomLegacyDistribution() {
        return randomFrom(ExchangeDistribution.GATHER, ExchangeDistribution.HASH);
    }
}
