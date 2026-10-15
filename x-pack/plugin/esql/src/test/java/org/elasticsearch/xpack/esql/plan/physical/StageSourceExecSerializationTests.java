/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

public class StageSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<StageSourceExec> {
    private static StageSourceExec randomStageSourceExec() {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean intermediateAgg = randomBoolean();
        String edgeKey = randomAlphaOfLength(5);
        return new StageSourceExec(source, output, intermediateAgg, edgeKey);
    }

    @Override
    protected StageSourceExec createTestInstance() {
        return randomStageSourceExec();
    }

    @Override
    protected StageSourceExec mutateInstance(StageSourceExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean intermediateAgg = instance.isIntermediateAgg();
        String edgeKey = instance.edgeKey();
        switch (between(0, 2)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> intermediateAgg = intermediateAgg == false;
            case 2 -> edgeKey = randomValueOtherThan(edgeKey, () -> randomAlphaOfLength(5));
            default -> throw new AssertionError();
        }
        return new StageSourceExec(instance.source(), output, intermediateAgg, edgeKey);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
