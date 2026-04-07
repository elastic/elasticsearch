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

public class ExchangeSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExchangeSourceExec> {
    static ExchangeSourceExec randomExchangeSourceExec() {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean intermediateAgg = randomBoolean();
        return new ExchangeSourceExec(source, output, intermediateAgg);
    }

    @Override
    protected ExchangeSourceExec createTestInstance() {
        return randomExchangeSourceExec();
    }

    @Override
    protected ExchangeSourceExec mutateInstance(ExchangeSourceExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean intermediateAgg = instance.isIntermediateAgg();
        if (randomBoolean()) {
            output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
        } else {
            intermediateAgg = false == intermediateAgg;
        }
        return new ExchangeSourceExec(instance.source(), output, intermediateAgg);
    }
}
