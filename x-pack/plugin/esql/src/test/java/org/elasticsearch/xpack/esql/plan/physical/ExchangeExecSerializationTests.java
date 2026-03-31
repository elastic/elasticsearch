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

public class ExchangeExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExchangeExec> {
    static ExchangeExec randomExchangeExec(int depth) {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean inBetweenAggs = randomBoolean();
        PhysicalPlan child = randomChild(depth);
        return new ExchangeExec(source, output, inBetweenAggs, child);
    }

    @Override
    protected ExchangeExec createTestInstance() {
        return randomExchangeExec(0);
    }

    @Override
    protected ExchangeExec mutateInstance(ExchangeExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean inBetweenAggs = instance.inBetweenAggs();
        PhysicalPlan child = instance.child();
        switch (between(0, 2)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> inBetweenAggs = false == inBetweenAggs;
            case 2 -> child = randomValueOtherThan(child, () -> randomChild(0));
        }
        return new ExchangeExec(instance.source(), output, inBetweenAggs, child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
