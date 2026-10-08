/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;

public class FetchSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<FetchSourceExec> {
    public static FetchSourceExec randomFetchSourceExec() {
        return new FetchSourceExec(randomSource(), DocRefEncodeExecSerializationTests.randomDoc(), randomEstimatedRowSize());
    }

    @Override
    protected FetchSourceExec createTestInstance() {
        return randomFetchSourceExec();
    }

    @Override
    protected FetchSourceExec mutateInstance(FetchSourceExec instance) throws IOException {
        Attribute doc = instance.doc();
        Integer estimatedRowSize = instance.estimatedRowSize();
        switch (between(0, 1)) {
            case 0 -> doc = randomValueOtherThan(doc, DocRefEncodeExecSerializationTests::randomDoc);
            case 1 -> estimatedRowSize = randomValueOtherThan(
                estimatedRowSize,
                AbstractPhysicalPlanSerializationTests::randomEstimatedRowSize
            );
            default -> throw new AssertionError("Unexpected case");
        }
        return new FetchSourceExec(instance.source(), doc, estimatedRowSize);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
