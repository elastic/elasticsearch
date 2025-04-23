/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.SampleSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.plan.logical.SampleSerializationTests.randomProbability;
import static org.elasticsearch.xpack.esql.plan.logical.SampleSerializationTests.randomSeed;

public class SampleExecSerializationTests extends AbstractPhysicalPlanSerializationTests<SampleExec> {
    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected SampleExec createTestInstance() {
        return new SampleExec(randomSource(), randomChild(0), randomProbability(), randomSeed());
    }

    /**
     * Returns an instance which is mutated slightly so it should not be equal
     * to the given instance.
     *
     * @param instance
     */
    @Override
    protected SampleExec mutateInstance(SampleExec instance) throws IOException {
        var probability = instance.probability();
        var seed = instance.seed();
        var child = instance.child();
        int updateSelector = randomIntBetween(0, 2);
        switch (updateSelector) {
            case 0 -> probability = randomValueOtherThan(probability, SampleSerializationTests::randomProbability);
            case 1 -> seed = randomValueOtherThan(seed, SampleSerializationTests::randomSeed);
            case 2 -> child = randomValueOtherThan(child, () -> randomChild(0));
            default -> throw new IllegalArgumentException("Invalid selector: " + updateSelector);
        }
        return new SampleExec(instance.source(), child, probability, seed);
    }
}
