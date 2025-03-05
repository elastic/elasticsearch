/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.RandomSampleSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.plan.logical.RandomSampleSerializationTests.randomProbability;
import static org.elasticsearch.xpack.esql.plan.logical.RandomSampleSerializationTests.randomSeed;

public class RandomSampleExecSerializationTests extends AbstractPhysicalPlanSerializationTests<RandomSampleExec> {
    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected RandomSampleExec createTestInstance() {
        return new RandomSampleExec(randomSource(), randomChild(0), randomProbability(), randomSeed());
    }

    /**
     * Returns an instance which is mutated slightly so it should not be equal
     * to the given instance.
     *
     * @param instance
     */
    @Override
    protected RandomSampleExec mutateInstance(RandomSampleExec instance) throws IOException {
        var probability = instance.probability();
        var seed = instance.seed();
        var child = instance.child();
        int updateSelector = randomIntBetween(0, 2);
        switch (updateSelector) {
            case 0 -> probability = randomValueOtherThan(probability, RandomSampleSerializationTests::randomProbability);
            case 1 -> seed = randomValueOtherThan(seed, RandomSampleSerializationTests::randomSeed);
            case 2 -> child = randomValueOtherThan(child, () -> randomChild(0));
            default -> throw new IllegalArgumentException("Invalid selector: " + updateSelector);
        }
        return new RandomSampleExec(instance.source(), child, probability, seed);
    }
}
