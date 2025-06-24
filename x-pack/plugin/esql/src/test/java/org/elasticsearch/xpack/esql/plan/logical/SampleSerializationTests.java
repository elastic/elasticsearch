/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

public class SampleSerializationTests extends AbstractLogicalPlanSerializationTests<Sample> {
    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected Sample createTestInstance() {
        return new Sample(randomSource(), randomProbability(), randomChild(0));
    }

    public static Literal randomProbability() {
        return new Literal(randomSource(), randomDoubleBetween(0, 1, false), DataType.DOUBLE);
    }

    public static Literal randomSeed() {
        return randomBoolean() ? new Literal(randomSource(), randomInt(), DataType.INTEGER) : null;
    }

    /**
     * Returns an instance which is mutated slightly so it should not be equal
     * to the given instance.
     *
     * @param instance
     */
    @Override
    protected Sample mutateInstance(Sample instance) throws IOException {
        var probability = instance.probability();
        var child = instance.child();
        int updateSelector = randomIntBetween(0, 1);
        switch (updateSelector) {
            case 0 -> probability = randomValueOtherThan(probability, SampleSerializationTests::randomProbability);
            case 1 -> child = randomValueOtherThan(child, () -> randomChild(0));
            default -> throw new IllegalArgumentException("Invalid selector: " + updateSelector);
        }
        return new Sample(instance.source(), probability, child);
    }
}
