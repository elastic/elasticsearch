/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LinearScoreEvalOperatorTests extends FuseOperatorTestCase {
    private LinearConfig config;

    @Before
    public void setup() {
        config = randomConfig();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertOutput(input, results, (discriminator, actualScore, initialScore) -> {
            var weight = config.weights().getOrDefault(discriminator, 1.0);
            assertEquals(actualScore, initialScore * weight, 0.00);
        });
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new LinearScoreEvalOperator.Factory(discriminatorPosition, scorePosition, config);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "LinearScoreEvalOperator[discriminatorPosition="
                + discriminatorPosition
                + ", scorePosition="
                + scorePosition
                + ", config="
                + config
                + "]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "LinearScoreEvalOperator[discriminatorPosition="
                + discriminatorPosition
                + ", scorePosition="
                + scorePosition
                + ", config="
                + config
                + "]"
        );
    }

    private LinearConfig randomConfig() {
        return new LinearConfig(LinearConfig.Normalizer.NONE, randomWeights());
    }
}
