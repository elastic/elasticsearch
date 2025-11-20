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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
            if (config.normalizer() == LinearConfig.Normalizer.NONE) {
                assertEquals(actualScore, initialScore * weight, 0.00);
            } else if (config.normalizer() == LinearConfig.Normalizer.MINMAX) {
                // for min_max, we know the normalized scores will be between 0..1
                // when we apply the weight, the scores should be between 0 and weight
                assertThat(actualScore, lessThanOrEqualTo(weight));
                assertThat(actualScore, greaterThanOrEqualTo(0.0));
            } else {
                // for l2_norm, we could be dealing with negative scores
                // in this case the normalized scores will be between -1 and 1.
                // when we apply the weight, the scores should be between -weight and weight
                assertThat(actualScore, lessThanOrEqualTo(weight));
                assertThat(actualScore, greaterThanOrEqualTo(-weight));
            }
        });
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new LinearScoreEvalOperator.Factory(discriminatorPosition, scorePosition, config, null, 0, 0);
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
        return new LinearConfig(randomFrom(LinearConfig.Normalizer.values()), randomWeights());
    }
}
