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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RrfScoreEvalOperatorTests extends FuseOperatorTestCase {
    protected RrfConfig config;

    @Before
    public void setup() {
        config = randomConfig();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Map<String, Integer> counts = new HashMap<>();

        assertOutput(input, results, (discriminator, actualScore, initialScore) -> {
            var rank = counts.getOrDefault(discriminator, 1);
            var weight = config.weights().getOrDefault(discriminator, 1.0);
            assertEquals(actualScore, 1.0 / (config.rankConstant() + rank) * weight, 0.0d);
            counts.put(discriminator, rank + 1);
        });
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RrfScoreEvalOperator.Factory(discriminatorPosition, scorePosition, config, null, 0, 0);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "RrfScoreEvalOperator[discriminatorPosition="
                + discriminatorPosition
                + ", scorePosition="
                + scorePosition
                + ", rrfConfig="
                + config
                + "]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("RrfScoreEvalOperator");
    }

    private RrfConfig randomConfig() {
        return new RrfConfig((double) randomIntBetween(1, 100), randomWeights());
    }
}
