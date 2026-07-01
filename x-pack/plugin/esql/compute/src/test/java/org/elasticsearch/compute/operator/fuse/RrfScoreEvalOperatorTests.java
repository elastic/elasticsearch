/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        return new RrfScoreEvalOperator.Factory(discriminatorPosition, scorePosition, config, new TestWarningsSource(null));
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

    /**
     * A multivalued group column cannot be grouped, so FUSE assigns that row a null score and emits two
     * warnings. This mirrors the {@code fuse.fuseWithRowRRFAndMultiValueGroupColumn} csv-spec case
     * asserted deterministically at the operator level.
     */
    public void testMultivaluedGroupColumnProducesWarning() {
        DriverContext ctx = driverContext();

        // The first row's group is multivalued; the rest are ordinary. size >= 3 so rows 1 and 2 exist.
        List<Page> input = CannedSourceOperator.collectPages(simpleInputWithMultivaluedGroup(ctx.blockFactory(), between(3, 100)));
        List<Page> output = fuseOutput(simple().get(ctx), input);

        try {
            assertThat(output, hasSize(1));
            DoubleBlock scores = output.get(0).getBlock(scorePosition);
            assertThat(scores.isNull(0), equalTo(true));   // multivalued group -> null score
            assertThat(scores.isNull(1), equalTo(false));  // ordinary rows keep a score
            assertThat(scores.isNull(2), equalTo(false));
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: group column contains multivalued entries; assigning null scores"
            );
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    private RrfConfig randomConfig() {
        return new RrfConfig((double) randomIntBetween(1, 100), randomWeights());
    }
}
