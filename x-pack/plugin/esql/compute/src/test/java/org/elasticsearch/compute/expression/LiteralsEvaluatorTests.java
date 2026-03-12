/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LiteralsEvaluatorTests extends ComputeTestCase {
    public record TestCase(Object lit, String expectedToString) {}

    @ParametersFactory(argumentFormatting = "%s")
    public static List<Object[]> params() {
        List<TestCase> params = new ArrayList<>();
        params.add(new TestCase(1, "LiteralsEvaluator[lit=1]"));
        params.add(new TestCase(new BytesRef(","), "LiteralsEvaluator[lit=[2c]]"));
        return params.stream().map(c -> new Object[] { c }).toList();
    }

    private final TestCase testCase;

    public LiteralsEvaluatorTests(TestCase testCase) {
        this.testCase = testCase;
    }

    public void testToString() {
        assertThat(new LiteralsEvaluator.Factory(testCase.lit).get(driverContext()).toString(), equalTo(testCase.expectedToString));
    }

    public void testFactoryToString() {
        assertThat(new LiteralsEvaluator.Factory(testCase.lit).toString(), equalTo(testCase.expectedToString));
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
