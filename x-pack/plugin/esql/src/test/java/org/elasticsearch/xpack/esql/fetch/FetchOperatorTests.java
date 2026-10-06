/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FetchOperatorTests extends ComputeTestCase {
    /**
     * {@code EXPLAIN} runs the plan over empty sources, so the cut keeps no rows and the operator finishes without
     * output.
     */
    public void testFinishesWithoutRows() {
        try (Operator operator = factory().get(driverContext(blockFactory()))) {
            assertTrue(operator.needsInput());
            operator.finish();
            assertNull(operator.getOutput());
            assertTrue(operator.isFinished());
        }
    }

    public void testFailsOnRows() {
        BlockFactory blockFactory = blockFactory();
        try (Operator operator = factory().get(driverContext(blockFactory))) {
            operator.addInput(new Page(RandomBlock.randomDocRefBlock(blockFactory, between(1, 10), between(1, 3)).block()));
            IllegalStateException e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(e.getMessage(), equalTo("the fetch phase can't load documents yet"));
        }
        // closing the operator released the page, and the test case checks that every breaker is empty
    }

    public void testDescribe() {
        assertThat(factory().describe(), equalTo("FetchOperator[docRefChannel=0, fetchedTypes=[BYTES_REF, LONG]]"));
    }

    public void testProviderBuildsTheFactory() {
        Operator.OperatorFactory factory = FetchOperator.PROVIDER.fetchOperator(null, 0, fetchedTypes());
        assertThat(factory, equalTo(factory()));
    }

    private static FetchOperator.Factory factory() {
        return new FetchOperator.Factory(0, fetchedTypes());
    }

    private static List<ElementType> fetchedTypes() {
        return List.of(ElementType.BYTES_REF, ElementType.LONG);
    }

    private static DriverContext driverContext(BlockFactory blockFactory) {
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
