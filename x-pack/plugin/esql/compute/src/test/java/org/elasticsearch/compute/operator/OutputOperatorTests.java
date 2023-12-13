/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class OutputOperatorTests extends AnyOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, p -> {});
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "OutputOperator[columns = [a]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    private Operator.OperatorFactory big() {
        return new OutputOperator.OutputOperatorFactory(IntStream.range(0, 20).mapToObj(i -> "a" + i).toList(), p -> p, p -> {});
    }

    private String expectedDescriptionOfBig() {
        return "OutputOperator[columns = [20 columns]]";
    }

    public void testBigToString() {
        try (Operator operator = big().get(driverContext())) {
            assertThat(operator.toString(), equalTo(expectedDescriptionOfBig()));
        }
    }

    public void testBigDescription() {
        assertThat(big().describe(), equalTo(expectedDescriptionOfBig()));
    }
}
