/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class LimitOperatorTests extends OperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new LimitOperator.Factory(100);
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        return new SequenceLongBlockSourceOperator(LongStream.range(0, size));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "LimitOperator[limit = 100]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "LimitOperator[limit = 100/100]";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputPositionCount = input.stream().mapToInt(p -> p.getPositionCount()).sum();
        int outputPositionCount = results.stream().mapToInt(p -> p.getPositionCount()).sum();
        assertThat(outputPositionCount, equalTo(Math.min(100, inputPositionCount)));
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeFalse("doesn't use big arrays", true);
        return null;
    }

    public void testStatus() {
        LimitOperator op = (LimitOperator) simple(BigArrays.NON_RECYCLING_INSTANCE).get(new DriverContext());

        LimitOperator.Status status = op.status();
        assertThat(status.limit(), equalTo(100));
        assertThat(status.limitRemaining(), equalTo(100));
        assertThat(status.pagesProcessed(), equalTo(0));

        Page p = new Page(Block.constantNullBlock(10));
        op.addInput(p);
        assertSame(p, op.getOutput());
        status = op.status();
        assertThat(status.limit(), equalTo(100));
        assertThat(status.limitRemaining(), equalTo(90));
        assertThat(status.pagesProcessed(), equalTo(1));
    }
}
