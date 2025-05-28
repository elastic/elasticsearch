/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.matchesPattern;

public class SampleOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();
        double meanExpectedOutputCount = 0.5 * inputCount;
        double stdDevExpectedOutputCount = Math.sqrt(meanExpectedOutputCount);
        assertThat((double) outputCount, closeTo(meanExpectedOutputCount, 10 * stdDevExpectedOutputCount));
    }

    @Override
    protected SampleOperator.Factory simple(SimpleOptions options) {
        return new SampleOperator.Factory(0.5, randomInt());
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesPattern("SampleOperator\\[probability = 0.5, seed = -?\\d+]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("SampleOperator[sampled = 0/0]");
    }

    public void testAccuracy() {
        BlockFactory blockFactory = driverContext().blockFactory();
        int totalPositionCount = 0;

        for (int iter = 0; iter < 10000; iter++) {
            SampleOperator operator = simple(SimpleOptions.DEFAULT).get(driverContext());
            operator.addInput(new Page(blockFactory.newConstantNullBlock(20000)));
            Page output = operator.getOutput();
            // 10000 expected rows, stddev=sqrt(10000)=100, so this is 10 stddevs.
            assertThat(output.getPositionCount(), both(greaterThan(9000)).and(lessThan(11000)));
            totalPositionCount += output.getPositionCount();
            output.releaseBlocks();
        }

        int averagePositionCount = totalPositionCount / 10000;
        // Running 10000 times, so the stddev is divided by sqrt(10000)=100, so this 10 stddevs again.
        assertThat(averagePositionCount, both(greaterThan(9990)).and(lessThan(10010)));
    }
}
