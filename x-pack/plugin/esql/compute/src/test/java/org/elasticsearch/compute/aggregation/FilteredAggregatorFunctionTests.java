/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SequenceIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class FilteredAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private final List<Exception> unclosed = Collections.synchronizedList(new ArrayList<>());

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new FilteredAggregatorFunctionSupplier(
            new SumIntAggregatorFunctionSupplier(inputChannels),
            new FilteredGroupingAggregatorFunctionTests.AnyGreaterThanFactory(unclosed, inputChannels)
        );
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "Filtered[next=sum of ints, filter=any > 0]";
    }

    @Override
    protected String expectedToStringOfSimpleAggregator() {
        return "FilteredAggregatorFunction[next=SumIntAggregatorFunction[channels=[0]], filter=any > 0]";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        long sum = 0;
        for (Block block : input) {
            IntBlock ints = (IntBlock) block;
            for (int p = 0; p < ints.getPositionCount(); p++) {
                /*
                 * Perform the sum on the values *only* if any of the
                 * values is > 0 to line up with the condition
                 */
                int start = ints.getFirstValueIndex(p);
                int end = start + ints.getValueCount(p);
                boolean selected = false;
                for (int i = start; i < end; i++) {
                    selected |= ints.getInt(i) > 0;
                }
                if (selected == false) {
                    continue;
                }
                start = ints.getFirstValueIndex(p);
                end = start + ints.getValueCount(p);
                for (int i = start; i < end; i++) {
                    sum += ints.getInt(i);
                }
            }
        }
        assertThat(((LongBlock) result).getLong(0), equalTo(sum));
    }

    @Override
    protected List<Page> nullIntermediateState(BlockFactory blockFactory) {
        return new SumIntAggregatorFunctionTests().nullIntermediateState(blockFactory);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, Integer.MAX_VALUE / size / 5);
        return new SequenceIntBlockSourceOperator(blockFactory, IntStream.range(0, size).map(l -> between(-max, max)));
    }

    @After
    public void checkUnclosed() {
        for (Exception tracker : unclosed) {
            logger.error("unclosed", tracker);
        }
        assertThat(unclosed, empty());
    }

    @Override
    public void testNoneFiltered() {
        assumeFalse("can't double filter. tests already filter.", true);
    }

    @Override
    public void testAllFiltered() {
        assumeFalse("can't double filter. tests already filter.", true);
    }

    @Override
    public void testSomeFiltered() {
        assumeFalse("can't double filter. tests already filter.", true);
    }
}
