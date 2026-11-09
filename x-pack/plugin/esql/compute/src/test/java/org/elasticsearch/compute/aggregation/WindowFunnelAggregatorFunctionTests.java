/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.WindowFunnelGroupingAggregatorFunctionTests.ExpectedFunnel;
import org.elasticsearch.compute.aggregation.WindowFunnelGroupingAggregatorFunctionTests.FunnelInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class WindowFunnelAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private final FunnelInput funnel = FunnelInput.random();

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.<Object>of(funnel.timestamp(), funnel.mask())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new WindowFunnelAggregatorFunctionSupplier(funnel.span(), funnel.steps());
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "window_funnel";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        ExpectedFunnel expected = new ExpectedFunnel();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                expected.add(page.getBlock(0), page.getBlock(1), p);
            }
        }
        assertThat(BlockUtils.toJavaObject(result, 0), equalTo(expected.level(funnel.span(), funnel.steps())));
    }
}
