/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.GroundTruthFirstLastAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.processPages;

public class AllLastBooleanByTimestampGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen tsgen = randomFrom(
            FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen.values()
        );
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.BOOLEAN, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomBoolean(), tsgen.gen())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllLastBooleanByTimestampAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_last_boolean_by_timestamp";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        GroundTruthFirstLastAggregator work = new GroundTruthFirstLastAggregator(false);
        processPages(work, input, group);
        work.check(BlockUtils.toJavaObject(result, position));
    }
}
