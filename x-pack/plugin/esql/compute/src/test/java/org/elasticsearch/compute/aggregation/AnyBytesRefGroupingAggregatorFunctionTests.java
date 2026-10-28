/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.GroundTruthFirstLastAggregator;

public class AnyBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.BYTES_REF),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomAlphanumericOfLength(randomInt(20)))).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 1;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AnyBytesRefAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "any_BytesRef_aggregator";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Set<List<Object>> expected = new HashSet<>();
        for (Page page : input) {
            Block block = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); ++p) {
                List<Object> values = BlockTestUtils.valuesAtPosition(block, p, true);
                Collections.sort(values, null);
                expected.add(values);
            }
        }

        Object actual = BlockUtils.toJavaObject(result, position);
        if (actual != null) {
            GroundTruthFirstLastAggregator.check(expected, actual);
        }
    }
}
