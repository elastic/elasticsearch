/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.operator.SequenceBytesRefBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;

public class TopIpAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private static final int LIMIT = 100;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new TopIpAggregatorFunctionSupplier(inputChannels, LIMIT, true);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "top of ips";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        Object[] values = input.stream().flatMap(b -> allBytesRefs(b)).sorted().limit(LIMIT).toArray(Object[]::new);
        assertThat((List<?>) BlockUtils.toJavaObject(result, 0), contains(values));
    }
}
