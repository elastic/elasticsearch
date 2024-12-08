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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxIpAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new MaxIpAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of ips";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        Optional<BytesRef> max = input.stream().flatMap(b -> allBytesRefs(b)).max(Comparator.naturalOrder());
        if (max.isEmpty()) {
            assertThat(result.isNull(0), equalTo(true));
            return;
        }
        assertThat(result.isNull(0), equalTo(false));
        assertThat(BlockUtils.toJavaObject(result, 0), equalTo(max.get()));
    }
}
