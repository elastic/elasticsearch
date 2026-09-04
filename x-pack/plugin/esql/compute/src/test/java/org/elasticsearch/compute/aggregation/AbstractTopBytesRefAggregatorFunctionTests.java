/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceBytesRefBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;

abstract class AbstractTopBytesRefAggregatorFunctionTests extends AggregatorFunctionTestCase {
    static final int LIMIT = 100;

    @Override
    protected final SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBytesRefBlockSourceOperator(blockFactory, IntStream.range(0, size).mapToObj(l -> randomValue()));
    }

    protected abstract BytesRef randomValue();

    @Override
    public final void assertSimpleOutput(List<Page> input, Block result) {
        Object[] values = input.stream().flatMap(p -> allBytesRefs(p.getBlock(0))).sorted().limit(LIMIT).toArray(Object[]::new);
        assertThat((List<?>) BlockUtils.toJavaObject(result, 0), contains(values));
    }
}
