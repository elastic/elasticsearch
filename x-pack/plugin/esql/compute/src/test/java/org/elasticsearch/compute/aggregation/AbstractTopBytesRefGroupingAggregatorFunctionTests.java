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
import org.elasticsearch.compute.operator.LongBytesRefTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTopBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    static final int LIMIT = 100;

    @Override
    protected final SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongBytesRefTupleBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomValue()))
        );
    }

    protected abstract BytesRef randomValue();

    @Override
    protected final void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Object[] values = input.stream().flatMap(b -> allBytesRefs(b, group)).sorted().limit(LIMIT).toArray(Object[]::new);
        if (values.length == 0) {
            assertThat(result.isNull(position), equalTo(true));
        } else if (values.length == 1) {
            assertThat(BlockUtils.toJavaObject(result, position), equalTo(values[0]));
        } else {
            assertThat((List<?>) BlockUtils.toJavaObject(result, position), contains(values));
        }
    }
}
