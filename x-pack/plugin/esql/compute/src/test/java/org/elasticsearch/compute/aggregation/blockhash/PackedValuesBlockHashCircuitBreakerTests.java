/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.util.MockBigArrays.ERROR_MESSAGE;
import static org.hamcrest.Matchers.equalTo;

public class PackedValuesBlockHashCircuitBreakerTests extends BlockHashTestCase {

    /**
     * Set the breaker limit low enough, and test that adding many(1000) groups of BYTES_REF into bytes {@code BreakingBytesRefBuilder}
     * , which is reused for each grouping set will trigger CBE. CBE happens when adding around 25th group to bytes.
     */
    public void testCircuitBreakerWithManyGroups() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(220000));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
        MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

        // 1000 group keys of BYTES_REF
        List<BlockHash.GroupSpec> groupSpecs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            groupSpecs.add(new BlockHash.GroupSpec(i, ElementType.BYTES_REF));
        }

        try (
            PackedValuesBlockHash blockHash = new PackedValuesBlockHash(groupSpecs, blockFactory, 32);
            BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            builder.appendBytesRef(new BytesRef("test"));
            Block block = builder.build();
            Block[] blocks = new Block[1000];
            for (int i = 0; i < 1000; i++) {
                blocks[i] = block;
            }
            Page page = new Page(blocks);

            CircuitBreakingException e = expectThrows(
                CircuitBreakingException.class,
                () -> blockHash.add(page, new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {}

                    @Override
                    public void close() {}
                })
            );
            assertThat(e.getMessage(), equalTo(ERROR_MESSAGE));
        } finally {
            blockFactory.ensureAllBlocksAreReleased();
        }
    }
}
