/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import static org.hamcrest.Matchers.equalTo;

public class ExchangeResponseTests extends ComputeTestCase {

    public void testReverseBytesWhenSerializing() throws Exception {
        BlockFactory factory = blockFactory();
        int numBlocks = between(1, 10);
        Block[] blocks = new Block[numBlocks];
        int positions = randomIntBetween(1, 10);
        for (int b = 0; b < numBlocks; b++) {
            var block = RandomBlock.randomBlock(
                factory,
                randomFrom(ElementType.BOOLEAN, ElementType.LONG, ElementType.BYTES_REF),
                positions,
                randomBoolean(),
                1,
                5,
                0,
                1
            );
            blocks[b] = block.block();
        }
        Page page = new Page(blocks);
        ExchangeResponse response = new ExchangeResponse(factory, page, randomBoolean());
        long beforeUsage = factory.breaker().getUsed();
        assertThat(page.ramBytesUsedByBlocks(), equalTo(beforeUsage));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
        }
        long afterUsage = factory.breaker().getUsed();
        assertThat(afterUsage, equalTo(beforeUsage * 2L));
        response.close();
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }
}
