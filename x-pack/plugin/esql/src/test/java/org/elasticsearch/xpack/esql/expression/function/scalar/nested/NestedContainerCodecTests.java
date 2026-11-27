/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class NestedContainerCodecTests extends ComputeTestCase {
    public void testRoundTripKeepsTypesValuesAndMultivalues() throws IOException {
        BlockFactory factory = blockFactory();
        try (Page children = children(factory)) {
            BytesRef bytes = NestedContainerCodec.encode(children);
            try (Page decoded = NestedContainerCodec.decode(factory, bytes)) {
                assertThat(decoded.getPositionCount(), equalTo(3));
                assertThat(decoded.getBlockCount(), equalTo(2));
                LongBlock qty = decoded.getBlock(0);
                assertThat(qty.getLong(qty.getFirstValueIndex(0)), equalTo(1L));
                assertThat(qty.isNull(1), equalTo(true));
                assertThat(qty.getLong(qty.getFirstValueIndex(2)), equalTo(5L));
                BytesRefBlock sku = decoded.getBlock(1);
                assertThat(sku.getValueCount(0), equalTo(2));
                assertThat(sku.getBytesRef(sku.getFirstValueIndex(0), new BytesRef()), equalTo(new BytesRef("a")));
                assertThat(sku.getBytesRef(sku.getFirstValueIndex(0) + 1, new BytesRef()), equalTo(new BytesRef("b")));
                assertThat(sku.getBytesRef(sku.getFirstValueIndex(2), new BytesRef()), equalTo(new BytesRef("c")));
            }
        }
    }

    public void testEncodedSizeGrowsWithTheValues() throws IOException {
        BlockFactory factory = blockFactory();
        try (Page children = children(factory)) {
            assertThat(NestedContainerCodec.encode(children).length, greaterThan(0));
        }
    }

    public void testDecodeBreaksTheBreakerWithoutLeaking() throws IOException {
        BytesRef bytes;
        try (Page children = children(blockFactory())) {
            bytes = NestedContainerCodec.encode(children);
        }
        testWithCrankyBlockFactory(factory -> {
            try (Page ignored = NestedContainerCodec.decode(factory, bytes)) {
                // the cranky breaker may or may not trip; ComputeTestCase checks that nothing is left allocated
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    private static Page children(BlockFactory factory) {
        LongBlock.Builder qty = factory.newLongBlockBuilder(3);
        qty.appendLong(1);
        qty.appendNull();
        qty.appendLong(5);
        BytesRefBlock.Builder sku = factory.newBytesRefBlockBuilder(3);
        sku.beginPositionEntry();
        sku.appendBytesRef(new BytesRef("a"));
        sku.appendBytesRef(new BytesRef("b"));
        sku.endPositionEntry();
        sku.appendNull();
        sku.appendBytesRef(new BytesRef("c"));
        Page page = new Page(qty.build(), sku.build());
        assertThat(page.getPositionCount(), equalTo(3));
        return page;
    }

}
