/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

import static org.hamcrest.Matchers.equalTo;

public class ChildRelationColumnsTests extends ESTestCase {
    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    /** Parents 0 and 2 have elements, 1 has none; a null and a multivalued leaf survive the round trip in element order. */
    public void testRoundTrip() {
        int[] offsets = { 0, 2, 2, 3 };
        Block[] leaves = new Block[2];
        try (
            LongBlock.Builder ttl = blockFactory.newLongBlockBuilder(3);
            BytesRefBlock.Builder tags = blockFactory.newBytesRefBlockBuilder(3)
        ) {
            ttl.appendLong(30);
            ttl.appendNull();
            ttl.appendLong(10);
            tags.beginPositionEntry();
            tags.appendBytesRef(new BytesRef("a"));
            tags.appendBytesRef(new BytesRef("b"));
            tags.endPositionEntry();
            tags.appendBytesRef(new BytesRef("c"));
            tags.appendNull();
            leaves[0] = ttl.build();
            leaves[1] = tags.build();
        }
        try (BytesRefBlock carrier = ChildRelationColumns.encode(blockFactory, 3, offsets, leaves)) {
            assertThat(
                decode(carrier, new int[] { 1, 0 }, 1024, p -> true),
                equalTo(
                    List.of(
                        Arrays.asList(0, List.of(new BytesRef("a"), new BytesRef("b")), 30L),
                        Arrays.asList(0, new BytesRef("c"), null),
                        Arrays.asList(2, null, 10L)
                    )
                )
            );
            // A subset of the leaves, in pages of one child
            assertThat(
                decode(carrier, new int[] { 0 }, 1, p -> true),
                equalTo(List.of(List.of(0, 30L), Arrays.asList(0, null), List.of(2, 10L)))
            );
            // A parent that needs no more children stops being read
            assertThat(decode(carrier, new int[] { 0 }, 1, p -> p != 0), equalTo(List.of(List.of(2, 10L))));
        } finally {
            Releasables.closeExpectNoException(leaves);
        }
    }

    private List<List<Object>> decode(BytesRefBlock carrier, int[] wanted, int pageSize, IntPredicate keepReading) {
        List<List<Object>> rows = new ArrayList<>();
        ChildRelationColumns.decode(
            carrier,
            new ElementType[] { ElementType.LONG, ElementType.BYTES_REF },
            wanted,
            p -> true,
            keepReading,
            blockFactory,
            pageSize,
            page -> {
                IntBlock parents = page.getBlock(0);
                for (int c = 0; c < page.getPositionCount(); c++) {
                    List<Object> row = new ArrayList<>();
                    row.add(parents.getInt(c));
                    for (int b = 1; b < page.getBlockCount(); b++) {
                        row.add(BlockUtils.toJavaObject(page.getBlock(b), c));
                    }
                    rows.add(row);
                }
            }
        );
        return rows;
    }
}
