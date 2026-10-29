/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.test.BreakerTestUtil;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;

/**
 * Tests that {@link ResultBuilderForDoc} releases every shard ref it takes. Decoding takes one ref per row.
 * The {@link DocBlock} that {@link ResultBuilderForDoc#build} returns adopts them. If that block is never
 * built then closing the builder must release them.
 */
public class ResultBuilderForDocTests extends ComputeTestCase {
    public void testBuild() throws Exception {
        List<AbstractRefCounted> shardRefs = shardRefs(between(1, 5));
        int[] shards = randomShards(shardRefs.size());
        Block block;
        try (ResultBuilderForDoc builder = decode(blockFactory(), shardRefs, shards, shards.length)) {
            block = builder.build();
        }
        IntVector builtShards = ((DocBlock) block).asVector().shards();
        assertThat(IntStream.range(0, builtShards.getPositionCount()).map(builtShards::getInt).toArray(), equalTo(shards));

        // The block owns one ref per row on top of the ref each shard starts with
        int[] expectedRefCounts = new int[shardRefs.size()];
        for (int shard : shards) {
            expectedRefCounts[shard]++;
        }
        assertThat(refCounts(shardRefs), equalTo(IntStream.of(expectedRefCounts).mapToObj(rows -> 1 + rows).toList()));

        block.close();
        assertAllReleased(shardRefs);
    }

    /**
     * A single row builds a constant shards vector. The {@link DocBlock} releases a single ref for
     * a constant shards vector, which matches the single ref that decoding took.
     */
    public void testBuildSingleRow() throws Exception {
        List<AbstractRefCounted> shardRefs = shardRefs(1);
        Block block;
        try (ResultBuilderForDoc builder = decode(blockFactory(), shardRefs, new int[] { 0 }, 1)) {
            block = builder.build();
        }
        assertTrue(((DocBlock) block).asVector().shards().isConstant());
        assertThat(refCounts(shardRefs), equalTo(List.of(2)));

        block.close();
        assertAllReleased(shardRefs);
    }

    /**
     * {@link TopNOperator} closes the builder without building it when decoding or building another column fails.
     */
    public void testCloseWithoutBuild() throws Exception {
        List<AbstractRefCounted> shardRefs = shardRefs(between(1, 5));
        int[] shards = randomShards(shardRefs.size());
        decode(blockFactory(), shardRefs, shards, shards.length).close();
        assertAllReleased(shardRefs);
    }

    /**
     * Trips the breaker while building the {@link DocBlock}, after decoding every row.
     */
    public void testBuildBreaks() throws Exception {
        int shardCount = between(1, 5);
        int[] shards = randomShards(shardCount);
        ByteSizeValue decodeBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
            try {
                decode(blockFactory(limit), shardRefs, shards, shards.length).close();
            } finally {
                assertAllReleased(shardRefs);
            }
        });
        /*
         * Checking the refs in here covers every limit the search tries. That includes the largest
         * limit that breaks, which always trips on the last charge, in the DocVector constructor.
         */
        ByteSizeValue buildBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
            try (ResultBuilderForDoc builder = decode(blockFactory(limit), shardRefs, shards, shards.length)) {
                builder.build().close();
            } finally {
                assertAllReleased(shardRefs);
            }
        });
        var limit = ByteSizeValue.ofBytes(randomLongBetween(decodeBreakLimit.getBytes() + 1, buildBreakLimit.getBytes()));
        List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
        try (ResultBuilderForDoc builder = decode(blockFactory(limit), shardRefs, shards, shards.length)) {
            Exception e = expectThrows(CircuitBreakingException.class, builder::build);
            assertThat(e.getMessage(), equalTo("over test limit"));
            logger.info("break position", e);
        }
        assertAllReleased(shardRefs);
    }

    /**
     * Trips the breaker while decoding more rows than the builder was sized for. {@link TopNOperator}
     * sizes the builder for every row it decodes, so it never grows there. This makes sure that a
     * failed append still releases the ref that decoding took.
     */
    public void testDecodeBreaks() throws Exception {
        int shardCount = between(1, 5);
        // Decode more rows than the builder has room for so it has to grow. It always has room for at least two rows.
        int[] shards = randomShards(shardCount, between(3, 100));
        int positions = between(1, shards.length - 1);
        ByteSizeValue createBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
            try {
                decode(blockFactory(limit), shardRefs, new int[0], positions).close();
            } finally {
                assertAllReleased(shardRefs);
            }
        });
        ByteSizeValue decodeBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
            try {
                decode(blockFactory(limit), shardRefs, shards, positions).close();
            } finally {
                assertAllReleased(shardRefs);
            }
        });
        var limit = ByteSizeValue.ofBytes(randomLongBetween(createBreakLimit.getBytes() + 1, decodeBreakLimit.getBytes()));
        List<AbstractRefCounted> shardRefs = shardRefs(shardCount);
        Exception e = expectThrows(CircuitBreakingException.class, () -> decode(blockFactory(limit), shardRefs, shards, positions));
        assertThat(e.getMessage(), equalTo("over test limit"));
        logger.info("break position", e);
        assertAllReleased(shardRefs);
    }

    /**
     * Decodes one row for each entry in {@code shards} and closes the builder if that fails,
     * like {@link TopNOperator} does when it builds its output.
     */
    private ResultBuilderForDoc decode(BlockFactory blockFactory, List<AbstractRefCounted> shardRefs, int[] shards, int positions) {
        DocVectorEncoder encoder = new DocVectorEncoder(new IndexedByShardIdFromList<>(shardRefs));
        ResultBuilderForDoc builder = new ResultBuilderForDoc(blockFactory, encoder, positions);
        boolean success = false;
        try {
            for (int shard : shards) {
                // The encoded row doesn't count against the limit so the breaker only sees what the builder allocates
                try (BreakingBytesRefBuilder row = new BreakingBytesRefBuilder(new NoopCircuitBreaker(CircuitBreaker.REQUEST), "topn")) {
                    encoder.encodeInt(shard, row);
                    encoder.encodeInt(randomNonNegativeInt(), row);
                    encoder.encodeInt(randomNonNegativeInt(), row);
                    builder.decodeValue(row.bytesRefView());
                }
            }
            success = true;
            return builder;
        } finally {
            if (success == false) {
                builder.close();
            }
        }
    }

    /**
     * Asserts that the breakers are empty and that every shard is back to the single ref it started with.
     */
    private void assertAllReleased(List<AbstractRefCounted> shardRefs) throws Exception {
        allBreakersEmpty();
        assertThat(refCounts(shardRefs), everyItem(equalTo(1)));
    }

    private static List<AbstractRefCounted> shardRefs(int shardCount) {
        return Stream.generate(() -> AbstractRefCounted.of(() -> {})).limit(shardCount).toList();
    }

    private static int[] randomShards(int shardCount) {
        return randomShards(shardCount, between(1, 100));
    }

    private static int[] randomShards(int shardCount, int rows) {
        return IntStream.range(0, rows).map(r -> between(0, shardCount - 1)).toArray();
    }

    private static List<Integer> refCounts(List<AbstractRefCounted> shardRefs) {
        return shardRefs.stream().map(AbstractRefCounted::refCount).toList();
    }
}
