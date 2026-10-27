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
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.test.BreakerTestUtil;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests that {@link ResultBuilderForDoc} releases every shard ref it takes. Decoding takes one ref per row.
 * The {@link DocBlock} that {@link ResultBuilderForDoc#build} returns adopts them. If that block is never
 * built then closing the builder must release them.
 * <p>
 * Rows that left their node pin no shard. For those the tests check how the builder numbers their origins for the block,
 * and that it charges and releases that memory on the breaker.
 */
public class ResultBuilderForDocTests extends ComputeTestCase {
    /**
     * Large, so the memory for the block ordinals clearly follows the origins a block references rather than the highest
     * ordinal of the table.
     */
    private static final int LARGE_TABLE_ORIGIN_COUNT = 10_000;

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
     * Rows that reference a few origins with high numbers in a large table get dense block ordinals in the order they
     * first appear, and the block names only those origins.
     */
    public void testDetachedSparseTableOrdinalsBecomeDenseBlockOrdinals() {
        try (DocOriginTable table = tableOf(LARGE_TABLE_ORIGIN_COUNT)) {
            int[] referenced = distinctTableOrdinals(LARGE_TABLE_ORIGIN_COUNT, between(1, 5));
            int[] tableOrdinals = IntStream.range(0, between(1, 100)).map(r -> referenced[between(0, referenced.length - 1)]).toArray();
            Block block;
            try (ResultBuilderForDoc builder = decode(blockFactory(), new DocVectorEncoder(table), tableOrdinals, tableOrdinals.length)) {
                block = builder.build();
            }
            try (block) {
                DocVector vector = ((DocBlock) block).asVector();
                DocOrigins blockOrigins = vector.origins();
                List<DocOrigin> firstReferenced = IntStream.of(tableOrdinals).distinct().mapToObj(table::get).toList();
                assertThat(IntStream.range(0, blockOrigins.size()).mapToObj(blockOrigins::get).toList(), equalTo(firstReferenced));
                assertThat(
                    IntStream.range(0, tableOrdinals.length).mapToObj(vector::origin).toList(),
                    equalTo(IntStream.of(tableOrdinals).mapToObj(table::get).toList())
                );
            }
        }
    }

    /**
     * The block ordinals are charged to the breaker, and their charge follows the origins the rows reference. A row from
     * the last origin of a large table costs much less than a slot for every origin of the table.
     */
    public void testDetachedBlockOrdinalsChargeByReferencedOrigins() throws Exception {
        List<AbstractRefCounted> shardRefs = shardRefs(1);
        BlockFactory attachedBlockFactory = blockFactory();
        long attachedBytes;
        try (ResultBuilderForDoc builder = decode(attachedBlockFactory, shardRefs, new int[] { 0 }, 1)) {
            attachedBytes = attachedBlockFactory.breaker().getUsed();
        }
        BlockFactory detachedBlockFactory = blockFactory();
        long detachedBytes;
        try (
            DocOriginTable table = tableOf(LARGE_TABLE_ORIGIN_COUNT);
            ResultBuilderForDoc builder = decode(
                detachedBlockFactory,
                new DocVectorEncoder(table),
                new int[] { LARGE_TABLE_ORIGIN_COUNT - 1 },
                1
            )
        ) {
            detachedBytes = detachedBlockFactory.breaker().getUsed();
        }
        long blockOrdinalBytes = detachedBytes - attachedBytes;
        assertThat("the block ordinals are charged", blockOrdinalBytes, greaterThan(0L));
        assertThat(blockOrdinalBytes, lessThan((long) Integer.BYTES * LARGE_TABLE_ORIGIN_COUNT));
        assertAllReleased(shardRefs);
    }

    /**
     * {@link ResultBuilderForDoc#estimatedBytes} covers the dictionary of origins the built block charges to the breaker.
     * {@link TopNOperator} splits its output on that estimate, and the dictionary is most of a block whose rows come from
     * many origins. The rest of the estimate is the same as for rows of this node, so the difference is the dictionary.
     */
    public void testDetachedEstimatedBytesCoversOrigins() throws Exception {
        int originCount = between(1, 1000);
        int[] tableOrdinals = IntStream.range(0, between(1, 100)).map(r -> between(0, originCount - 1)).toArray();
        List<AbstractRefCounted> shardRefs = shardRefs(1);
        long attachedEstimatedBytes;
        try (ResultBuilderForDoc builder = decode(blockFactory(), shardRefs, new int[tableOrdinals.length], tableOrdinals.length)) {
            attachedEstimatedBytes = builder.estimatedBytes();
        }
        long detachedEstimatedBytes;
        Block block;
        try (
            DocOriginTable table = tableOf(originCount);
            ResultBuilderForDoc builder = decode(blockFactory(), new DocVectorEncoder(table), tableOrdinals, tableOrdinals.length)
        ) {
            detachedEstimatedBytes = builder.estimatedBytes();
            block = builder.build();
        }
        try (block) {
            DocOrigins blockOrigins = ((DocBlock) block).asVector().origins();
            assertThat(detachedEstimatedBytes - attachedEstimatedBytes, equalTo(blockOrigins.ramBytesUsed()));
        }
        assertAllReleased(shardRefs);
    }

    /**
     * {@link TopNOperator} closes the builder without building it when decoding or building another column fails.
     */
    public void testDetachedCloseWithoutBuild() throws Exception {
        int originCount = between(1, 1000);
        try (DocOriginTable table = tableOf(originCount)) {
            int[] tableOrdinals = IntStream.range(0, between(1, 100)).map(r -> between(0, originCount - 1)).toArray();
            decode(blockFactory(), new DocVectorEncoder(table), tableOrdinals, tableOrdinals.length).close();
        }
        allBreakersEmpty();
    }

    /**
     * Trips the breaker while decoding more rows, from more origins, than the builder was sized for, so both the
     * {@link DocBlock.Builder} and the block ordinals have to grow.
     */
    public void testDetachedDecodeBreaks() throws Exception {
        int originCount = 1000;
        try (DocOriginTable table = tableOf(originCount)) {
            DocVectorEncoder encoder = new DocVectorEncoder(table);
            // Decode more rows than the builder has room for so it has to grow. It always has room for at least two rows.
            int[] tableOrdinals = distinctTableOrdinals(originCount, between(3, 100));
            int positions = between(1, tableOrdinals.length - 1);
            ByteSizeValue createBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
                try {
                    decode(blockFactory(limit), encoder, new int[0], positions).close();
                } finally {
                    allBreakersEmpty();
                }
            });
            ByteSizeValue decodeBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
                try {
                    decode(blockFactory(limit), encoder, tableOrdinals, positions).close();
                } finally {
                    allBreakersEmpty();
                }
            });
            var limit = ByteSizeValue.ofBytes(randomLongBetween(createBreakLimit.getBytes() + 1, decodeBreakLimit.getBytes()));
            Exception e = expectThrows(
                CircuitBreakingException.class,
                () -> decode(blockFactory(limit), encoder, tableOrdinals, positions)
            );
            assertThat(e.getMessage(), equalTo("over test limit"));
            logger.info("break position", e);
        }
        allBreakersEmpty();
    }

    /**
     * Trips the breaker while building the {@link DocBlock}, after decoding every row. The {@link DocVector} charges
     * its dictionary of origins there.
     */
    public void testDetachedBuildBreaks() throws Exception {
        int originCount = between(1, 1000);
        try (DocOriginTable table = tableOf(originCount)) {
            DocVectorEncoder encoder = new DocVectorEncoder(table);
            int[] tableOrdinals = IntStream.range(0, between(1, 100)).map(r -> between(0, originCount - 1)).toArray();
            ByteSizeValue decodeBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
                try {
                    decode(blockFactory(limit), encoder, tableOrdinals, tableOrdinals.length).close();
                } finally {
                    allBreakersEmpty();
                }
            });
            ByteSizeValue buildBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
                try (ResultBuilderForDoc builder = decode(blockFactory(limit), encoder, tableOrdinals, tableOrdinals.length)) {
                    builder.build().close();
                } finally {
                    allBreakersEmpty();
                }
            });
            var limit = ByteSizeValue.ofBytes(randomLongBetween(decodeBreakLimit.getBytes() + 1, buildBreakLimit.getBytes()));
            try (ResultBuilderForDoc builder = decode(blockFactory(limit), encoder, tableOrdinals, tableOrdinals.length)) {
                Exception e = expectThrows(CircuitBreakingException.class, builder::build);
                assertThat(e.getMessage(), equalTo("over test limit"));
                logger.info("break position", e);
            }
        }
        allBreakersEmpty();
    }

    private ResultBuilderForDoc decode(BlockFactory blockFactory, List<AbstractRefCounted> shardRefs, int[] shards, int positions) {
        return decode(blockFactory, new DocVectorEncoder(new IndexedByShardIdFromList<>(shardRefs)), shards, positions);
    }

    /**
     * Decodes one row for each entry in {@code shards} and closes the builder if that fails,
     * like {@link TopNOperator} does when it builds its output. With an encoder that numbers
     * origins in a {@link DocOriginTable}, {@code shards} are ordinals of that table.
     */
    private ResultBuilderForDoc decode(BlockFactory blockFactory, DocVectorEncoder encoder, int[] shards, int positions) {
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

    /**
     * A table numbering {@code originCount} origins. It charges a breaker of its own, so the breakers of the test only see
     * what the builder charges.
     */
    private static DocOriginTable tableOf(int originCount) {
        DocOriginTable table = new DocOriginTable(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        int lastOrdinal = -1;
        while (lastOrdinal < originCount - 1) {
            // a random origin that repeats an earlier one gets its old ordinal, so this keeps going until the table is full
            lastOrdinal = table.ordinal(RandomBlock.randomDocOrigin());
        }
        return table;
    }

    private static int[] distinctTableOrdinals(int originCount, int count) {
        return randomSubsetOf(count, IntStream.range(0, originCount).boxed().toList()).stream().mapToInt(Integer::intValue).toArray();
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
