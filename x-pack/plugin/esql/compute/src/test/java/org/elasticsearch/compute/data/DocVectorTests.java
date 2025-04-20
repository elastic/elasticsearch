/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.BreakerTestUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DocVectorTests extends ComputeTestCase {
    public void testNonDecreasingSetTrue() {
        int length = between(1, 100);
        DocVector docs = new DocVector(intRange(0, length), intRange(0, length), intRange(0, length), true);
        assertTrue(docs.singleSegmentNonDecreasing());
    }

    public void testNonDecreasingSetFalse() {
        BlockFactory blockFactory = blockFactory();
        DocVector docs = new DocVector(intRange(0, 2), intRange(0, 2), blockFactory.newIntArrayVector(new int[] { 1, 0 }, 2), false);
        assertFalse(docs.singleSegmentNonDecreasing());
        docs.close();
    }

    public void testNonDecreasingNonConstantShard() {
        BlockFactory blockFactory = blockFactory();
        DocVector docs = new DocVector(intRange(0, 2), blockFactory.newConstantIntVector(0, 2), intRange(0, 2), null);
        assertFalse(docs.singleSegmentNonDecreasing());
        docs.close();
    }

    public void testNonDecreasingNonConstantSegment() {
        BlockFactory blockFactory = blockFactory();
        DocVector docs = new DocVector(blockFactory.newConstantIntVector(0, 2), intRange(0, 2), intRange(0, 2), null);
        assertFalse(docs.singleSegmentNonDecreasing());
        docs.close();
    }

    public void testNonDecreasingDescendingDocs() {
        BlockFactory blockFactory = blockFactory();
        DocVector docs = new DocVector(
            blockFactory.newConstantIntVector(0, 2),
            blockFactory.newConstantIntVector(0, 2),
            blockFactory.newIntArrayVector(new int[] { 1, 0 }, 2),
            null
        );
        assertFalse(docs.singleSegmentNonDecreasing());
        docs.close();
    }

    public void testBuildBreaks() {
        var maxBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            BlockFactory blockFactory = blockFactory(limit);
            buildDocBlock(blockFactory).close();
        });
        var limit = ByteSizeValue.ofBytes(randomLongBetween(0, maxBreakLimit.getBytes()));
        BlockFactory blockFactory = blockFactory(limit);
        Exception e = expectThrows(CircuitBreakingException.class, () -> buildDocBlock(blockFactory).close());
        assertThat(e.getMessage(), equalTo("over test limit"));
        logger.info("break position", e);
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    private DocBlock buildDocBlock(BlockFactory blockFactory) {
        int size = 100;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, size)) {
            for (int r = 0; r < size; r++) {
                builder.appendShard(3 - r % 4);
                builder.appendSegment(r % 10);
                builder.appendDoc(size);
            }
            return builder.build();
        }
    }

    public void testShardSegmentDocMap() {
        assertShardSegmentDocMap(
            new int[][] {
                new int[] { 1, 0, 0 },
                new int[] { 1, 1, 1 },
                new int[] { 1, 1, 0 },
                new int[] { 0, 0, 2 },
                new int[] { 0, 1, 1 },
                new int[] { 0, 1, 0 },
                new int[] { 0, 2, 1 },
                new int[] { 0, 2, 0 },
                new int[] { 0, 2, 2 },
                new int[] { 0, 2, 3 }, },
            new int[][] {
                new int[] { 0, 0, 2 },
                new int[] { 0, 1, 0 },
                new int[] { 0, 1, 1 },
                new int[] { 0, 2, 0 },
                new int[] { 0, 2, 1 },
                new int[] { 0, 2, 2 },
                new int[] { 0, 2, 3 },
                new int[] { 1, 0, 0 },
                new int[] { 1, 1, 0 },
                new int[] { 1, 1, 1 }, }
        );
    }

    public void testRandomShardSegmentDocMap() {
        int[][] tracker = new int[5][];
        for (int shard = 0; shard < 5; shard++) {
            tracker[shard] = new int[] { 0, 0, 0, 0, 0 };
        }
        List<int[]> data = new ArrayList<>();
        for (int r = 0; r < 10000; r++) {
            int shard = between(0, 4);
            int segment = between(0, 4);
            data.add(new int[] { shard, segment, tracker[shard][segment]++ });
        }
        Randomness.shuffle(data);

        List<int[]> sorted = new ArrayList<>(data);
        Collections.sort(sorted, Comparator.comparing((int[] r) -> r[0]).thenComparing(r -> r[1]).thenComparing(r -> r[2]));
        assertShardSegmentDocMap(data.toArray(int[][]::new), sorted.toArray(int[][]::new));
    }

    private void assertShardSegmentDocMap(int[][] data, int[][] expected) {
        BlockFactory blockFactory = blockFactory();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, data.length)) {
            for (int r = 0; r < data.length; r++) {
                builder.appendShard(data[r][0]);
                builder.appendSegment(data[r][1]);
                builder.appendDoc(data[r][2]);
            }
            try (DocVector docVector = builder.build().asVector()) {
                assertThat(blockFactory.breaker().getUsed(), equalTo(docVector.ramBytesUsed()));
                int[] forwards = docVector.shardSegmentDocMapForwards();
                assertThat(blockFactory.breaker().getUsed(), equalTo(docVector.ramBytesUsed()));

                int[][] result = new int[docVector.getPositionCount()][];
                for (int p = 0; p < result.length; p++) {
                    result[p] = new int[] {
                        docVector.shards().getInt(forwards[p]),
                        docVector.segments().getInt(forwards[p]),
                        docVector.docs().getInt(forwards[p]) };
                }
                assertThat(result, equalTo(expected));

                int[] backwards = docVector.shardSegmentDocMapBackwards();
                for (int p = 0; p < result.length; p++) {
                    result[p] = new int[] {
                        docVector.shards().getInt(backwards[forwards[p]]),
                        docVector.segments().getInt(backwards[forwards[p]]),
                        docVector.docs().getInt(backwards[forwards[p]]) };
                }

                assertThat(result, equalTo(data));
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testShardSegmentDocMapBreaks() {
        ByteSizeValue buildBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            BlockFactory blockFactory = blockFactory(limit);
            buildDocBlock(blockFactory).close();
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        });
        ByteSizeValue docMapBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            BlockFactory blockFactory = blockFactory(limit);
            try (DocBlock docBlock = buildDocBlock(blockFactory)) {
                docBlock.asVector().shardSegmentDocMapForwards();
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        });
        var limit = ByteSizeValue.ofBytes(randomLongBetween(buildBreakLimit.getBytes() + 1, docMapBreakLimit.getBytes()));
        BlockFactory blockFactory = blockFactory(limit);
        testShardSegmentDocMapBreaks(blockFactory);
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    private void testShardSegmentDocMapBreaks(BlockFactory blockFactory) {
        int size = 100;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, size)) {
            for (int r = 0; r < size; r++) {
                builder.appendShard(3 - r % 4);
                builder.appendSegment(r % 10);
                builder.appendDoc(size);
            }
            try (DocBlock docBlock = builder.build()) {
                Exception e = expectThrows(CircuitBreakingException.class, docBlock.asVector()::shardSegmentDocMapForwards);
                assertThat(e.getMessage(), equalTo("over test limit"));
                logger.info("broke at", e);
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testCannotDoubleRelease() {
        BlockFactory blockFactory = blockFactory();
        var block = new DocVector(intRange(0, 2), blockFactory.newConstantIntBlockWith(0, 2).asVector(), intRange(0, 2), null).asBlock();
        assertThat(block.isReleased(), is(false));
        Page page = new Page(block);

        Releasables.closeExpectNoException(block);
        assertThat(block.isReleased(), is(true));

        Exception e = expectThrows(IllegalStateException.class, () -> block.close());
        assertThat(e.getMessage(), containsString("can't release already released object"));

        e = expectThrows(IllegalStateException.class, () -> page.getBlock(0));
        assertThat(e.getMessage(), containsString("can't read released block"));

        e = expectThrows(IllegalArgumentException.class, () -> new Page(block));
        assertThat(e.getMessage(), containsString("can't build page out of released blocks"));
    }

    public void testRamBytesUsedWithout() {
        BlockFactory blockFactory = blockFactory();
        DocVector docs = new DocVector(
            blockFactory.newConstantIntBlockWith(0, 1).asVector(),
            blockFactory.newConstantIntBlockWith(0, 1).asVector(),
            blockFactory.newConstantIntBlockWith(0, 1).asVector(),
            false
        );
        assertThat(docs.singleSegmentNonDecreasing(), is(false));
        docs.ramBytesUsed(); // ensure non-singleSegmentNonDecreasing handles nulls in ramByteUsed
        docs.close();
    }

    public void testFilter() {
        BlockFactory factory = blockFactory();
        try (
            DocVector docs = new DocVector(
                factory.newConstantIntVector(0, 10),
                factory.newConstantIntVector(0, 10),
                factory.newIntArrayVector(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, 10),
                false
            );
            DocVector filtered = docs.filter(1, 2, 3);
            DocVector expected = new DocVector(
                factory.newConstantIntVector(0, 3),
                factory.newConstantIntVector(0, 3),
                factory.newIntArrayVector(new int[] { 1, 2, 3 }, 3),
                false
            );
        ) {
            assertThat(filtered, equalTo(expected));
        }
    }

    public void testFilterBreaks() {
        Function<BlockFactory, DocVector> buildDocVector = factory -> {
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            DocVector result = null;
            try {
                shards = factory.newConstantIntVector(0, 10);
                segments = factory.newConstantIntVector(0, 10);
                docs = factory.newConstantIntVector(0, 10);
                result = new DocVector(shards, segments, docs, false);
                return result;
            } finally {
                if (result == null) {
                    Releasables.close(shards, segments, docs);
                }
            }
        };
        ByteSizeValue buildBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            BlockFactory factory = blockFactory(limit);
            buildDocVector.apply(factory).close();
        });
        ByteSizeValue filterBreakLimit = BreakerTestUtil.findBreakerLimit(ByteSizeValue.ofMb(128), limit -> {
            BlockFactory factory = blockFactory(limit);
            try (DocVector docs = buildDocVector.apply(factory)) {
                docs.filter(1, 2, 3).close();
            }
        });
        ByteSizeValue limit = ByteSizeValue.ofBytes(randomLongBetween(buildBreakLimit.getBytes() + 1, filterBreakLimit.getBytes()));
        BlockFactory factory = blockFactory(limit);
        try (DocVector docs = buildDocVector.apply(factory)) {
            Exception e = expectThrows(CircuitBreakingException.class, () -> docs.filter(1, 2, 3));
            assertThat(e.getMessage(), equalTo("over test limit"));
        }
    }

    IntVector intRange(int startInclusive, int endExclusive) {
        return IntVector.range(startInclusive, endExclusive, TestBlockFactory.getNonBreakingInstance());
    }
}
