/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.Releasables;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the {@link DistinctByOperator} built by {@link DistinctByOperator.BytesRefKeyPerShardFactory}, which treats equal keys
 * from different shards as distinct. The {@link OperatorTestCase} harness sends all pages of one shard and then all pages of
 * another, with the same keys in both.
 */
public class PerShardDistinctByOperatorTests extends OperatorTestCase {
    private static final int DOC_CHANNEL = 0;
    private static final int KEY_CHANNEL = 1;

    @Override
    protected DistinctByOperator.BytesRefKeyPerShardFactory simple(SimpleOptions options) {
        return new DistinctByOperator.BytesRefKeyPerShardFactory(DOC_CHANNEL, KEY_CHANNEL);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SourceOperator() {
            private static final int PAGE_SIZE = 100;
            private final int rowsPerShard = size * 2;
            private int position = 0;

            @Override
            public void finish() {
                position = rowsPerShard * 2;
            }

            @Override
            public boolean isFinished() {
                return position >= rowsPerShard * 2;
            }

            @Override
            public Page getOutput() {
                if (isFinished()) {
                    return null;
                }
                int shard = position / rowsPerShard;
                int shardPosition = position % rowsPerShard;
                int pageSize = Math.min(PAGE_SIZE, rowsPerShard - shardPosition);
                String[] keys = new String[pageSize];
                for (int i = 0; i < pageSize; i++) {
                    // Keys repeat within a shard: 0, 1, ..., size-1, 0, 1, ... and every shard has the same keys
                    keys[i] = String.valueOf((shardPosition + i) % size);
                }
                position += pageSize;
                return singleShardPage(blockFactory, shard, keys);
            }

            @Override
            public void close() {}
        };
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("DistinctByOperator[docChannel=0, keyChannel=1, factory=BytesRefKeyPerShardFactory]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return startsWith("DistinctByOperator[keyChannel=1, processor=BytesRefPerShardProcessor[shard=");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<Row> uniqueRows = new HashSet<>(rows(input));
        List<Row> output = rows(results);
        assertThat(Set.copyOf(output), equalTo(uniqueRows));
        assertThat("each key passes once per shard", output.size(), equalTo(uniqueRows.size()));
    }

    public void testSameKeyInDifferentShardsIsKept() {
        List<Row> output = run(singleShardPage(0, "a", "b"), singleShardPage(1, "a", "b"));
        assertThat(output, equalTo(List.of(new Row(0, "a"), new Row(0, "b"), new Row(1, "a"), new Row(1, "b"))));
    }

    public void testRepeatedKeyInTheSameShardIsDropped() {
        List<Row> output = run(singleShardPage(0, "a", "b", "a"), singleShardPage(0, "b", "c"));
        assertThat(output, equalTo(List.of(new Row(0, "a"), new Row(0, "b"), new Row(0, "c"))));
    }

    /** Only the latest shard's keys are kept, so a key passes again when its shard comes back. */
    public void testKeyPassesAgainWhenItsShardComesBack() {
        List<Row> output = run(singleShardPage(0, "a"), singleShardPage(1, "a"), singleShardPage(0, "a"));
        assertThat(output, equalTo(List.of(new Row(0, "a"), new Row(1, "a"), new Row(0, "a"))));
    }

    public void testPageWithRowsFromSeveralShards() {
        Page page = mixedShardPage(new int[] { 0, 0, 1, 1, 1, 0 }, "a", "a", "a", "b", "a", "a");
        List<Row> output = run(page);
        assertThat(output, equalTo(List.of(new Row(0, "a"), new Row(1, "a"), new Row(1, "b"), new Row(0, "a"))));
    }

    private record Row(int shard, String key) {}

    private List<Row> run(Page... pages) {
        List<Page> output = new ArrayList<>();
        try (Operator operator = simple().get(driverContext())) {
            for (Page page : pages) {
                operator.addInput(page);
                Page out = operator.getOutput();
                if (out != null) {
                    output.add(out);
                }
            }
        }
        try {
            return rows(output);
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    private static List<Row> rows(List<Page> pages) {
        List<Row> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            DocBlock docBlock = page.getBlock(DOC_CHANNEL);
            IntVector shards = docBlock.asVector().shards();
            BytesRefBlock keys = page.getBlock(KEY_CHANNEL);
            for (int p = 0; p < page.getPositionCount(); p++) {
                rows.add(new Row(shards.getInt(p), keys.getBytesRef(keys.getFirstValueIndex(p), scratch).utf8ToString()));
            }
        }
        return rows;
    }

    private Page singleShardPage(int shard, String... keys) {
        return singleShardPage(driverContext().blockFactory(), shard, keys);
    }

    /** Builds a page like the sources do, with a constant shard vector. */
    private static Page singleShardPage(BlockFactory blockFactory, int shard, String... keys) {
        IntVector shards = blockFactory.newConstantIntVector(shard, keys.length);
        return page(blockFactory, shards, keys);
    }

    private Page mixedShardPage(int[] shardOfRow, String... keys) {
        BlockFactory blockFactory = driverContext().blockFactory();
        IntVector shards = blockFactory.newIntArrayVector(shardOfRow, shardOfRow.length);
        return page(blockFactory, shards, keys);
    }

    private static Page page(BlockFactory blockFactory, IntVector shards, String... keys) {
        IntVector segments = null;
        IntVector docs = null;
        BytesRefBlock keyBlock = null;
        boolean success = false;
        try {
            segments = blockFactory.newConstantIntVector(0, keys.length);
            try (IntVector.FixedBuilder docBuilder = blockFactory.newIntVectorFixedBuilder(keys.length)) {
                for (int p = 0; p < keys.length; p++) {
                    docBuilder.appendInt(p);
                }
                docs = docBuilder.build();
            }
            try (BytesRefBlock.Builder keyBuilder = blockFactory.newBytesRefBlockBuilder(keys.length)) {
                for (String key : keys) {
                    keyBuilder.appendBytesRef(new BytesRef(key));
                }
                keyBlock = keyBuilder.build();
            }
            DocBlock docBlock = new DocVector(AlwaysReferencedIndexedByShardId.INSTANCE, shards, segments, docs, DocVector.config())
                .asBlock();
            success = true;
            return new Page(docBlock, keyBlock);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(shards, segments, docs, keyBlock);
            }
        }
    }
}
