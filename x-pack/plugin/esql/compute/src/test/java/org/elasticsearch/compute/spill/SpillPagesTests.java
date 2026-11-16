/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The spill page codec keeps document references attached: a page read back on the node takes a reference on each shard
 * its rows name and releases it when it closes, and a run read without the shard references refuses such a page.
 */
public class SpillPagesTests extends ComputeTestCase {

    /** Shard references that count what is taken and given back. */
    private static final class Shards implements IndexedByShardId<RefCounted> {
        final List<AbstractRefCounted> shards = new ArrayList<>();

        Shards(int count) {
            for (int i = 0; i < count; i++) {
                shards.add(AbstractRefCounted.of(() -> {}));
            }
        }

        @Override
        public RefCounted get(int shardId) {
            return shards.get(shardId);
        }

        @Override
        public Iterable<? extends RefCounted> iterable() {
            return shards;
        }

        @Override
        public int size() {
            return shards.size();
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<RefCounted, S> mapper) {
            throw new UnsupportedOperationException();
        }

        int references(int shard) {
            return shards.get(shard).refCount();
        }
    }

    private static Page docPage(BlockFactory blockFactory, Shards shards, int[] shardOfRow, long[] values) {
        int size = shardOfRow.length;
        int[] segments = new int[size];
        int[] docs = new int[size];
        for (int i = 0; i < size; i++) {
            docs[i] = i;
        }
        DocVector vector = new DocVector(
            shards,
            blockFactory.newIntArrayVector(shardOfRow, size),
            blockFactory.newIntArrayVector(segments, size),
            blockFactory.newIntArrayVector(docs, size),
            DocVector.config()
        );
        return new Page(vector.asBlock(), blockFactory.newLongArrayVector(values, size).asBlock());
    }

    public void testDocumentsRoundTripAndTakeTheirReferences() throws IOException {
        BlockFactory blockFactory = blockFactory();
        Shards shards = new Shards(3);
        int[] shardOfRow = { 0, 2, 2, 0 };
        long[] values = { 10, 20, 30, 40 };
        Page page = docPage(blockFactory, shards, shardOfRow, values);
        assertThat("the page holds one reference per row on the row's shard", shards.references(0), equalTo(3));
        assertThat(shards.references(1), equalTo(1));
        assertThat(shards.references(2), equalTo(3));
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        SpillPages.write(page, out);
        page.releaseBlocks();
        assertThat("the page's references are gone with it", shards.references(0), equalTo(1));
        assertThat(shards.references(2), equalTo(1));

        try (BlockStreamInput in = new BlockStreamInput(out.bytes().streamInput(), blockFactory)) {
            in.setTransportVersion(TransportVersion.current());
            Page read = SpillPages.read(in, shards);
            try {
                assertThat(read.getPositionCount(), equalTo(4));
                DocVector docs = ((DocBlock) read.getBlock(0)).asVector();
                for (int i = 0; i < 4; i++) {
                    assertThat(docs.shards().getInt(i), equalTo(shardOfRow[i]));
                    assertThat(docs.docs().getInt(i), equalTo(i));
                    assertThat(((LongBlock) read.getBlock(1)).getLong(i), equalTo(values[i]));
                }
                assertThat("the page read back takes the references again", shards.references(0), equalTo(3));
                assertThat(shards.references(1), equalTo(1));
                assertThat(shards.references(2), equalTo(3));
            } finally {
                read.releaseBlocks();
            }
        }
        assertThat(shards.references(0), equalTo(1));
        assertThat(shards.references(2), equalTo(1));
    }

    public void testDocumentsNeedTheShardReferencesToComeBack() throws IOException {
        BlockFactory blockFactory = blockFactory();
        Shards shards = new Shards(1);
        Page page = docPage(blockFactory, shards, new int[] { 0, 0 }, new long[] { 1, 2 });
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        SpillPages.write(page, out);
        page.releaseBlocks();
        try (BlockStreamInput in = new BlockStreamInput(out.bytes().streamInput(), blockFactory)) {
            in.setTransportVersion(TransportVersion.current());
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> SpillPages.read(in, null));
            assertThat(e.getMessage(), containsString("without shard references"));
        }
        assertThat("nothing leaks from the refused read", shards.references(0), equalTo(1));
    }

    public void testAPageWithoutDocumentsReadsBackWithoutShardReferences() throws IOException {
        BlockFactory blockFactory = blockFactory();
        long[] values = { 5, 6, 7 };
        Page page = new Page(blockFactory.newLongArrayVector(values, 3).asBlock());
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        SpillPages.write(page, out);
        page.releaseBlocks();
        try (BlockStreamInput in = new BlockStreamInput(out.bytes().streamInput(), blockFactory)) {
            in.setTransportVersion(TransportVersion.current());
            Page read = SpillPages.read(in, null);
            try {
                assertThat(((LongBlock) read.getBlock(0)).getLong(2), equalTo(7L));
            } finally {
                read.releaseBlocks();
            }
        }
    }
}
