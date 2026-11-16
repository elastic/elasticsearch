/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.RefCounted;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class DetachingExchangeSinkTests extends ComputeTestCase {
    private final CountingRefCounted[] shards = { new CountingRefCounted(), new CountingRefCounted(), new CountingRefCounted() };
    private final List<DocOrigin> origins = RandomBlock.randomDocOrigins(3);

    public void testDetachesTheDocumentsAndPassesTheOtherBlocks() {
        BlockFactory blockFactory = blockFactory();
        int[] rowShards = { 2, 0, 2 };
        LongBlock other = blockFactory.newConstantLongBlockWith(randomLong(), rowShards.length);
        CollectingSink sink = new CollectingSink();
        new DetachingExchangeSink(sink, origins::get).addPage(new Page(docBlock(blockFactory, rowShards), other));
        try (Page out = sink.only()) {
            DocBlock docs = out.getBlock(0);
            assertThat(docs.asVector().origins().size(), equalTo(2));
            for (int p = 0; p < rowShards.length; p++) {
                assertThat(docs.asVector().origin(p), equalTo(origins.get(rowShards[p])));
            }
            assertThat(out.getBlock(1), sameInstance(other));
            for (CountingRefCounted shard : shards) {
                assertThat("the page released its shards while its rows live on", shard.count.get(), equalTo(1));
            }
        }
    }

    /**
     * The pages in the sink go to another driver, here or on another node. The detached documents are a new block that
     * the sink operator never prepared for that, so the sink prepares it.
     */
    public void testDetachedDocumentsCanPassToAnotherDriver() {
        BlockFactory parentFactory = blockFactory();
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(parentFactory.breaker(), 1024, 1024);
        BlockFactory driverFactory = parentFactory.newChildFactory(localBreaker);
        try {
            CollectingSink sink = new CollectingSink();
            new DetachingExchangeSink(sink, origins::get).addPage(new Page(docBlock(driverFactory, new int[] { 0, 1 })));
            try (Page out = sink.only()) {
                DocBlock docs = out.getBlock(0);
                assertThat(docs.blockFactory(), sameInstance(parentFactory));
                assertThat(docs.asVector().shards().blockFactory(), sameInstance(parentFactory));
            }
        } finally {
            localBreaker.close();
        }
    }

    public void testPagesWithoutDocumentsOfThisNodePassThrough() {
        BlockFactory blockFactory = blockFactory();
        CollectingSink sink = new CollectingSink();
        DetachingExchangeSink detaching = new DetachingExchangeSink(sink, slot -> { throw new AssertionError("no row of this node"); });
        Page values = new Page(blockFactory.newConstantLongBlockWith(randomLong(), 3));
        Page detached = new Page(RandomBlock.randomDetachedDocBlock(blockFactory, 3, 2).block());
        detaching.addPage(values);
        detaching.addPage(detached);
        assertThat(sink.pages, hasSize(2));
        assertThat(sink.pages.get(0), sameInstance(values));
        assertThat(sink.pages.get(1), sameInstance(detached));
        sink.pages.forEach(Page::releaseBlocks);
    }

    /**
     * An empty page still has to lose its rows of this node, which can't be written to the wire.
     */
    public void testEmptyPageKeepsItsBatchMetadata() {
        BlockFactory blockFactory = blockFactory();
        BatchMetadata metadata = new BatchMetadata(7, 3, true);
        CollectingSink sink = new CollectingSink();
        new DetachingExchangeSink(sink, origins::get).addPage(new Page(metadata, docBlock(blockFactory, new int[0])));
        try (Page out = sink.only()) {
            assertThat(out.getPositionCount(), equalTo(0));
            assertThat(out.<DocBlock>getBlock(0).asVector().origins().size(), equalTo(0));
            assertThat(out.batchMetadata(), equalTo(metadata));
        }
    }

    public void testBatchMarkerPassesThrough() {
        CollectingSink sink = new CollectingSink();
        Page marker = Page.createBatchMarkerPage(7, 3);
        new DetachingExchangeSink(sink, origins::get).addPage(marker);
        assertThat(sink.only(), sameInstance(marker));
    }

    public void testBreakerTripReleasesThePage() {
        int[] rowShards = { 0, 1, 2, 0 };
        testWithCrankyBlockFactory(cranky -> {
            CollectingSink sink = new CollectingSink();
            DocBlock docs = docBlock(cranky, rowShards);
            LongBlock other = null;
            try {
                other = cranky.newConstantLongBlockWith(1, rowShards.length);
            } finally {
                if (other == null) {
                    docs.close();
                }
            }
            new DetachingExchangeSink(sink, origins::get).addPage(new Page(docs, other));
            sink.only().releaseBlocks();
        });
        for (CountingRefCounted shard : shards) {
            assertThat(shard.count.get(), equalTo(1));
        }
    }

    private DocBlock docBlock(BlockFactory blockFactory, int[] rowShards) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, rowShards.length)) {
            for (int p = 0; p < rowShards.length; p++) {
                builder.appendShard(rowShards[p]).appendSegment(0).appendDoc(p);
            }
            return builder.shardRefCounters(new IndexedByShardIdFromList<>(Arrays.asList(shards))).build();
        }
    }

    private static class CollectingSink implements ExchangeSink {
        private final List<Page> pages = new ArrayList<>();

        @Override
        public void addPage(Page page) {
            pages.add(page);
        }

        Page only() {
            assertThat(pages, hasSize(1));
            return pages.getFirst();
        }

        @Override
        public void finish() {}

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {}

        @Override
        public IsBlockedResult waitForWriting() {
            return Operator.NOT_BLOCKED;
        }
    }

    /**
     * Starts with the one reference its owner holds, like the shard contexts of a data node.
     */
    private static class CountingRefCounted implements RefCounted {
        final AtomicInteger count = new AtomicInteger(1);

        @Override
        public void incRef() {
            count.incrementAndGet();
        }

        @Override
        public boolean tryIncRef() {
            count.incrementAndGet();
            return true;
        }

        @Override
        public boolean decRef() {
            return count.decrementAndGet() == 0;
        }

        @Override
        public boolean hasReferences() {
            return count.get() > 0;
        }
    }
}
