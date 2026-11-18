/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The contexts that the rows leaving the coordinator of a remote cluster read, which it hands over to the querying cluster.
 */
public class LeavingDocumentsSinkTests extends ComputeTestCase {
    private static final DocOrigin FIRST = origin("r1", 0);
    private static final DocOrigin SECOND = origin("r2", 1);
    private static final DocOrigin UNREAD = origin("r2", 2);

    private final CollectingSink delegate = new CollectingSink();
    private final Set<ShardSearchContextId> read = ConcurrentCollections.newConcurrentSet();
    private final LeavingDocumentsSink sink = new LeavingDocumentsSink(delegate, read);

    /**
     * Only the contexts that rows read count. A page may name an origin no row reads, like a page that a filter or a limit
     * sliced, and the querying cluster never fetches from it.
     */
    public void testRecordsTheContextsThatTheRowsRead() {
        Page page = detachedPage(blockFactory(), List.of(FIRST, SECOND, UNREAD), new int[] { 1, 0, 1 });

        sink.addPage(page);

        assertThat(read, equalTo(Set.of(FIRST.contextId(), SECOND.contextId())));
        assertThat(delegate.pages, hasSize(1));
        assertThat(delegate.pages.getFirst(), sameInstance(page));
        delegate.release();
    }

    /**
     * The contexts of every page add up.
     */
    public void testAddsUpThePagesOfEveryDriver() {
        sink.addPage(detachedPage(blockFactory(), List.of(FIRST), new int[] { 0 }));
        sink.addPage(detachedPage(blockFactory(), List.of(SECOND), new int[] { 0, 0 }));

        assertThat(read, equalTo(Set.of(FIRST.contextId(), SECOND.contextId())));
        delegate.release();
    }

    public void testAPageWithoutDocumentsPassesThrough() {
        Page page = new Page(blockFactory().newConstantIntBlockWith(1, 3));

        sink.addPage(page);

        assertThat(read, empty());
        assertThat(delegate.pages.getFirst(), sameInstance(page));
        delegate.release();
    }

    /**
     * A row of this node would name a reader slot that means nothing on the querying cluster. The sink refuses its page,
     * and releases it, since it took it.
     */
    public void testDocumentsWithoutTheirOriginsAreRefused() {
        BlockFactory blockFactory = blockFactory();
        Page page;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 1)) {
            builder.appendShard(0).appendSegment(0).appendDoc(0);
            page = new Page(builder.shardRefCounters(AlwaysReferencedIndexedByShardId.INSTANCE).build());
        }

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> sink.addPage(page));

        assertThat(e.getMessage(), containsString("without their origins"));
        assertThat(delegate.pages, empty());
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    private static Page detachedPage(BlockFactory blockFactory, List<DocOrigin> origins, int[] ordinalOfRow) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, ordinalOfRow.length)) {
            for (int p = 0; p < ordinalOfRow.length; p++) {
                builder.appendShard(ordinalOfRow[p]).appendSegment(0).appendDoc(p);
            }
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins))));
        }
    }

    private static DocOrigin origin(String nodeId, int shard) {
        return new DocOrigin("remote", nodeId, new ShardId("logs", "uuid", shard), new ShardSearchContextId("session", shard));
    }

    private static class CollectingSink implements ExchangeSink {
        private final List<Page> pages = new ArrayList<>();

        @Override
        public void addPage(Page page) {
            pages.add(page);
        }

        void release() {
            pages.forEach(Page::releaseBlocks);
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
}
