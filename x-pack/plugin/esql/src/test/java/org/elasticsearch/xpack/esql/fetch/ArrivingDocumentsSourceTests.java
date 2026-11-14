/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The fetch contexts that the coordinator learns of from the documents that reach it.
 */
public class ArrivingDocumentsSourceTests extends ComputeTestCase {
    private static final DocOrigin LOCAL = new DocOrigin("", "n1", new ShardId("logs", "uuid", 0), new ShardSearchContextId("s", 1));
    private static final DocOrigin REMOTE = new DocOrigin("remote", "n1", new ShardId("logs", "uuid", 1), new ShardSearchContextId("s", 2));
    private static final DocOrigin UNREAD = new DocOrigin("", "n2", new ShardId("logs", "uuid", 2), new ShardSearchContextId("s", 3));

    private final RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
    private final QueuedSource delegate = new QueuedSource();

    /**
     * A data node keeps every context the origins of a page name, also one that no row of the page reads, so the execution
     * holds every one of them.
     */
    public void testRecordsEveryContextThePageNames() {
        FetchContextLease lease = lease();
        ArrivingDocumentsSource source = new ArrivingDocumentsSource(
            delegate,
            lease.newExecution(),
            ArrivingDocumentsSource::nodeInItsCluster
        );
        Page page = detachedPage(blockFactory(), List.of(LOCAL, REMOTE, UNREAD), new int[] { 1, 0, 1 });
        delegate.pages.add(page);

        assertThat(source.pollPage(), sameInstance(page));
        page.releaseBlocks();
        lease.close();

        assertThat(
            transport.frees,
            containsInAnyOrder(
                new RecordingFetchContextTransport.Sent(FetchNode.local("n1"), List.of(LOCAL.contextId())),
                new RecordingFetchContextTransport.Sent(new FetchNode("remote", "n1"), List.of(REMOTE.contextId())),
                new RecordingFetchContextTransport.Sent(FetchNode.local("n2"), List.of(UNREAD.contextId()))
            )
        );
    }

    /**
     * The coordinator of a remote cluster reaches its own data nodes as local nodes, whatever alias the querying cluster
     * gave it.
     */
    public void testTheCoordinatorOfARemoteClusterReachesItsDataNodesLocally() {
        FetchContextLease lease = lease();
        ArrivingDocumentsSource source = new ArrivingDocumentsSource(
            delegate,
            lease.newExecution(),
            ArrivingDocumentsSource::nodeInThisCluster
        );
        Page page = detachedPage(blockFactory(), List.of(REMOTE), new int[] { 0 });
        delegate.pages.add(page);

        source.pollPage().releaseBlocks();
        lease.close();

        assertThat(
            transport.frees,
            containsInAnyOrder(new RecordingFetchContextTransport.Sent(FetchNode.local("n1"), List.of(REMOTE.contextId())))
        );
    }

    public void testAPageWithoutDocumentsPassesThrough() {
        FetchContextLease lease = lease();
        ArrivingDocumentsSource source = new ArrivingDocumentsSource(
            delegate,
            lease.newExecution(),
            ArrivingDocumentsSource::nodeInItsCluster
        );
        Page page = new Page(blockFactory().newConstantIntBlockWith(1, 3));
        delegate.pages.add(page);

        assertThat(source.pollPage(), sameInstance(page));
        page.releaseBlocks();
        lease.close();

        assertThat(transport.frees, empty());
    }

    /**
     * Documents of this node carry no origins. They name no context the node keeps.
     */
    public void testDocumentsWithoutOriginsNameNoContext() {
        FetchContextLease lease = lease();
        ArrivingDocumentsSource source = new ArrivingDocumentsSource(
            delegate,
            lease.newExecution(),
            ArrivingDocumentsSource::nodeInItsCluster
        );
        BlockFactory blockFactory = blockFactory();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 1)) {
            builder.appendShard(0).appendSegment(0).appendDoc(0);
            delegate.pages.add(new Page(builder.shardRefCounters(AlwaysReferencedIndexedByShardId.INSTANCE).build()));
        }

        source.pollPage().releaseBlocks();
        lease.close();

        assertThat(transport.frees, empty());
    }

    public void testNoPageNoContext() {
        FetchContextLease lease = lease();
        ArrivingDocumentsSource source = new ArrivingDocumentsSource(
            delegate,
            lease.newExecution(),
            ArrivingDocumentsSource::nodeInItsCluster
        );

        assertThat(source.pollPage(), nullValue());
        lease.close();

        assertThat(transport.frees, empty());
    }

    private FetchContextLease lease() {
        CancellableTask task = new CancellableTask(1, "transport", "indices:data/read/esql", "", TaskId.EMPTY_TASK_ID, Map.of());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        return new FetchContextLease(
            new FetchContextLeases(null, null, null, null),
            transport,
            task,
            threadContext.newRestorableContext(false),
            TimeValue.timeValueMinutes(5)
        );
    }

    private static Page detachedPage(BlockFactory blockFactory, List<DocOrigin> origins, int[] ordinalOfRow) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, ordinalOfRow.length)) {
            for (int p = 0; p < ordinalOfRow.length; p++) {
                builder.appendShard(ordinalOfRow[p]).appendSegment(0).appendDoc(p);
            }
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins))));
        }
    }

    /**
     * Hands out the pages a test queued, one per poll.
     */
    private static class QueuedSource implements ExchangeSource {
        private final Deque<Page> pages = new ArrayDeque<>();

        @Override
        public Page pollPage() {
            return pages.poll();
        }

        @Override
        public void finish() {}

        @Override
        public boolean isFinished() {
            return pages.isEmpty();
        }

        @Override
        public int bufferSize() {
            return pages.size();
        }

        @Override
        public IsBlockedResult waitForReading() {
            return Operator.NOT_BLOCKED;
        }
    }
}
