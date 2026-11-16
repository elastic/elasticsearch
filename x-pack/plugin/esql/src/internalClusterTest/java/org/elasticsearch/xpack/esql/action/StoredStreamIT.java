/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.StoredResultMetadata;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.StoredPage;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Streaming queries that store their result with {@code store=true}.
 */
public class StoredStreamIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireStoredResults() {
        assumeTrue("stored results are behind a feature flag", EsqlStreamResultsIndex.FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .build();
    }

    private int indexDocs() {
        int numDocs = between(100, 300);
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("v", "type=long"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            bulk.add(prepareIndex("test").setSource("v", i));
        }
        assertNoFailures(bulk.get());
        return numDocs;
    }

    private EsqlStreamQueryAction.ResultStream stream(boolean store, StreamQueryTestUtils.CountingStreamSubscriber subscriber) {
        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest("FROM test | KEEP v | LIMIT 10000");
        // small pages, so that a result is stored as many pages
        source.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.PAGE_SIZE.getKey(), between(1, 10)).build()));
        source.acceptedPragmaRisks(true);
        AtomicReference<EsqlStreamQueryAction.ResultStream> startRef = new AtomicReference<>();
        EsqlStreamQueryRequest request = new EsqlStreamQueryRequest(source, ActionListener.wrap(resultStream -> {
            startRef.set(resultStream);
            resultStream.publisher().subscribe(subscriber);
        }, subscriber.failure::set), false);
        if (store) {
            request.store(true);
        }
        ActionFuture<ActionResponse.Empty> future = client().execute(EsqlStreamQueryAction.INSTANCE, request);
        future.actionGet(TEST_REQUEST_TIMEOUT);
        safeAwait(subscriber.completed);
        assertThat(subscriber.failure.get(), nullValue());
        return startRef.get();
    }

    public void testNotStoredByDefault() {
        int numDocs = indexDocs();
        var subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        var resultStream = stream(false, subscriber);
        assertThat(subscriber.rowCount.get(), equalTo(numDocs));
        assertThat(resultStream.id(), nullValue());
        assertFalse(indexExists(EsqlStreamResultsIndex.INDEX));
    }

    public void testStoreRequiresBatchIndexing() {
        indexDocs();
        updateClusterSettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), false));
        try {
            EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest("FROM test | KEEP v");
            EsqlStreamQueryRequest request = new EsqlStreamQueryRequest(source, ActionListener.noop(), false).store(true);
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().execute(EsqlStreamQueryAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(e.getMessage(), containsString(BatchIndexingEnabled.BATCH_INDEXING.getKey()));
        } finally {
            updateClusterSettings(Settings.builder().putNull(BatchIndexingEnabled.BATCH_INDEXING.getKey()));
        }
    }

    @TestLogging(value = "org.elasticsearch.action.bulk.ShardBatchIndexer:TRACE", reason = "checks that pages take the batch path")
    public void testStored() throws Exception {
        int numDocs = indexDocs();
        var subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        AtomicReference<EsqlStreamQueryAction.ResultStream> streamRef = new AtomicReference<>();
        MockLog.assertThatLogger(() -> {
            streamRef.set(stream(true, subscriber));
            // pages are stored in the background, after the stream completes
            String id = streamRef.get().id();
            try {
                assertBusy(() -> {
                    try {
                        assertThat(readPages(id, 0, 1).size(), equalTo(1));
                    } catch (Exception e) {
                        // the result is stored in the background and may not be there yet
                        throw new AssertionError(e);
                    }
                });
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        },
            ShardBatchIndexer.class,
            new MockLog.SeenEventExpectation(
                "batch path",
                ShardBatchIndexer.class.getCanonicalName(),
                Level.TRACE,
                "batch indexed [*] operations on primary shard*"
            )
        );
        var resultStream = streamRef.get();
        assertThat(subscriber.rowCount.get(), equalTo(numDocs));
        assertThat(resultStream.id(), notNullValue());
        String docId = resultStream.id();

        NamedWriteableRegistry registry = internalCluster().getInstance(NamedWriteableRegistry.class);
        AtomicReference<StoredResultMetadata> metadata = new AtomicReference<>();
        assertBusy(() -> {
            List<StoredPage> first;
            try {
                first = readPages(docId, 0, 1);
            } catch (Exception e) {
                // the result is stored in the background and may not be there yet
                throw new AssertionError(e);
            }
            assertThat(first.size(), equalTo(1));
            StoredResultMetadata m = new StoredResultMetadata(input(first.get(0), registry));
            assertThat(m.status(), equalTo(StoredResultMetadata.Status.COMPLETED));
            metadata.set(m);
        });
        assertThat(metadata.get().rowCount(), equalTo((long) numDocs));
        assertThat(metadata.get().pageCount(), greaterThan(1));
        assertThat(metadata.get().columns().get(0).name(), equalTo("v"));

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        List<Long> values = new ArrayList<>();
        List<StoredPage> pages = readPages(docId, 1, metadata.get().pageCount() + 1);
        assertThat(pages.size(), equalTo(metadata.get().pageCount()));
        long expectedFirstRow = 0;
        for (int i = 0; i < pages.size(); i++) {
            StoredPage stored = pages.get(i);
            assertThat(stored.seq(), equalTo(i + 1));
            assertThat(stored.firstRow(), equalTo(expectedFirstRow));
            try (BlockStreamInput in = new BlockStreamInput(input(stored, registry), blockFactory)) {
                Page page = new Page(in);
                try {
                    assertThat(page.getPositionCount(), equalTo(stored.rowCount()));
                    LongBlock block = page.getBlock(0);
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        values.add(block.getLong(block.getFirstValueIndex(p)));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            expectedFirstRow += stored.rowCount();
        }
        Collections.sort(values);
        assertThat(values, equalTo(LongStream.range(0, numDocs).boxed().toList()));
    }

    /**
     * Concurrent results spread over the shards of the pages index, and its shards over the nodes, while all the documents
     * of one result stay on the shard its id routes to.
     */
    public void testResultsSpreadOverShards() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        int numDocs = indexDocs();
        int results = between(8, 12);
        NamedWriteableRegistry registry = internalCluster().getInstance(NamedWriteableRegistry.class);
        for (int r = 0; r < results; r++) {
            var subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
            String docId = stream(true, subscriber).id();
            assertBusy(() -> {
                List<StoredPage> first;
                try {
                    first = readPages(docId, 0, 1);
                } catch (Exception e) {
                    // the result is stored in the background and may not be there yet
                    throw new AssertionError(e);
                }
                assertThat(first.size(), equalTo(1));
                StoredResultMetadata metadata = new StoredResultMetadata(input(first.get(0), registry));
                assertThat(metadata.status(), equalTo(StoredResultMetadata.Status.COMPLETED));
                List<StoredPage> pages = readPages(docId, 1, metadata.pageCount());
                assertThat(pages.size(), equalTo(metadata.pageCount()));
                assertThat(pages.stream().mapToLong(StoredPage::rowCount).sum(), equalTo((long) numDocs));
            });
        }

        int shardsWithPages = 0;
        for (var shard : client().admin().indices().prepareStats(EsqlStreamResultsIndex.INDEX).setDocs(true).get().getShards()) {
            if (shard.getShardRouting().primary() && shard.getStats().getDocs().getCount() > 0) {
                shardsWithPages++;
            }
        }
        assertThat(shardsWithPages, greaterThan(1));

        var routingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().routingTable();
        long nodes = routingTable.allShards(EsqlStreamResultsIndex.INDEX)
            .stream()
            .filter(ShardRouting::primary)
            .map(ShardRouting::currentNodeId)
            .distinct()
            .count();
        assertThat(nodes, greaterThan(1L));
    }

    private List<StoredPage> readPages(String docId, int fromSeq, int maxPages) {
        return client().execute(TransportReadStreamPagesAction.TYPE, new TransportReadStreamPagesAction.Request(docId, fromSeq, maxPages))
            .actionGet(TEST_REQUEST_TIMEOUT)
            .pages();
    }

    private static StreamInput input(StoredPage stored, NamedWriteableRegistry registry) throws IOException {
        StreamInput in = new NamedWriteableAwareStreamInput(stored.bytes().streamInput(), registry);
        in.setTransportVersion(stored.transportVersion());
        return in;
    }
}
