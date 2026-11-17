/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.StoredResultMetadata;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Streaming queries that store their result with {@code store=true}.
 */
public class StoredStreamIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireStoredResults() {
        assumeTrue("stored results are behind a feature flag", EsqlStreamResultsIndex.FEATURE_FLAG.isEnabled());
        assumeTrue("storing needs batch indexing, behind a feature flag", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // nodes refuse the setting while its feature flag is off, and the tests are skipped then
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
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

    private static final TimeValue KEEP_ALIVE = TimeValue.timeValueHours(3);

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
            request.store(true).storeKeepAlive(KEEP_ALIVE);
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
        long before = System.currentTimeMillis();
        MockLog.assertThatLogger(() -> {
            streamRef.set(stream(true, subscriber));
            // pages are stored in the background, after the stream completes
            String id = streamRef.get().id();
            try {
                assertBusy(() -> { assertThat(readPages(id).size(), greaterThan(0)); });
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
        AtomicReference<List<StoredPage>> allPages = new AtomicReference<>();
        assertBusy(() -> {
            List<StoredPage> all = readPages(docId);
            assertThat(all.get(0).seq(), equalTo(0));
            StoredResultMetadata m = new StoredResultMetadata(input(all.get(0), registry));
            assertThat(m.status(), equalTo(StoredResultMetadata.Status.COMPLETED));
            metadata.set(m);
            allPages.set(all);
            // the result is kept for the keep_alive of its request; the cluster's clock is cached, so it can lag by a little
            long slack = TimeValue.timeValueMinutes(1).millis();
            assertThat(all.get(0).expirationTime(), greaterThanOrEqualTo(before + KEEP_ALIVE.millis() - slack));
            assertThat(all.get(0).expirationTime(), lessThanOrEqualTo(System.currentTimeMillis() + KEEP_ALIVE.millis() + slack));
        });
        assertThat(metadata.get().rowCount(), equalTo((long) numDocs));
        assertThat(metadata.get().pageCount(), greaterThan(1));
        assertThat(metadata.get().columns().get(0).name(), equalTo("v"));

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        List<Long> values = new ArrayList<>();
        List<StoredPage> pages = allPages.get().subList(1, allPages.get().size());
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
                List<StoredPage> all = readPages(docId);
                assertThat(all.get(0).seq(), equalTo(0));
                StoredResultMetadata metadata = new StoredResultMetadata(input(all.get(0), registry));
                assertThat(metadata.status(), equalTo(StoredResultMetadata.Status.COMPLETED));
                List<StoredPage> pages = all.subList(1, all.size());
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

    /**
     * A stored page as its document holds it; {@code expirationTime} is {@code -1} when the document has none.
     */
    private record StoredPage(
        int seq,
        long firstRow,
        int rowCount,
        TransportVersion transportVersion,
        long expirationTime,
        BytesReference bytes
    ) {}

    /**
     * The pages stored for a result so far, in order, read from the doc values of their documents. Fails with an
     * {@link AssertionError} while nothing is stored, so that it can be retried in {@code assertBusy}.
     */
    private List<StoredPage> readPages(String streamId) {
        SearchResponse response;
        try {
            response = searchPages(streamId);
        } catch (Exception e) {
            // the result is stored in the background and its index may not exist yet
            throw new AssertionError(e);
        }
        try {
            List<StoredPage> pages = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                var expiration = hit.field(EsqlStreamResultsIndex.EXPIRATION_TIME_FIELD);
                pages.add(
                    new StoredPage(
                        ((Number) hit.field(EsqlStreamResultsIndex.PAGE_SEQ_FIELD).getValue()).intValue(),
                        ((Number) hit.field(EsqlStreamResultsIndex.FIRST_ROW_FIELD).getValue()).longValue(),
                        ((Number) hit.field(EsqlStreamResultsIndex.ROW_COUNT_FIELD).getValue()).intValue(),
                        TransportVersion.fromId(((Number) hit.field(EsqlStreamResultsIndex.TRANSPORT_VERSION_FIELD).getValue()).intValue()),
                        expiration == null || expiration.getValues().isEmpty() ? -1 : ((Number) expiration.getValue()).longValue(),
                        new BytesArray(Base64.getDecoder().decode((String) hit.field(EsqlStreamResultsIndex.PAGE_FIELD).getValue()))
                    )
                );
            }
            assertThat(pages.size(), greaterThan(0));
            return pages;
        } finally {
            response.decRef();
        }
    }

    private SearchResponse searchPages(String streamId) {
        return new OriginSettingClient(client(), ESQL_ORIGIN).prepareSearch(EsqlStreamResultsIndex.INDEX)
            .setRouting(streamId)
            .setQuery(termQuery(EsqlStreamResultsIndex.STREAM_ID_FIELD, streamId))
            .addSort(EsqlStreamResultsIndex.PAGE_SEQ_FIELD, SortOrder.ASC)
            .setSize(10_000)
            .setFetchSource(false)
            .addDocValueField(EsqlStreamResultsIndex.PAGE_SEQ_FIELD)
            .addDocValueField(EsqlStreamResultsIndex.FIRST_ROW_FIELD)
            .addDocValueField(EsqlStreamResultsIndex.ROW_COUNT_FIELD)
            .addDocValueField(EsqlStreamResultsIndex.TRANSPORT_VERSION_FIELD)
            .addDocValueField(EsqlStreamResultsIndex.EXPIRATION_TIME_FIELD)
            .addDocValueField(EsqlStreamResultsIndex.PAGE_FIELD)
            .get(TEST_REQUEST_TIMEOUT);
    }

    private static StreamInput input(StoredPage stored, NamedWriteableRegistry registry) throws IOException {
        StreamInput in = new NamedWriteableAwareStreamInput(stored.bytes().streamInput(), registry);
        in.setTransportVersion(stored.transportVersion());
        return in;
    }
}
