/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.plugin.StoredPagesService;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.StoredPage;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Reads of stored stream pages straight from the shard holding them. The tests write the pages and the metadata document of a
 * result themselves, in the layout of {@link EsqlStreamResultsIndexDescriptor}, so that a read can be checked at any point of a
 * result being written.
 */
public class ReadStreamPagesIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireStoredResults() {
        assumeTrue("stored results are behind a feature flag", EsqlStreamResultsIndexDescriptor.FEATURE_FLAG.isEnabled());
        assumeTrue("pages are written as columnar batches, behind a feature flag", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // reads are charged to the request breaker, which the test cluster may otherwise make a no-op
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            );
        // nodes refuse the setting while its feature flag is off, and the tests are skipped then
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
    }

    @After
    public void clearWaitingListeners() {
        for (StoredPagesService service : internalCluster().getInstances(StoredPagesService.class)) {
            service.setWaitingListener(streamId -> {});
        }
    }

    /**
     * A read sees pages without a refresh of the index: it refreshes its shard locally, and waits for pages not indexed yet.
     */
    public void testReadWaitsForNewPages() throws Exception {
        String streamId = randomAlphaOfLength(12);
        writeMetadata(running(streamId));
        List<BytesRef> first = randomBlobs(between(1, 3));
        writePages(streamId, 0, first);
        // indexed but never refreshed
        List<StoredPage> visible = read(
            new TransportReadStreamPagesAction.Request(streamId, 0, first.size(), Long.MAX_VALUE, TimeValue.timeValueSeconds(30))
        );
        assertThat(visible.size(), equalTo(first.size()));

        int nextSeq = first.size();
        ActionFuture<TransportReadStreamPagesAction.Response> waiting = startWaitingRead(
            streamId,
            new TransportReadStreamPagesAction.Request(streamId, nextSeq, 10, Long.MAX_VALUE, TimeValue.timeValueSeconds(30))
        );
        assertFalse(waiting.isDone());

        List<BytesRef> next = randomBlobs(between(1, 3));
        writePages(streamId, nextSeq, next);
        List<StoredPage> woken = pagesOf(waiting);
        assertThat(woken.size(), equalTo(next.size()));
        for (int i = 0; i < next.size(); i++) {
            assertThat(woken.get(i).seq(), equalTo(nextSeq + i));
            assertThat(woken.get(i).bytes().toBytesRef(), equalTo(next.get(i)));
        }

        // the wait times out
        List<StoredPage> timedOut = read(
            new TransportReadStreamPagesAction.Request(streamId, nextSeq + next.size(), 10, Long.MAX_VALUE, TimeValue.timeValueMillis(100))
        );
        assertThat(timedOut.size(), equalTo(0));
        // a read frees its slot once it has answered
        assertBusy(() -> {
            for (StoredPagesService service : internalCluster().getInstances(StoredPagesService.class)) {
                assertThat(service.waitingReads(), equalTo(0));
            }
        });
    }

    /**
     * A read past the last page of a result that ended answers at once that it ended: its wait is longer than the test waits
     * for the answer. A result that failed has no page count, and ends wherever it stopped.
     */
    public void testReadPastEndedResultDoesNotWait() {
        String streamId = randomAlphaOfLength(12);
        int pages = between(1, 4);
        StreamMetadata started = running(streamId);
        writeMetadata(started);
        writePages(streamId, 0, randomBlobs(pages));
        boolean failed = randomBoolean();
        writeMetadata(
            failed ? started.failed(false, 10, new IllegalStateException("boom")) : started.complete(false, 10, pages, pages * 10L)
        );
        refreshPages();

        TransportReadStreamPagesAction.Response response = client().execute(
            TransportReadStreamPagesAction.TYPE,
            new TransportReadStreamPagesAction.Request(streamId, pages, 1, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        ).actionGet(TEST_REQUEST_TIMEOUT);
        try {
            assertTrue(response.resultEnded());
            assertThat(response.pages().size(), equalTo(0));
        } finally {
            response.decRef();
        }
        // the pages before the end are still read
        assertThat(read(new TransportReadStreamPagesAction.Request(streamId, 0, pages)).size(), equalTo(pages));
    }

    /**
     * A read past the last page of a result that is still written waits, and is not answered as ended.
     */
    public void testReadPastRunningResultWaits() throws Exception {
        String streamId = randomAlphaOfLength(12);
        writeMetadata(running(streamId));
        writePages(streamId, 0, randomBlobs(2));
        refreshPages();

        ActionFuture<TransportReadStreamPagesAction.Response> waiting = startWaitingRead(
            streamId,
            new TransportReadStreamPagesAction.Request(streamId, 2, 1, Long.MAX_VALUE, TimeValue.timeValueSeconds(30))
        );
        assertFalse(waiting.isDone());
        writePages(streamId, 2, randomBlobs(1));
        List<StoredPage> pages = pagesOf(waiting);
        assertThat(pages.size(), equalTo(1));
        assertThat(pages.get(0).seq(), equalTo(2));
    }

    /**
     * Reading from a row starts at the page that holds it, or waits for the page that will hold it. The pages written by
     * {@link #writePages} hold 10 rows each.
     */
    public void testReadFromRow() throws Exception {
        String streamId = randomAlphaOfLength(12);
        writePages(streamId, 0, randomBlobs(4));
        refreshPages();
        assertThat(firstSeqFromRow(streamId, 0), equalTo(0));
        assertThat(firstSeqFromRow(streamId, 9), equalTo(0));
        assertThat(firstSeqFromRow(streamId, 10), equalTo(1));
        assertThat(firstSeqFromRow(streamId, 39), equalTo(3));
        assertThat(firstSeqFromRow(streamId, 40), equalTo(-1));
        assertThat(firstSeqFromRow(streamId, Long.MAX_VALUE), equalTo(-1));

        ActionFuture<TransportReadStreamPagesAction.Response> waiting = startWaitingRead(
            streamId,
            TransportReadStreamPagesAction.Request.fromRow(streamId, 45, 1, Long.MAX_VALUE, TimeValue.timeValueSeconds(30))
        );
        writePages(streamId, 4, randomBlobs(1));
        List<StoredPage> pages = pagesOf(waiting);
        assertThat(pages.size(), equalTo(1));
        assertThat(pages.get(0).seq(), equalTo(4));
    }

    /**
     * A row before the first stored page is never stored: its read fails, also once it waits.
     */
    public void testReadFromRowBeforeStoredPages() {
        String streamId = randomAlphaOfLength(12);
        writeMetadata(running(streamId));
        ActionFuture<TransportReadStreamPagesAction.Response> waiting = startWaitingRead(
            streamId,
            TransportReadStreamPagesAction.Request.fromRow(streamId, 5, 1, Long.MAX_VALUE, TimeValue.timeValueSeconds(30))
        );
        writePages(streamId, 3, randomBlobs(2));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> pagesOf(waiting));
        assertThat(e.getMessage(), equalTo("row [5] is not stored, the stored rows start at [30]"));

        expectThrows(
            IllegalArgumentException.class,
            () -> read(TransportReadStreamPagesAction.Request.fromRow(streamId, 29, 1, Long.MAX_VALUE, TimeValue.ZERO))
        );
        assertThat(firstSeqFromRow(streamId, 30), equalTo(3));
    }

    /**
     * A read takes pages while they fit in its budget, and always the first.
     */
    public void testReadStopsAtByteBudget() {
        String streamId = randomAlphaOfLength(12);
        int pageBytes = 1024;
        int pageCount = between(2, 8);
        writePages(streamId, 0, sameSizeBlobs(pageCount, pageBytes));
        refreshPages();

        int fitting = between(1, pageCount);
        long maxBytes = fitting * pageBytes + between(0, pageBytes - 1);
        assertThat(read(new TransportReadStreamPagesAction.Request(streamId, 0, 100, maxBytes, TimeValue.ZERO)).size(), equalTo(fitting));
        assertThat(read(new TransportReadStreamPagesAction.Request(streamId, 0, 100, 0, TimeValue.ZERO)).size(), equalTo(1));
    }

    /**
     * A read ends before a page that is not visible, so that the pages after it are read once it is.
     */
    public void testReadStopsAtMissingPage() {
        String streamId = randomAlphaOfLength(12);
        int before = between(1, 4);
        writePages(streamId, 0, randomBlobs(before));
        writePages(streamId, before + 1, randomBlobs(between(1, 4)));
        refreshPages();

        List<StoredPage> pages = read(new TransportReadStreamPagesAction.Request(streamId, 0, 100, Long.MAX_VALUE, TimeValue.ZERO));
        assertThat(pages.stream().map(StoredPage::seq).toList(), equalTo(IntStream.range(0, before).boxed().toList()));
    }

    /**
     * The pages of a read are charged to the request breaker: a page that trips it ends the read, unless it is the first.
     */
    public void testReadChargesRequestBreaker() {
        String streamId = randomAlphaOfLength(12);
        int pageBytes = 64 * 1024;
        writePages(streamId, 0, sameSizeBlobs(between(2, 4), pageBytes));
        refreshPages();
        TransportReadStreamPagesAction.Request request = new TransportReadStreamPagesAction.Request(
            streamId,
            1,
            100,
            Long.MAX_VALUE,
            TimeValue.ZERO
        );
        try {
            setRequestBreakerLimit(pageBytes * 3 / 2 + "b");
            assertThat(read(request).size(), equalTo(1));

            setRequestBreakerLimit(pageBytes / 2 + "b");
            expectThrows(CircuitBreakingException.class, () -> read(request));
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    /**
     * Pages that trip the breaker of the coordinating node fail the read there, without reading them again from another copy.
     */
    public void testCoordinatingBreakerTripDoesNotRetry() throws IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String coordinator = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "16kb").build()
        );
        String streamId = randomAlphaOfLength(12);
        writePages(streamId, 0, sameSizeBlobs(1, 64 * 1024));
        refreshPages();
        ensureGreen(EsqlStreamResultsIndexDescriptor.INDEX);
        AtomicInteger shardReads = new AtomicInteger();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).addRequestHandlingBehavior(
                TransportReadStreamPagesAction.NAME + "[s]",
                (handler, request, channel, task) -> {
                    shardReads.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        try {
            expectThrows(
                CircuitBreakingException.class,
                () -> pagesOf(
                    client(coordinator).execute(
                        TransportReadStreamPagesAction.TYPE,
                        new TransportReadStreamPagesAction.Request(streamId, 0, 100)
                    )
                )
            );
            assertThat(shardReads.get(), equalTo(1));
        } finally {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) transportService).clearAllRules();
            }
            internalCluster().stopNode(coordinator);
        }
    }

    /**
     * A read finds its pages among those of other results and across segments, from a page or from a row, also when expensive
     * queries are disabled: the fields of the index have doc values only.
     */
    public void testReadAmongResultsWithExpensiveQueriesDisabled() {
        List<String> streamIds = IntStream.range(0, between(2, 5)).mapToObj(i -> randomAlphaOfLength(12)).toList();
        int batches = between(2, 4);
        int pagesPerBatch = between(1, 5);
        for (int b = 0; b < batches; b++) {
            for (String streamId : streamIds) {
                writePages(streamId, b * pagesPerBatch, randomBlobs(pagesPerBatch));
            }
            // a segment per batch
            refreshPages();
        }
        int pageCount = batches * pagesPerBatch;
        try {
            updateClusterSettings(Settings.builder().put(ALLOW_EXPENSIVE_QUERIES.getKey(), false));
            for (String streamId : streamIds) {
                int fromSeq = between(0, pageCount - 1);
                List<StoredPage> pages = read(new TransportReadStreamPagesAction.Request(streamId, fromSeq, 100));
                assertThat(pages.stream().map(StoredPage::seq).toList(), equalTo(IntStream.range(fromSeq, pageCount).boxed().toList()));
                long row = randomLongBetween(0, pageCount * 10L - 1);
                assertThat(firstSeqFromRow(streamId, row), equalTo((int) (row / 10)));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(ALLOW_EXPENSIVE_QUERIES.getKey()));
        }
    }

    /**
     * A result spanning many blocks of its fields' skippers, between other results in the same segment, is read from any page
     * or row.
     */
    public void testReadAcrossSkipperBlocks() {
        String streamId = "b" + randomAlphaOfLength(11);
        int pageCount = between(10_000, 20_000);
        writePages(streamId, 0, sameSizeBlobs(pageCount, 1));
        // the neighbors sort before and after the result, on its shard
        IndexRouting routing = IndexRouting.fromIndexMetadata(
            internalCluster().clusterService(internalCluster().getMasterName())
                .state()
                .metadata()
                .getProject(ProjectId.DEFAULT)
                .index(EsqlStreamResultsIndexDescriptor.INDEX)
        );
        int shardId = routing.getShard(streamId, streamId);
        String before = randomValueOtherThanMany(id -> routing.getShard(id, id) != shardId, () -> "a" + randomAlphaOfLength(11));
        String after = randomValueOtherThanMany(id -> routing.getShard(id, id) != shardId, () -> "c" + randomAlphaOfLength(11));
        int beforeCount = between(1, 5_000);
        int afterCount = between(1, 5_000);
        writePages(before, 0, sameSizeBlobs(beforeCount, 1));
        writePages(after, 0, sameSizeBlobs(afterCount, 1));
        ensureGreen(EsqlStreamResultsIndexDescriptor.INDEX);
        assertNoFailures(
            esqlClient().admin().indices().prepareForceMerge(EsqlStreamResultsIndexDescriptor.INDEX).setMaxNumSegments(1).get()
        );
        refreshPages();
        int copies = 0;
        for (IndicesService indices : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService index : indices) {
                if (index.index().getName().equals(EsqlStreamResultsIndexDescriptor.INDEX) == false || index.hasShard(shardId) == false) {
                    continue;
                }
                try (Engine.Searcher searcher = index.getShard(shardId).acquireSearcher("test")) {
                    copies++;
                    assertThat(searcher.getLeafContexts().size(), equalTo(1));
                    LeafReaderContext leaf = searcher.getLeafContexts().getFirst();
                    assertThat(leaf.reader().numDocs(), equalTo(beforeCount + pageCount + afterCount));
                    for (String field : List.of(
                        EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD,
                        EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD
                    )) {
                        assertThat(
                            field,
                            leaf.reader().getFieldInfos().fieldInfo(field).docValuesSkipIndexType(),
                            not(equalTo(DocValuesSkipIndexType.NONE))
                        );
                    }
                }
            }
        }
        assertThat(copies, greaterThan(0));
        for (int i = 0; i < 20; i++) {
            int fromSeq = between(0, pageCount - 1);
            List<StoredPage> pages = read(new TransportReadStreamPagesAction.Request(streamId, fromSeq, 3));
            assertThat(
                pages.stream().map(StoredPage::seq).toList(),
                equalTo(IntStream.rangeClosed(fromSeq, Math.min(fromSeq + 2, pageCount - 1)).boxed().toList())
            );
            long row = randomLongBetween(0, pageCount * 10L - 1);
            assertThat(firstSeqFromRow(streamId, row), equalTo((int) (row / 10)));
        }
        assertThat(read(new TransportReadStreamPagesAction.Request(streamId, pageCount, 3)), empty());
        assertThat(firstSeqFromRow(streamId, pageCount * 10L), equalTo(-1));
    }

    private static StreamMetadata running(String streamId) {
        return StreamMetadata.started(
            streamId,
            new TaskId("node", 1),
            Instant.now().plusSeconds(3600),
            List.of(new ColumnInfoImpl("v", "long", null)),
            Map.of(),
            List.of()
        );
    }

    /**
     * Writes the metadata document of a result, without refreshing.
     */
    private void writeMetadata(StreamMetadata metadata) {
        try {
            esqlClient().index(StreamMetadataDocument.indexRequest(metadata)).actionGet(TEST_REQUEST_TIMEOUT);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Writes {@code blobs} as the pages of a result from {@code fromSeq}, of 10 rows each, in one pre-built batch, without
     * refreshing.
     */
    private void writePages(String streamId, int fromSeq, List<BytesRef> blobs) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            BulkRequestBuilder bulk = esqlClient().prepareBulk();
            List<IndexRequest> requests = new ArrayList<>();
            byte[] id = streamId.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < blobs.size(); i++) {
                int seq = fromSeq + i;
                EscfRowBuffer row = builder.beginRow();
                row.stringField(EsqlStreamResultsIndexDescriptor.STREAM_ID_FIELD, id, 0, id.length);
                row.longField(EsqlStreamResultsIndexDescriptor.PAGE_SEQ_FIELD, seq);
                row.longField(EsqlStreamResultsIndexDescriptor.PAGE_OFFSET_FIELD, seq * 10L);
                row.longField(EsqlStreamResultsIndexDescriptor.PAGE_SIZE_FIELD, 10);
                row.longField(EsqlStreamResultsIndexDescriptor.PAGE_TRANSPORT_VERSION_FIELD, 1);
                row.binaryField(EsqlStreamResultsIndexDescriptor.PAGE_FIELD, blobs.get(i));
                row.finishRow();
                builder.commit(0);
                requests.add(
                    new IndexRequest(EsqlStreamResultsIndexDescriptor.INDEX).id(streamId + "/" + seq)
                        .routing(streamId)
                        .opType(DocWriteRequest.OpType.CREATE)
                );
            }
            EscfBatch batch = builder.buildPartition(0);
            for (int r = 0; r < requests.size(); r++) {
                requests.get(r).indexSource().setSourceRow(batch, r, XContentType.JSON);
                bulk.add(requests.get(r));
            }
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
            bulk.setPreBuiltBatches(Map.of(EsqlStreamResultsIndexDescriptor.INDEX, batch));
            assertNoFailures(bulk.get());
        }
    }

    private Client esqlClient() {
        return new OriginSettingClient(client(), ClientHelper.ESQL_ORIGIN);
    }

    private void refreshPages() {
        esqlClient().admin().indices().prepareRefresh(EsqlStreamResultsIndexDescriptor.INDEX).get();
    }

    private static List<BytesRef> randomBlobs(int count) {
        List<BytesRef> blobs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            blobs.add(new BytesRef(randomByteArrayOfLength(between(1, 1024))));
        }
        return blobs;
    }

    private static List<BytesRef> sameSizeBlobs(int count, int bytes) {
        List<BytesRef> blobs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            blobs.add(new BytesRef(randomByteArrayOfLength(bytes)));
        }
        return blobs;
    }

    private int firstSeqFromRow(String streamId, long row) {
        List<StoredPage> pages = read(TransportReadStreamPagesAction.Request.fromRow(streamId, row, 1, Long.MAX_VALUE, TimeValue.ZERO));
        return pages.isEmpty() ? -1 : pages.get(0).seq();
    }

    /**
     * Completes the next time a read waits for pages of the result, having found none.
     */
    private SubscribableListener<Void> onWaiting(String streamId) {
        SubscribableListener<Void> waiting = new SubscribableListener<>();
        for (StoredPagesService service : internalCluster().getInstances(StoredPagesService.class)) {
            service.setWaitingListener(id -> {
                if (id.equals(streamId)) {
                    waiting.onResponse(null);
                }
            });
        }
        return waiting;
    }

    /**
     * Sends a read and returns once it waits on its shard.
     */
    private ActionFuture<TransportReadStreamPagesAction.Response> startWaitingRead(
        String streamId,
        TransportReadStreamPagesAction.Request request
    ) {
        SubscribableListener<Void> waiting = onWaiting(streamId);
        ActionFuture<TransportReadStreamPagesAction.Response> read = client().execute(TransportReadStreamPagesAction.TYPE, request);
        safeAwait(waiting);
        return read;
    }

    private void setRequestBreakerLimit(String limit) {
        updateClusterSettings(Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit));
    }

    /**
     * Returns the pages of a read and releases the response, which the future holds a reference to.
     */
    private static List<StoredPage> pagesOf(ActionFuture<TransportReadStreamPagesAction.Response> future) {
        TransportReadStreamPagesAction.Response response = future.actionGet(TEST_REQUEST_TIMEOUT);
        try {
            return response.pages();
        } finally {
            response.decRef();
        }
    }

    private List<StoredPage> read(TransportReadStreamPagesAction.Request request) {
        return pagesOf(client().execute(TransportReadStreamPagesAction.TYPE, request));
    }
}
