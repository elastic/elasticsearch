/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.core.esql.EsqlStreamResultsIndex;
import org.junit.After;
import org.junit.Before;

import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * The bulks of a {@link StoredResultWriter} are answered by the test, so it sees what is sent at each step. Pages are taken from
 * a mock recycler, which fails the test if one is not released.
 */
public class StoredResultWriterTests extends ESTestCase {

    private static final String RESULT = "result";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Bulks bulks;
    private StoredResultWriter writer;

    @Before
    public void createWriter() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        bulks = new Bulks(threadPool);
        writer = new StoredResultWriter(
            bulks,
            clusterService,
            new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY)),
            RESULT,
            StoredResultMetadata.running(Map.of(), new TaskId("node", 1), List.of(), ZoneOffset.UTC),
            0,
            () -> {}
        );
    }

    @After
    public void stop() {
        bulks.releaseAll();
        clusterService.close();
        terminate(threadPool);
    }

    public void testMetadataFirstThenPagesThenMetadataLast() {
        writer.start();
        assertThat(ids(bulks.next()), equalTo(List.of(RESULT)));
        bulks.succeed();

        int pages = between(1, 40);
        for (int i = 0; i < pages; i++) {
            addPage();
        }
        // one bulk at a time
        int sent = 0;
        while (sent < pages) {
            BulkRequest bulk = bulks.next();
            assertThat(bulk.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.NONE));
            sent += bulk.requests().size();
            bulks.succeed();
        }
        assertThat(sent, equalTo(pages));

        writer.finish(null, null);
        BulkRequest last = bulks.next();
        assertThat(ids(last), equalTo(List.of(RESULT)));
        assertThat(last.requests().get(0).opType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertThat(last.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
        bulks.succeed();
        assertThat(bulks.sent.size(), equalTo(0));
    }

    /**
     * A failed bulk drops the pages not sent yet, and the last bulk rewrites page {@code 0} to say that storing failed.
     */
    public void testFailedBulkRewritesMetadata() {
        writer.start();
        bulks.succeed();
        addPage();
        addPage();
        bulks.next();
        bulks.fail(new EsRejectedExecutionException("rejected", false));

        BulkRequest last = bulks.next();
        assertThat(ids(last), equalTo(List.of(RESULT)));
        assertThat(last.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
        assertTrue(writer.hasCapacity());

        // nothing more is stored
        addPage();
        writer.finish(null, null);
        bulks.succeed();
        assertThat(bulks.sent.size(), equalTo(0));
    }

    /**
     * A page delivered while the stream is cancelled, once page {@code 0} is rewritten, is not stored.
     */
    public void testPageAfterFinishIsDropped() {
        writer.start();
        bulks.succeed();
        writer.finish(null, null);
        addPage();
        assertThat(ids(bulks.next()), equalTo(List.of(RESULT)));
        bulks.succeed();
        assertThat(bulks.sent.size(), equalTo(0));
    }

    public void testFailureWhileBulkInFlight() {
        writer.start();
        addPage();
        bulks.fail(new EsRejectedExecutionException("rejected", false));
        // the queued page is dropped, page 0 says that storing failed
        BulkRequest last = bulks.next();
        assertThat(ids(last), equalTo(List.of(RESULT)));
        bulks.succeed();
        assertThat(bulks.sent.size(), equalTo(0));
    }

    private void addPage() {
        Page page = new Page(TestBlockFactory.getNonBreakingInstance().newConstantIntBlockWith(randomInt(), between(1, 10)));
        try {
            writer.onPage(page);
        } finally {
            page.releaseBlocks();
        }
    }

    private static List<String> ids(BulkRequest bulk) {
        return bulk.requests().stream().map(DocWriteRequest::id).toList();
    }

    private record Bulk(BulkRequest request, ActionListener<BulkResponse> listener) {}

    /**
     * Holds the bulks sent, for the test to answer.
     */
    private static class Bulks extends NoOpClient {
        private final Deque<Bulk> sent = new ArrayDeque<>();

        Bulks(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Req extends ActionRequest, Resp extends ActionResponse> void doExecute(
            ActionType<Resp> action,
            Req request,
            ActionListener<Resp> listener
        ) {
            BulkRequest bulk = (BulkRequest) request;
            assertThat(bulk.requests().get(0).index(), equalTo(EsqlStreamResultsIndex.INDEX));
            sent.add(new Bulk(bulk, (ActionListener<BulkResponse>) listener));
        }

        BulkRequest next() {
            assertThat(sent.size(), equalTo(1));
            return sent.getFirst().request;
        }

        void succeed() {
            Bulk bulk = sent.removeFirst();
            bulk.request.releasePreBuiltBatches();
            bulk.listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
        }

        void fail(Exception e) {
            Bulk bulk = sent.removeFirst();
            bulk.request.releasePreBuiltBatches();
            bulk.listener.onFailure(e);
        }

        void releaseAll() {
            sent.forEach(b -> b.request.releasePreBuiltBatches());
            sent.clear();
        }
    }
}
