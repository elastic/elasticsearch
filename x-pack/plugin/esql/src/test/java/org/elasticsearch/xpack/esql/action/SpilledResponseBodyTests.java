/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.ResultCollector;
import org.elasticsearch.compute.spill.ResultPages;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A response whose pages are in a run renders, through the REST layer's parts, exactly what the same pages in memory
 * render, for every format, with and without the null columns dropped; the parts read the run through the
 * continuation; and a close with a continuation pending releases the cursor and the page once the read lands.
 */
public class SpilledResponseBodyTests extends ESTestCase {

    private static final List<ColumnInfoImpl> COLUMNS = List.of(
        new ColumnInfoImpl("id", "long", null),
        new ColumnInfoImpl("name", "keyword", null),
        new ColumnInfoImpl("empty", "keyword", null),
        new ColumnInfoImpl("tags", "integer", null)
    );

    private TestThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testJsonRows() throws Exception {
        assertSameRendering(Map.of(), false);
    }

    public void testJsonRowsDroppingNullColumns() throws Exception {
        assertSameRendering(Map.of("drop_null_columns", "true"), false);
    }

    public void testJsonColumnar() throws Exception {
        assertSameRendering(Map.of(), true);
    }

    public void testJsonColumnarDroppingNullColumns() throws Exception {
        assertSameRendering(Map.of("drop_null_columns", "true"), true);
    }

    public void testYaml() throws Exception {
        assertSameRendering(Map.of("format", "yaml"), randomBoolean());
    }

    public void testCsv() throws Exception {
        assertSameRendering(Map.of("format", "csv"), false);
    }

    public void testCsvDroppingNullColumns() throws Exception {
        assertSameRendering(Map.of("format", "csv", "drop_null_columns", "true"), false);
    }

    public void testCsvWithoutAHeader() throws Exception {
        assertSameRendering(Map.of("format", "csv", "header", "absent"), false);
    }

    public void testTsv() throws Exception {
        assertSameRendering(Map.of("format", "tsv"), false);
    }

    public void testMarkdown() throws Exception {
        assertSameRendering(Map.of("format", "md"), false);
    }

    public void testTable() throws Exception {
        assertSameRendering(Map.of("format", "txt"), false);
    }

    public void testTableDroppingNullColumns() throws Exception {
        assertSameRendering(Map.of("format", "txt", "drop_null_columns", "true"), false);
    }

    public void testArrow() throws Exception {
        assertSameRendering(Map.of("format", "arrow"), false);
    }

    public void testArrowDroppingNullColumns() throws Exception {
        assertSameRendering(Map.of("format", "arrow", "drop_null_columns", "true"), false);
    }

    /**
     * The response is closed while a part's continuation waits on the store: the read lands into a closed body, and the
     * page it brought and the cursor go, leaving the query's account at zero.
     */
    public void testCloseWithAContinuationPendingReleasesEverything() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> pages = randomPages(between(3, 10), between(1, 50));
        Spilled spilled = spill(store, pages, threadPool.executor(ThreadPool.Names.SEARCH));
        try {
            EsqlQueryResponse response = response(spilled.pages(), false);
            FakeRestChannel channel = channel(Map.of());
            new EsqlResponseListener(channel, channel.request()).onResponse(response);
            // as the transport layer does once the listener has the response: the REST response holds the last reference
            response.decRef();
            RestResponse restResponse = channel.capturedResponse();
            ChunkedRestResponseBodyPart part = restResponse.chunkedContent();
            // the first part is in memory; the next needs a read, which the store holds
            while (part.isPartComplete() == false) {
                part.encodeChunk(between(1, 1024), BytesRefRecycler.NON_RECYCLING_INSTANCE).close();
            }
            assertFalse(part.isLastPart());
            PlainActionFuture<ChunkedRestResponseBodyPart> next = new PlainActionFuture<>();
            part.getNextPart(next);
            assertBusy(() -> assertThat("the store holds the read", store.heldOperations(), greaterThan(0)));
            assertFalse("the continuation waits", next.isDone());
            // the channel closed: the response closes, its hook closing the body
            restResponse.close();
            assertFalse("the response closed with the channel", response.hasReferences());
            releaseUntilDone(store, next);
            // the read landed into a closed body: a failure, with nothing held
            expectThrows(Exception.class, next::actionGet);
        } finally {
            spilled.close(store);
        }
    }

    // ---- the comparison

    /**
     * A row handed out by a spilled response stays readable after the walk has moved on and released its page, as a
     * resident response's rows do until the response closes: a caller may collect rows first and read them later.
     */
    public void testRetainedRowsStayReadableAfterTheWalkMovesOn() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> pages = randomPagesWithRows(between(3, 8), between(1, 20));
        List<Page> copy = BlockTestUtils.deepCopyOf(pages, TestBlockFactory.getNonBreakingInstance());
        Spilled spilled = spill(store, copy, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try {
            List<List<Object>> expected = new ArrayList<>();
            try (EsqlQueryResponse resident = response(ResultPages.resident(pages), false)) {
                for (Iterable<Object> row : resident.rows()) {
                    expected.add(Iterators.toList(row.iterator()));
                }
            }
            try (EsqlQueryResponse response = response(spilled.pages(), false)) {
                assertTrue(response.isSpilled());
                List<Iterable<Object>> retained = new ArrayList<>();
                for (Iterable<Object> row : response.rows()) {
                    retained.add(row);
                }
                List<List<Object>> read = new ArrayList<>();
                for (Iterable<Object> row : retained) {
                    read.add(Iterators.toList(row.iterator()));
                }
                assertThat(read, equalTo(expected));
                List<Iterator<Object>> retainedValues = new ArrayList<>();
                response.values().forEachRemaining(retainedValues::add);
                read.clear();
                for (Iterator<Object> row : retainedValues) {
                    read.add(Iterators.toList(row));
                }
                assertThat(read, equalTo(expected));
            }
        } finally {
            spilled.close(store);
        }
    }

    /**
     * Random pages of which at least three have rows, so that a walk releases a page before it ends.
     */
    private static List<Page> randomPagesWithRows(int count, int maxRows) {
        while (true) {
            List<Page> pages = randomPages(count, maxRows);
            if (pages.stream().filter(page -> page.getPositionCount() > 0).count() >= 3) {
                return pages;
            }
            Releasables.close(pages);
        }
    }

    private void assertSameRendering(Map<String, String> params, boolean columnar) throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> pages = randomPages(between(1, 12), between(0, 40));
        List<Page> copy = BlockTestUtils.deepCopyOf(pages, TestBlockFactory.getNonBreakingInstance());
        Spilled spilled = spill(store, copy, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try {
            byte[] expected;
            try (EsqlQueryResponse resident = response(ResultPages.resident(pages), columnar)) {
                expected = render(resident, params);
            }
            byte[] actual;
            try (EsqlQueryResponse response = response(spilled.pages(), columnar)) {
                assertTrue(response.isSpilled());
                actual = render(response, params);
                // the blocking walks, whole and abandoned, leave nothing behind once the response closes
                long rows = 0;
                for (Iterable<Object> row : response.rows()) {
                    rows += row.iterator().hasNext() ? 1 : 0;
                }
                assertThat(rows, equalTo(response.getRowCount()));
                Iterator<Iterator<Object>> abandoned = response.values();
                if (abandoned.hasNext()) {
                    abandoned.next();
                }
            }
            assertThat(new String(actual, StandardCharsets.UTF_8), equalTo(new String(expected, StandardCharsets.UTF_8)));
            assertArrayEquals(expected, actual);
        } finally {
            spilled.close(store);
        }
    }

    /**
     * Renders the response as the REST layer would: part by part, chunk by chunk, continuations waited for.
     */
    private static byte[] render(EsqlQueryResponse response, Map<String, String> params) throws Exception {
        FakeRestChannel channel = channel(params);
        new EsqlResponseListener(channel, channel.request()).onResponse(response);
        RestResponse restResponse = channel.capturedResponse();
        assertThat("the response was sent", restResponse, org.hamcrest.Matchers.notNullValue());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            if (restResponse.chunkedContent() == null) {
                BytesReference content = restResponse.content();
                out.write(BytesReference.toBytes(content));
                return out.toByteArray();
            }
            ChunkedRestResponseBodyPart part = restResponse.chunkedContent();
            while (true) {
                while (part.isPartComplete() == false) {
                    try (ReleasableBytesReference chunk = part.encodeChunk(between(1, 4096), BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
                        out.write(BytesReference.toBytes(chunk));
                    }
                }
                if (part.isLastPart()) {
                    break;
                }
                PlainActionFuture<ChunkedRestResponseBodyPart> next = new PlainActionFuture<>();
                part.getNextPart(next);
                part = next.actionGet();
            }
            return out.toByteArray();
        } finally {
            restResponse.close();
        }
    }

    private static FakeRestChannel channel(Map<String, String> params) {
        Map<String, String> all = new HashMap<>(params);
        all.put("id", "query");
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(all).build();
        return new FakeRestChannel(request, true);
    }

    // ---- the responses

    private static EsqlQueryResponse response(ResultPages pages, boolean columnar) {
        return new EsqlQueryResponse(
            COLUMNS,
            pages,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            null,
            columnar,
            null,
            false,
            false,
            ZoneOffset.UTC,
            0,
            0,
            null,
            null
        );
    }

    /**
     * Pages of an id, a name sometimes null, a column null throughout and a multivalued integer column.
     */
    private static List<Page> randomPages(int count, int maxRows) {
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();
        List<Page> pages = new ArrayList<>();
        for (int p = 0; p < count; p++) {
            // a page without rows now and then: a part must have a chunk to give, so such a page is skipped
            int rows = randomInt(6) == 0 ? 0 : between(1, Math.max(1, maxRows));
            try (
                LongBlock.Builder ids = factory.newLongBlockBuilder(rows);
                BytesRefBlock.Builder names = factory.newBytesRefBlockBuilder(rows);
                BytesRefBlock.Builder empty = factory.newBytesRefBlockBuilder(rows);
                IntBlock.Builder tags = factory.newIntBlockBuilder(rows)
            ) {
                for (int r = 0; r < rows; r++) {
                    ids.appendLong(p * 1_000L + r);
                    if (randomInt(5) == 0) {
                        names.appendNull();
                    } else {
                        names.appendBytesRef(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                    empty.appendNull();
                    int values = between(0, 3);
                    if (values == 0) {
                        tags.appendNull();
                    } else if (values == 1) {
                        tags.appendInt(randomInt(100));
                    } else {
                        tags.beginPositionEntry();
                        for (int v = 0; v < values; v++) {
                            tags.appendInt(randomInt(100));
                        }
                        tags.endPositionEntry();
                    }
                }
                pages.add(new Page(ids.build(), names.build(), empty.build(), tags.build()));
            }
        }
        return pages;
    }

    /**
     * A result in a run of a scope of its own, with the scope's lease handed to the result.
     */
    private record Spilled(QueryScope scope, ResultPages pages) {
        void close(MemorySpillStore store) throws Exception {
            PlainActionFuture<Void> closed = new PlainActionFuture<>();
            scope.addCloseListener(closed);
            releaseUntilDone(store, closed);
            assertThat("the query's account is settled", scope.breaker().used(), equalTo(0L));
        }
    }

    private static Spilled spill(MemorySpillStore store, List<Page> pages, Executor codec) throws Exception {
        QueryScope scope = QueryScope.create(
            "query",
            TestBlockFactory.getNonBreakingInstance(),
            ByteSizeValue.ofMb(64).getBytes(),
            0,
            store,
            codec,
            ignored -> {}
        );
        QueryScope.Lease lease = scope.tryAcquire();
        ResultCollector collector = new ResultCollector(scope, lease, 0, "result");
        for (Page page : pages) {
            collector.accept(page);
        }
        PlainActionFuture<ResultPages> finished = new PlainActionFuture<>();
        collector.finish(finished);
        releaseUntilDone(store, finished);
        ResultPages result = finished.get();
        assertThat(result, instanceOf(ResultPages.Spilled.class));
        return new Spilled(scope, result);
    }

    private static void releaseUntilDone(MemorySpillStore store, PlainActionFuture<?> done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (done.isDone() == false) {
            if (store.releaseAll() > 0) {
                continue;
            }
            assertTrue("the store's operations did not complete in time", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
    }
}
