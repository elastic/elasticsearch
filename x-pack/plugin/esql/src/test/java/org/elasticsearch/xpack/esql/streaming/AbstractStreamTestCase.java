/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.StreamNdjsonBody;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

/**
 * Support for the tests of the stream GET: a real {@link InMemoryStreamStore}, real pages from a circuit breaking {@link BlockFactory}
 * (every test checks that the breaker is back to zero, so a page that is not released fails the test), a writer that adds pages to a
 * stream, and a reader that runs a request through {@link StreamResultsService} and {@link StreamNdjsonBody}, the way the REST layer does,
 * and parses what a client would see.
 */
public abstract class AbstractStreamTestCase extends ESTestCase {

    protected BlockFactory blockFactory;
    protected ThreadPool threadPool;
    protected InMemoryStreamStore store;
    private final List<String> createdStreams = new ArrayList<>();

    @Before
    public void setUpStreamTest() {
        blockFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking()
        ).build();
        threadPool = new TestThreadPool(getTestName());
        store = new InMemoryStreamStore();
    }

    @After
    public void tearDownStreamTest() {
        // the store owns the stored pages and releases them when a stream is deleted
        for (String id : createdStreams) {
            awaitVoid(l -> store.delete(id, l));
        }
        terminate(threadPool);
        assertThat("every page must be released", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /** One long column named {@code n}. The value of a row is its position in the whole result. */
    protected static List<ColumnInfo> columns() {
        return List.of(new ColumnInfoImpl("n", "long", null));
    }

    protected Page page(long firstValue, int rows) {
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = firstValue + i;
        }
        LongBlock block = blockFactory.newLongArrayVector(values, rows).asBlock();
        Page page = new Page(block);
        // The in-memory store hands shallow copies of the same blocks to readers on several threads. Block reference counts are not
        // thread safe until the page is allowed to pass to another thread, so a test that reads concurrently has to do this. The real
        // storage deserializes a new page for every read, so it does not share blocks.
        page.allowPassingToDifferentDriver();
        return page;
    }

    protected final void awaitVoid(CheckedConsumer<ActionListener<Void>, ?> consumer) {
        safeAwait(consumer);
    }

    protected static long farFuture() {
        return System.currentTimeMillis() + TimeValue.timeValueHours(1).millis();
    }

    /** Adds pages to one stream the way the query would, and tracks the offset. */
    protected final class Writer {
        final String id;
        long rows = 0;
        int pages = 0;

        Writer(String id) {
            this.id = id;
        }

        Writer append(int pageRows) {
            Page page = page(rows, pageRows);
            awaitVoid(l -> store.appendPage(id, rows, page, l));
            rows += pageRows;
            pages++;
            return this;
        }

        Writer complete() {
            awaitVoid(l -> store.markComplete(id, false, rows, pages, l));
            return this;
        }

        Writer fail(Exception e) {
            awaitVoid(l -> store.markFailed(id, e, l));
            return this;
        }
    }

    protected Writer createStream(String id) {
        return createStream(id, Map.of(), farFuture());
    }

    protected Writer createStream(String id, Map<String, String> headers, long expirationMillis) {
        createdStreams.add(id);
        awaitVoid(l -> store.create(id, new TaskId("node", 1), headers, columns(), expirationMillis, l));
        return new Writer(id);
    }

    protected StreamResultsService service() {
        return service(store);
    }

    protected StreamResultsService service(StreamStorage storage) {
        return new StreamResultsService(
            storage,
            new StreamSearchSecurity(new SecurityContext(Settings.EMPTY, threadPool.getThreadContext())),
            threadPool
        );
    }

    protected static CancellableTask newTask() {
        return new CancellableTask(1, "transport", "indices:data/read/esql/stream/get", "test", TaskId.EMPTY_TASK_ID, Map.of());
    }

    protected static GetStreamResultRequest request(String id) {
        return new GetStreamResultRequest(id, 0, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT, null);
    }

    protected static GetStreamResultRequest request(String id, long offset, long maxRows, long maxBytes) {
        return new GetStreamResultRequest(id, offset, maxRows, maxBytes, null);
    }

    /** What a client sees: the first line, every row, and the last line. */
    protected record Read(Map<String, Object> first, List<List<Object>> rows, Map<String, Object> last) {
        List<Long> values() {
            List<Long> values = new ArrayList<>();
            for (List<Object> row : rows) {
                values.add(((Number) row.get(0)).longValue());
            }
            return values;
        }
    }

    protected GetStreamResultResponse response(StreamResultsService service, GetStreamResultRequest request) {
        PlainActionFuture<GetStreamResultResponse> future = new PlainActionFuture<>();
        service.retrieveResult(newTask(), request, future);
        return safeGet(future);
    }

    protected Read read(GetStreamResultRequest request) {
        return read(service(), request);
    }

    /** Runs the request and writes the whole body, like the HTTP layer does, one part at a time. */
    protected Read read(StreamResultsService service, GetStreamResultRequest request) {
        GetStreamResultResponse response = response(service, request);
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        StreamNdjsonBody body = new StreamNdjsonBody(channel, response);
        try {
            List<String> lines = new ArrayList<>();
            ChunkedRestResponseBodyPart part = body.firstPart();
            while (true) {
                String text = encode(part).utf8ToString();
                for (String line : text.split("\n")) {
                    if (line.isEmpty() == false) {
                        lines.add(line);
                    }
                }
                if (part.isLastPart()) {
                    break;
                }
                AtomicReference<ChunkedRestResponseBodyPart> next = new AtomicReference<>();
                part.getNextPart(ActionListener.wrap(next::set, e -> fail(e, "unexpected failure getting the next part")));
                part = next.get();
                assertNotNull("the next part must be delivered", part);
            }
            Map<String, Object> first = parseMap(lines.get(0));
            Map<String, Object> last = parseMap(lines.get(lines.size() - 1));
            List<List<Object>> rows = new ArrayList<>();
            for (String line : lines.subList(1, lines.size() - 1)) {
                rows.add(parseList(line));
            }
            return new Read(first, rows, last);
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            body.close();
        }
    }

    protected Exception readFailure(StreamResultsService service, GetStreamResultRequest request) {
        return safeAwaitFailure(GetStreamResultResponse.class, l -> service.retrieveResult(newTask(), request, l));
    }

    /** Records the calls the GET makes to the storage, in order, and passes them to the real store. */
    protected final class RecordingStore extends DelegatingStreamStorage {
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        final List<Long> requestedRows = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger getPagesCalls = new AtomicInteger();

        RecordingStore() {
            super(store);
        }

        @Override
        public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
            calls.add("metadata");
            super.getMetadata(streamId, listener);
        }

        @Override
        public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
            calls.add("pages");
            getPagesCalls.incrementAndGet();
            requestedRows.add(rows);
            super.getPages(streamId, offset, rows, listener);
        }
    }

    protected static BytesReference encode(ChunkedRestResponseBodyPart part) throws IOException {
        List<BytesReference> refs = new ArrayList<>();
        while (part.isPartComplete() == false) {
            refs.add(part.encodeChunk(randomIntBetween(2, 64), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        return CompositeBytesReference.of(refs.toArray(new BytesReference[0]));
    }

    protected static Map<String, Object> parseMap(String json) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }

    protected static List<Object> parseList(String json) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.list();
        }
    }

    protected static List<Long> range(long from, long to) {
        List<Long> values = new ArrayList<>();
        for (long i = from; i < to; i++) {
            values.add(i);
        }
        return values;
    }

    protected ThreadContext threadContext() {
        return threadPool.getThreadContext();
    }
}
