/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.streaming.EsqlStreamResultsIndexDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

/**
 * The counterpart of {@code AsyncEsqlQueryActionIT} for {@code GET /_query/stream/{id}}, through the REST layer on a real node.
 * A test writer adds pages to the in-memory stream storage, standing in for the query, and the GET reads them back over HTTP.
 * The in-memory storage is per node, so the cluster has a single node.
 * <p>
 * The tests do not sleep. A test that changes a stream while a GET waits for rows first waits until the storage has seen a read of the
 * GET that found no rows, which is how a waiting GET looks from the storage.
 */
@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 1, numClientNodes = 0)
public class StreamEsqlGetResultActionIT extends AbstractEsqlIntegTestCase {

    @BeforeClass
    public static void onlyWhereTheEndpointExists() {
        assumeTrue("the stream GET is registered only where streams can be created", RestEsqlStreamQueryAction.ENABLED);
        assumeTrue("the stream GET is registered with this feature flag", EsqlStreamResultsIndexDescriptor.FEATURE_FLAG.isEnabled());
    }

    private StreamTestEsqlPlugin.ObservedStore store;
    private BlockFactory pageFactory;
    private RestClient restClient;
    private final List<String> streams = new ArrayList<>();

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The stream GET reads from the in-memory storage, through StreamTestEsqlPlugin, which replaces its transport action.
        // StreamTestEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense. Both would register the ES|QL settings and actions,
        // so the parent has to come out.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(StreamTestEsqlPlugin.class);
        return plugins;
    }

    @Before
    public void setUpStreams() throws IOException {
        store = StreamTestEsqlPlugin.STORE;
        store.resetEmptyReads();
        // The cluster may have dedicated master nodes. The HTTP client must use a data node, which is where the stream GET runs.
        String node = internalCluster().getRandomDataNodeName();
        pageFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking()
        ).build();
        restClient = createRestClient(node);
    }

    @After
    public void tearDownStreams() throws IOException {
        for (String id : streams) {
            awaitVoid(l -> store.delete(id, l));
        }
        restClient.close();
        assertThat("every page was released", pageFactory.breaker().getUsed(), equalTo(0L));
    }

    /** Runs one storage call and fails the test if it does not complete, or completes with a failure. */
    private static void awaitVoid(CheckedConsumer<ActionListener<Void>, ?> call) {
        safeAwait(call);
    }

    private final class Writer {
        final String id;
        long rows = 0;
        int pages = 0;

        Writer(String id) {
            this.id = id;
        }

        Writer append(int pageRows) {
            long[] values = new long[pageRows];
            for (int i = 0; i < pageRows; i++) {
                values[i] = rows + i;
            }
            Page page = new Page(pageFactory.newLongArrayVector(values, pageRows).asBlock());
            // shallow copies of the stored page are read on several threads, which needs thread safe reference counts
            page.allowPassingToDifferentDriver();
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

    private Writer createStream(String id) {
        streams.add(id);
        List<ColumnInfo> columns = List.of(new ColumnInfoImpl("n", "long", null));
        awaitVoid(
            l -> store.create(id, new TaskId("node", 1), Map.of(), columns, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1), l)
        );
        return new Writer(id);
    }

    /** What a client sees: the first line, the rows, and the last line. */
    private record Read(Map<String, Object> first, List<Long> values, Map<String, Object> last) {}

    private Read get(String id, String... params) throws IOException {
        Request request = new Request("GET", "/_query/stream/" + id);
        for (int i = 0; i < params.length; i += 2) {
            request.addParameter(params[i], params[i + 1]);
        }
        Response response = restClient.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), startsWith("application/x-ndjson"));
        String[] lines = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8).split("\n");
        List<Long> values = new ArrayList<>();
        for (int i = 1; i < lines.length - 1; i++) {
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, lines[i])) {
                values.add(((Number) parser.list().get(0)).longValue());
            }
        }
        return new Read(parse(lines[0]), values, parse(lines[lines.length - 1]));
    }

    private static Map<String, Object> parse(String line) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line)) {
            return parser.map();
        }
    }

    private static List<Long> range(long from, long to) {
        List<Long> values = new ArrayList<>();
        for (long i = from; i < to; i++) {
            values.add(i);
        }
        return values;
    }

    private static int status(ResponseException e) {
        return e.getResponse().getStatusLine().getStatusCode();
    }

    // ---- reading a stored result

    /** The story of {@code testBasicAsyncExecution}: a short timeout while running, then rows, then the end, again, delete, not found. */
    public void testBasicStreamGet() throws Exception {
        Writer writer = createStream("basic");

        // nothing is stored yet: a short wait_timeout returns the running status with no rows, not an error
        Read empty = get("basic", "wait_timeout", "50ms");
        assertThat(empty.first().get("id"), equalTo("basic"));
        assertThat(empty.first().get("query_status"), equalTo("running"));
        assertThat(empty.values(), equalTo(List.of()));
        assertThat(empty.last().get("complete"), equalTo(false));
        assertThat(empty.last().get("next_offset"), equalTo(0));

        // Rows are stored while the stream is still running: they come back at once, even with a long wait_timeout. Nothing else
        // would end this wait before the 60 seconds, so a GET that waits here is seen in the time it takes.
        writer.append(4).append(3);
        long start = System.nanoTime();
        Read running = get("basic", "wait_timeout", "60s");
        assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), lessThan(30L));
        assertThat(running.values(), equalTo(range(0, 7)));
        assertThat(running.first().get("query_status"), equalTo("running"));
        assertThat(running.last().get("complete"), equalTo(false));
        assertThat(running.last().get("next_offset"), equalTo(7));

        // the stream finishes: the end of the result, from the next offset
        writer.append(5).complete();
        Read end = get("basic", "offset", "7");
        assertThat(end.values(), equalTo(range(7, 12)));
        assertThat(end.first().get("query_status"), equalTo("completed"));
        assertThat(end.last().get("complete"), equalTo(true));
        assertThat(end.last().get("next_offset"), equalTo(12));

        // the stored result can be read again, from any offset
        assertThat(get("basic").values(), equalTo(range(0, 12)));
        assertThat(get("basic", "offset", "5", "max_rows", "3").values(), equalTo(range(5, 8)));

        // deleted: no longer found
        awaitVoid(l -> store.delete("basic", l));
        ResponseException e = expectThrows(ResponseException.class, () -> get("basic"));
        assertThat(status(e), equalTo(404));
    }

    public void testStatusOnlyAndLimits() throws Exception {
        createStream("limits").append(4).append(3).append(5).complete();
        Read statusOnly = get("limits", "max_rows", "0");
        assertThat(statusOnly.values(), equalTo(List.of()));
        assertThat(statusOnly.first().get("query_status"), equalTo("completed"));
        assertThat(get("limits", "max_bytes", "0").values(), equalTo(List.of()));
        assertThat(get("limits", "max_rows", "5").values(), equalTo(range(0, 5)));
        // the rows [0] to [9] are 4 bytes each: 10 bytes is reached at the third row
        assertThat(get("limits", "max_bytes", "10b").values(), equalTo(range(0, 3)));
        assertThat(get("limits", "max_rows", "100", "max_bytes", "5b").values(), equalTo(range(0, 2)));
        assertThat(get("limits").values(), equalTo(range(0, 12)));
    }

    public void testResumeLoop() throws Exception {
        Writer writer = createStream("resume");
        for (int i = 0; i < 12; i++) {
            writer.append(randomIntBetween(1, 40));
        }
        writer.complete();
        List<Long> all = new ArrayList<>();
        long offset = 0;
        boolean complete = false;
        int calls = 0;
        while (complete == false) {
            Read read = get("resume", "offset", Long.toString(offset), "max_rows", Integer.toString(randomIntBetween(1, 50)));
            all.addAll(read.values());
            offset = ((Number) read.last().get("next_offset")).longValue();
            complete = (boolean) read.last().get("complete");
            assertThat(calls++ < 1000, equalTo(true));
        }
        assertThat(all, equalTo(range(0, writer.rows)));
    }

    public void testConcurrentGetsOfOneStream() throws Exception {
        Writer writer = createStream("concurrent");
        for (int i = 0; i < 10; i++) {
            writer.append(randomIntBetween(1, 30));
        }
        writer.complete();
        int clients = 8;
        try (ExecutorService executor = Executors.newFixedThreadPool(clients)) {
            List<Future<Read>> reads = new ArrayList<>();
            List<Long> offsets = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                long offset = randomLongBetween(0, writer.rows);
                offsets.add(offset);
                reads.add(executor.submit(() -> get("concurrent", "offset", Long.toString(offset))));
            }
            for (int i = 0; i < clients; i++) {
                assertThat(reads.get(i).get(30, TimeUnit.SECONDS).values(), equalTo(range(offsets.get(i), writer.rows)));
            }
        }
    }

    // ---- waiting for rows

    public void testWaitForRowsReturnsWhenRowsArrive() throws Exception {
        Writer writer = createStream("wait");
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<Read> read = executor.submit(() -> get("wait", "wait_timeout", "60s"));
            store.awaitEmptyRead("wait");
            assertThat("still waiting for the first rows", read.isDone(), equalTo(false));
            writer.append(6);
            Read result = read.get(30, TimeUnit.SECONDS);
            assertThat(result.values(), equalTo(range(0, 6)));
            assertThat(result.first().get("query_status"), equalTo("running"));
            assertThat(result.last().get("complete"), equalTo(false));
        }
    }

    public void testWaitEndsWhenTheStreamCompletesWithoutMoreRows() throws Exception {
        Writer writer = createStream("done").append(3);
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<Read> read = executor.submit(() -> get("done", "offset", "3", "wait_timeout", "60s"));
            store.awaitEmptyRead("done");
            writer.complete();
            Read result = read.get(30, TimeUnit.SECONDS);
            assertThat(result.values(), equalTo(List.of()));
            assertThat(result.first().get("query_status"), equalTo("completed"));
            assertThat(result.last().get("complete"), equalTo(true));
        }
    }

    public void testWaitingGetsDoNotBlockOtherRequests() throws Exception {
        // One stream for every waiter, so that the storage can tell when each of them is waiting. The REST client opens at most 10
        // connections to a node, so with more waiters than that the rest would queue in the client, and so would the request below.
        int waiters = 8;
        List<Writer> writers = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            writers.add(createStream("blocked-" + i));
        }
        try (ExecutorService executor = Executors.newFixedThreadPool(waiters)) {
            List<Future<Read>> waiting = new ArrayList<>();
            for (Writer writer : writers) {
                waiting.add(executor.submit(() -> get(writer.id, "wait_timeout", "60s")));
            }
            for (Writer writer : writers) {
                store.awaitEmptyRead(writer.id);
            }
            // while every waiter waits, an ordinary request is still served
            Response health = restClient.performRequest(new Request("GET", "/_cluster/health"));
            assertThat(health.getStatusLine().getStatusCode(), equalTo(200));
            // rows end the waits
            for (Writer writer : writers) {
                writer.append(1);
            }
            for (Future<Read> read : waiting) {
                Read result = read.get(30, TimeUnit.SECONDS);
                assertThat(result.values(), equalTo(List.of(0L)));
                assertThat(result.last().get("complete"), equalTo(false));
            }
        }
    }

    // ---- failures and errors

    public void testFailedStream() throws Exception {
        createStream("failed").append(4).fail(new IllegalStateException("boom"));
        Read read = get("failed");
        assertThat(read.values(), equalTo(range(0, 4)));
        assertThat(read.first().get("query_status"), equalTo("failed"));
        assertThat(read.last().get("status"), equalTo(500));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(4));

        // nothing left to read at the offset: a normal HTTP error
        ResponseException e = expectThrows(ResponseException.class, () -> get("failed", "offset", "4"));
        assertThat(status(e), equalTo(500));
    }

    public void testErrors() throws Exception {
        createStream("errors").append(3).complete();
        assertThat(status(expectThrows(ResponseException.class, () -> get("unknown"))), equalTo(404));
        assertThat(status(expectThrows(ResponseException.class, () -> get("errors", "offset", "-1"))), equalTo(400));
        assertThat(status(expectThrows(ResponseException.class, () -> get("errors", "offset", "99"))), equalTo(400));
        assertThat(status(expectThrows(ResponseException.class, () -> get("errors", "max_rows", "-1"))), equalTo(400));
        assertThat(status(expectThrows(ResponseException.class, () -> get("errors", "max_bytes", "lots"))), equalTo(400));
        // keep_alive is not supported in this iteration. (format is accepted by every REST handler and ignored: the response is NDJSON.)
        assertThat(status(expectThrows(ResponseException.class, () -> get("errors", "keep_alive", "1h"))), equalTo(400));
    }

    public void testExpiredStreamIsNotFound() throws Exception {
        createStream("expiring").append(3).complete();
        assertThat(get("expiring").values(), equalTo(range(0, 3)));
        // move the expiration into the past, instead of waiting for a short one to pass
        awaitVoid(l -> store.extendTtl("expiring", System.currentTimeMillis() - 1, l));
        assertThat(status(expectThrows(ResponseException.class, () -> get("expiring"))), equalTo(404));
    }
}
