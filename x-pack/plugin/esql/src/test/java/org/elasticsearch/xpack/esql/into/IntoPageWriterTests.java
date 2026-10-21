/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The writer against a client that captures what it is asked to do and answers when the test says so, which is how
 * the driver sees it: pages go in, bulks come out, and the driver is held back or released as they are acknowledged.
 */
public class IntoPageWriterTests extends ESTestCase {

    private static final String DESTINATION = "dst";
    private static final ShardId SHARD = new ShardId(DESTINATION, "uuid", 0);

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
    private ThreadPool threadPool;
    private CapturingClient client;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        client = new CapturingClient(threadPool);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Stands in for the bulk and refresh actions, which need a running cluster: it keeps every request with its
     * listener so a test can answer them in whatever order and with whatever outcome it wants.
     */
    private static class CapturingClient extends NoOpClient {
        final List<BulkRequest> bulks = new ArrayList<>();
        final List<ActionListener<BulkResponse>> bulkListeners = new ArrayList<>();
        final List<RefreshRequest> refreshes = new ArrayList<>();
        final List<ActionListener<BroadcastResponse>> refreshListeners = new ArrayList<>();

        CapturingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == TransportBulkAction.TYPE) {
                bulks.add((BulkRequest) request);
                bulkListeners.add((ActionListener<BulkResponse>) listener);
            } else if (action == RefreshAction.INSTANCE) {
                refreshes.add((RefreshRequest) request);
                refreshListeners.add((ActionListener<BroadcastResponse>) listener);
            } else {
                throw new AssertionError("unexpected action " + action.name());
            }
        }

        /** Answers the oldest unanswered bulk, one outcome per item, in order. */
        void answer(Outcome... outcomes) {
            BulkRequest bulk = bulks.remove(0);
            ActionListener<BulkResponse> listener = bulkListeners.remove(0);
            assertThat(outcomes.length, equalTo(bulk.numberOfActions()));
            BulkItemResponse[] items = new BulkItemResponse[outcomes.length];
            for (int i = 0; i < outcomes.length; i++) {
                DocWriteRequest<?> request = bulk.requests().get(i);
                String id = request.id() == null ? "auto-" + i : request.id();
                items[i] = switch (outcomes[i]) {
                    case CREATED -> BulkItemResponse.success(i, request.opType(), new IndexResponse(SHARD, id, 0, 1, 1, true));
                    case UPDATED -> BulkItemResponse.success(i, request.opType(), new IndexResponse(SHARD, id, 0, 1, 2, false));
                    case CONFLICT -> BulkItemResponse.failure(
                        i,
                        request.opType(),
                        new BulkItemResponse.Failure(
                            DESTINATION,
                            id,
                            new VersionConflictEngineException(SHARD, id, "document already exists (current version [1])")
                        )
                    );
                    case FORBIDDEN -> BulkItemResponse.failure(
                        i,
                        request.opType(),
                        new BulkItemResponse.Failure(
                            DESTINATION,
                            id,
                            new ElasticsearchSecurityException("action [indices:data/write/index] is unauthorized", RestStatus.FORBIDDEN)
                        )
                    );
                    case REFUSED -> BulkItemResponse.failure(
                        i,
                        request.opType(),
                        new BulkItemResponse.Failure(DESTINATION, id, new IllegalArgumentException("mapper_parsing_exception: bad value"))
                    );
                };
            }
            listener.onResponse(new BulkResponse(items, 1));
        }

        void failOldestBulk(Exception e) {
            bulks.remove(0);
            bulkListeners.remove(0).onFailure(e);
        }

        void answerRefresh() {
            refreshes.remove(0);
            refreshListeners.remove(0).onResponse(new BroadcastResponse(1, 1, 0, List.of()));
        }
    }

    private enum Outcome {
        CREATED,
        UPDATED,
        CONFLICT,
        REFUSED,
        FORBIDDEN
    }

    private static final List<Attribute> COLUMNS = List.of(
        new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD),
        new ReferenceAttribute(Source.EMPTY, "cpu", DataType.DOUBLE)
    );

    private static final List<Attribute> COLUMNS_WITH_ID = List.of(
        new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD),
        new ReferenceAttribute(Source.EMPTY, "cpu", DataType.DOUBLE),
        new ReferenceAttribute(Source.EMPTY, IntoPageWriter.ID_COLUMN, DataType.KEYWORD)
    );

    /** The defaults with the refresh turned off, for tests that are not about the refresh and would otherwise wait for one. */
    private static final IntoOptions NO_REFRESH = new IntoOptions(IntoOptions.Collisions.ABORT, false, false, false);
    private static final IntoOptions KEEP_GOING = new IntoOptions(IntoOptions.Collisions.ABORT, false, true, false);

    private IntoPageWriter writer(IntoOptions options, List<Attribute> columns) {
        return new IntoPageWriter(client, DESTINATION, options, columns);
    }

    private static final IntoOptions REPLACE = new IntoOptions(IntoOptions.Collisions.REPLACE, false, false, false);

    /** A page of {@code host}/{@code cpu} rows; a null host or a NaN cpu is written as null. */
    private Page page(String[] hosts, double[] cpus) {
        try (
            BytesRefBlock.Builder host = blockFactory.newBytesRefBlockBuilder(hosts.length);
            DoubleBlock.Builder cpu = blockFactory.newDoubleBlockBuilder(cpus.length)
        ) {
            for (int i = 0; i < hosts.length; i++) {
                if (hosts[i] == null) {
                    host.appendNull();
                } else {
                    host.appendBytesRef(new BytesRef(hosts[i]));
                }
                if (Double.isNaN(cpus[i])) {
                    cpu.appendNull();
                } else {
                    cpu.appendDouble(cpus[i]);
                }
            }
            return new Page(host.build(), cpu.build());
        }
    }

    /** Like {@link #page} with an {@code _id} column last; a null id leaves the destination to generate one. */
    private Page pageWithIds(String[] ids, String[] hosts, double[] cpus) {
        Page rest = page(hosts, cpus);
        try (BytesRefBlock.Builder id = blockFactory.newBytesRefBlockBuilder(ids.length)) {
            for (String value : ids) {
                if (value == null) {
                    id.appendNull();
                } else {
                    id.appendBytesRef(new BytesRef(value));
                }
            }
            return rest.appendBlock(id.build());
        }
    }

    public void testRowsBecomeDocuments() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.accept(page(new String[] { "a", null }, new double[] { 1.5, Double.NaN }));

        assertThat(client.bulks, hasSize(1));
        List<DocWriteRequest<?>> requests = client.bulks.get(0).requests();
        assertThat(requests, hasSize(2));
        IndexRequest first = (IndexRequest) requests.get(0);
        assertThat(first.index(), equalTo(DESTINATION));
        assertThat(first.opType(), equalTo(DocWriteRequest.OpType.CREATE));
        assertThat(first.id(), nullValue());
        assertThat(first.sourceAsMap(), equalTo(Map.of("host", "a", "cpu", 1.5)));
        // Nulls are left out rather than written as explicit nulls.
        assertThat(((IndexRequest) requests.get(1)).sourceAsMap(), equalTo(Map.of()));
    }

    public void testIdColumnBecomesTheDocumentId() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "one", null }, new String[] { "a", "b" }, new double[] { 1, 2 }));

        List<DocWriteRequest<?>> requests = client.bulks.get(0).requests();
        assertThat(requests.get(0).id(), equalTo("one"));
        assertThat(((IndexRequest) requests.get(0)).sourceAsMap(), equalTo(Map.of("host", "a", "cpu", 1.0)));
        assertThat(requests.get(1).id(), nullValue());
    }

    public void testMultiValuedIdIsSkippedWithKeepGoing() {
        IntoPageWriter writer = writer(KEEP_GOING, COLUMNS_WITH_ID);
        Page rest = page(new String[] { "a", "b" }, new double[] { 1, 2 });
        try (BytesRefBlock.Builder id = blockFactory.newBytesRefBlockBuilder(2)) {
            id.beginPositionEntry().appendBytesRef(new BytesRef("x")).appendBytesRef(new BytesRef("y")).endPositionEntry();
            id.appendBytesRef(new BytesRef("two"));
            writer.accept(rest.appendBlock(id.build()));
        }
        List<DocWriteRequest<?>> requests = client.bulks.get(0).requests();
        assertThat(requests, hasSize(1));
        assertThat(requests.get(0).id(), equalTo("two"));
        client.answer(Outcome.CREATED);
        writer.finish();
        assertThat(writer.stats().total(), equalTo(2L));
        assertThat(writer.stats().created(), equalTo(1L));
        assertThat(writer.stats().failures(), equalTo(1L));
        assertThat(writer.stats().failureMessages().get(0), containsString("multi-valued [_id]"));
    }

    public void testReplaceUsesIndexOpType() {
        IntoPageWriter writer = writer(REPLACE, COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "one" }, new String[] { "a" }, new double[] { 1 }));
        assertThat(client.bulks.get(0).requests().get(0).opType(), equalTo(DocWriteRequest.OpType.INDEX));
        client.answer(Outcome.UPDATED);
        writer.finish();
        assertThat(writer.stats().updated(), equalTo(1L));
        assertThat(writer.stats().created(), equalTo(0L));
    }

    public void testDatesAreWrittenInUtc() {
        IntoPageWriter writer = writer(NO_REFRESH, List.of(new ReferenceAttribute(Source.EMPTY, "@timestamp", DataType.DATETIME)));
        writer.accept(new Page(blockFactory.newConstantLongBlockWith(1_750_000_000_000L, 1)));
        assertThat(
            ((IndexRequest) client.bulks.get(0).requests().get(0)).sourceAsMap(),
            equalTo(Map.of("@timestamp", "2025-06-15T15:06:40.000Z"))
        );
    }

    public void testMultiValuedFieldsBecomeLists() {
        IntoPageWriter writer = writer(NO_REFRESH, List.of(new ReferenceAttribute(Source.EMPTY, "tags", DataType.KEYWORD)));
        try (BytesRefBlock.Builder tags = blockFactory.newBytesRefBlockBuilder(2)) {
            tags.beginPositionEntry().appendBytesRef(new BytesRef("x")).appendBytesRef(new BytesRef("y")).endPositionEntry();
            writer.accept(new Page(tags.build()));
        }
        assertThat(((IndexRequest) client.bulks.get(0).requests().get(0)).sourceAsMap(), equalTo(Map.of("tags", List.of("x", "y"))));
    }

    /** With {@code collisions=keep} and {@code keep_going}, conflicts and refusals are counted and the rest carries on. */
    public void testKeepGoingCountsConflictsAndRefusalsAndCarriesOn() {
        IntoPageWriter writer = writer(new IntoOptions(IntoOptions.Collisions.KEEP, false, true, false), COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "1", "2", "3", "4" }, new String[] { "a", "b", "c", "d" }, new double[] { 1, 2, 3, 4 }));
        client.answer(Outcome.CREATED, Outcome.CONFLICT, Outcome.REFUSED, Outcome.CREATED);
        writer.accept(pageWithIds(new String[] { "5" }, new String[] { "e" }, new double[] { 5 }));
        assertThat("later pages are still written", client.bulks, hasSize(1));
        client.answer(Outcome.CREATED);
        writer.finish();

        assertTrue(writer.isFinished());
        assertThat(writer.failure(), nullValue());
        IntoStats stats = writer.stats();
        assertThat(stats.total(), equalTo(5L));
        assertThat(stats.created(), equalTo(3L));
        assertThat(stats.updated(), equalTo(0L));
        assertThat(stats.versionConflicts(), equalTo(1L));
        assertThat(stats.batches(), equalTo(2L));
        assertThat(stats.failures(), equalTo(1L));
        assertThat(stats.failureMessages(), hasSize(1));
        assertThat(stats.failureMessages().get(0), containsString("dst/3: "));
        assertThat(stats.failureMessages().get(0), containsString("bad value"));
    }

    /** With {@code collisions=abort}, the first conflict fails the query and nothing more is sent. */
    public void testAbortStopsOnTheFirstConflict() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "1", "2" }, new String[] { "a", "b" }, new double[] { 1, 2 }));
        client.answer(Outcome.CREATED, Outcome.CONFLICT);
        writer.accept(pageWithIds(new String[] { "3" }, new String[] { "c" }, new double[] { 3 }));
        assertThat("pages after the failure are dropped", client.bulks, hasSize(0));
        writer.finish();

        assertTrue(writer.isFinished());
        assertThat(writer.failure(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) writer.failure()).status(), equalTo(RestStatus.CONFLICT));
        assertThat(
            writer.failure().getMessage(),
            containsString("document [2] already exists; use \"collisions\": \"replace\" or \"collisions\": \"keep\"")
        );
    }

    /** A refused row fails the query unless {@code keep_going}: nothing after it is sent. */
    public void testRefusedRowFailsTheQueryByDefault() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.accept(page(new String[] { "a", "b" }, new double[] { 1, 2 }));
        client.answer(Outcome.CREATED, Outcome.REFUSED);
        writer.accept(page(new String[] { "c" }, new double[] { 3 }));
        assertThat("pages after the failure are dropped", client.bulks, hasSize(0));
        writer.finish();

        assertTrue(writer.isFinished());
        assertThat(writer.failure(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) writer.failure()).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(writer.failure().getMessage(), containsString("INTO [dst] failed: dst/auto-1: "));
        assertThat(
            writer.failure().getMessage(),
            containsString("bad value; use \"keep_going\": true to skip rows the destination refuses")
        );
        assertThat(writer.stats().failures(), equalTo(0L));
    }

    public void testMultiValuedIdFailsTheQueryByDefault() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS_WITH_ID);
        Page rest = page(new String[] { "a" }, new double[] { 1 });
        try (BytesRefBlock.Builder id = blockFactory.newBytesRefBlockBuilder(1)) {
            id.beginPositionEntry().appendBytesRef(new BytesRef("x")).appendBytesRef(new BytesRef("y")).endPositionEntry();
            writer.accept(rest.appendBlock(id.build()));
        }
        assertThat(client.bulks, hasSize(0));
        writer.finish();
        assertTrue(writer.isFinished());
        assertThat(writer.failure().getMessage(), containsString("row has a multi-valued [_id]"));
    }

    public void testFailureMessagesAreCapped() {
        IntoPageWriter writer = writer(KEEP_GOING, COLUMNS);
        int rows = IntoStats.MAX_FAILURE_MESSAGES + 5;
        String[] hosts = new String[rows];
        double[] cpus = new double[rows];
        Outcome[] outcomes = new Outcome[rows];
        for (int i = 0; i < rows; i++) {
            hosts[i] = "h" + i;
            cpus[i] = i;
            outcomes[i] = Outcome.REFUSED;
        }
        writer.accept(page(hosts, cpus));
        client.answer(outcomes);
        writer.finish();
        assertThat(writer.stats().failures(), equalTo((long) rows));
        assertThat(writer.stats().failureMessages(), hasSize(IntoStats.MAX_FAILURE_MESSAGES));
    }

    /** A bulk that fails as a whole was not written at all, so the query fails. */
    public void testWholeBulkFailureFailsTheQuery() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        Exception denied = new ElasticsearchStatusException("no write privilege", RestStatus.FORBIDDEN);
        client.failOldestBulk(denied);
        writer.finish();
        assertTrue(writer.isFinished());
        assertThat(writer.failure(), sameInstance(denied));
        assertThat(writer.stats().batches(), equalTo(0L));
    }

    /** Only so many bulks may be outstanding; the driver is parked until one comes back. */
    public void testBackpressure() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        for (int i = 0; i < IntoPageWriter.MAX_CONCURRENT_BULKS - 1; i++) {
            writer.accept(page(new String[] { "a" }, new double[] { 1 }));
            assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        }
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        var blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());
        assertThat(blocked.reason(), equalTo("INTO dst"));
        assertThat("the same listener is handed out while still blocked", writer.isBlocked().listener(), sameInstance(blocked.listener()));

        client.answer(Outcome.CREATED);
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
    }

    /** After the last page the driver waits until every bulk has come back, and no longer. */
    public void testFinishWaitsForInFlightBulks() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        writer.accept(page(new String[] { "b" }, new double[] { 2 }));
        writer.finish();
        assertFalse(writer.isFinished());
        var blocked = writer.isBlocked();
        assertFalse(blocked.listener().isDone());

        client.answer(Outcome.CREATED);
        assertFalse(writer.isFinished());
        assertFalse(blocked.listener().isDone());
        client.answer(Outcome.CREATED);
        assertTrue(writer.isFinished());
        assertTrue(blocked.listener().isDone());
        assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(client.refreshes, hasSize(0));
    }

    public void testFinishWithNothingWrittenIsImmediate() {
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        writer.accept(page(new String[0], new double[0]));
        assertThat("an empty page sends nothing", client.bulks, hasSize(0));
        writer.finish();
        // Nothing was written, but a refresh was asked for and nothing failed, so it still runs.
        assertFalse(writer.isFinished());
        client.answerRefresh();
        assertTrue(writer.isFinished());
        assertThat(writer.stats().total(), equalTo(0L));
    }

    /** With {@code refresh=false} the query completes as soon as the last bulk is acknowledged, and no refresh goes out. */
    public void testRefreshFalseSendsNoRefresh() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        writer.finish();
        assertFalse(writer.isFinished());
        client.answer(Outcome.CREATED);
        assertTrue(writer.isFinished());
        assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(client.refreshes, hasSize(0));
        assertThat(writer.stats().created(), equalTo(1L));
    }

    /** By default the refresh goes out once, after the last bulk is acknowledged, and the query waits for it. */
    public void testRefreshRunsAfterTheLastBulkByDefault() {
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        writer.finish();
        assertThat(client.refreshes, hasSize(0));

        client.answer(Outcome.CREATED);
        assertThat(client.refreshes, hasSize(1));
        assertThat(client.refreshes.get(0).indices(), equalTo(new String[] { DESTINATION }));
        assertFalse(writer.isFinished());
        assertFalse(writer.isBlocked().listener().isDone());

        client.answerRefresh();
        assertTrue(writer.isFinished());
        assertThat(writer.isBlocked(), sameInstance(Operator.NOT_BLOCKED));
        assertThat(client.refreshes, hasSize(0));
    }

    public void testRefreshIsSkippedAfterAFailure() {
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "1" }, new String[] { "a" }, new double[] { 1 }));
        writer.finish();
        client.answer(Outcome.CONFLICT);
        assertTrue(writer.isFinished());
        assertThat(client.refreshes, hasSize(0));
    }

    /**
     * The rows are written by the time the refresh runs, so its failure (a caller allowed to write but not to refresh,
     * say) does not fail the query, which a retry would answer by writing everything again. It is reported instead.
     */
    public void testRefreshFailureIsReportedNotFatal() {
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        writer.finish();
        client.answer(Outcome.CREATED);
        client.refreshListeners.remove(0).onFailure(new IllegalStateException("action [indices:admin/refresh] is unauthorized"));
        assertTrue(writer.isFinished());
        assertThat(writer.failure(), nullValue());
        assertThat(writer.stats().created(), equalTo(1L));
        assertThat(writer.refreshWarning(), containsString("could not refresh the destination"));
        assertThat(writer.refreshWarning(), containsString("indices:admin/refresh] is unauthorized"));
    }

    /** An item the caller may not write is not a row the destination dislikes: it fails the query even with keep_going. */
    public void testForbiddenItemFailsTheQueryEvenWithKeepGoing() {
        IntoPageWriter writer = writer(KEEP_GOING, COLUMNS);
        writer.accept(page(new String[] { "a", "b" }, new double[] { 1, 2 }));
        writer.finish();
        client.answer(Outcome.CREATED, Outcome.FORBIDDEN);
        assertTrue(writer.isFinished());
        assertThat(writer.failure(), instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) writer.failure()).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(writer.stats().failures(), equalTo(0L));
    }

    /**
     * A cancelled query stops offering pages but does not abandon the bulks already sent: the driver completes, with
     * the cancellation, only once every outstanding bulk has come back, so what they carry is written or refused by
     * the time the query reports and nothing lands afterwards. No refresh follows a cancelled write.
     */
    public void testCancelledDriverWaitsForOutstandingBulksAndSendsNoMore() throws Exception {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < IntoPageWriter.MAX_CONCURRENT_BULKS * 2; i++) {
            pages.add(page(new String[] { "host-" + i }, new double[] { i }));
        }
        SinkOperator sink = new OutputOperator.OutputOperatorFactory(List.of("host", "cpu"), Function.identity(), writer).get(
            driverContext
        );
        Driver driver = TestDriverFactory.create(driverContext, new CannedSourceOperator(pages.iterator()), List.of(), sink);
        PlainActionFuture<Void> completion = new PlainActionFuture<>();
        Driver.start(threadPool.getThreadContext(), threadPool.generic(), driver, 10_000, completion);

        // The driver is parked once the cap on outstanding bulks is reached.
        assertBusy(() -> assertThat(client.bulks, hasSize(IntoPageWriter.MAX_CONCURRENT_BULKS)));
        driver.cancel("test cancel");
        assertBusy(() -> assertTrue("the operators are drained and closed", driverContext.isFinished()));
        assertFalse("the driver waits for the bulks in flight", completion.isDone());
        assertThat("no bulk is sent after the cancel", client.bulks, hasSize(IntoPageWriter.MAX_CONCURRENT_BULKS));

        for (int i = 0; i < IntoPageWriter.MAX_CONCURRENT_BULKS; i++) {
            client.answer(Outcome.CREATED);
        }
        var e = expectThrows(TaskCancelledException.class, () -> completion.actionGet(10, TimeUnit.SECONDS));
        assertThat(e.getMessage(), equalTo("test cancel"));
        assertFalse(driverContext.hasPendingAsyncActions());
        assertThat(client.bulks, hasSize(0));
        assertThat("a cancelled write is not refreshed", client.refreshes, hasSize(0));
        assertThat(writer.stats().created(), equalTo((long) IntoPageWriter.MAX_CONCURRENT_BULKS));
    }

    /**
     * An async STOP fires the driver's stop hooks: the writer then drops every page offered, lets the bulks in flight
     * settle, and finishes with the statistics of what was written. The hook reports a cut only the first time, and
     * not at all once the last page has already been handed over.
     */
    public void testStopHookDropsLaterPagesAndFinishesWithWhatWasSent() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        writer.bind(driverContext);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        assertThat(client.bulks, hasSize(1));

        assertTrue("a write was cut", driverContext.runStopHooks());
        assertFalse("nothing left to cut", driverContext.runStopHooks());
        writer.accept(page(new String[] { "b" }, new double[] { 2 }));
        assertThat("no page is written after the stop", client.bulks, hasSize(1));

        writer.finish();
        assertFalse("the bulk in flight still settles", writer.isFinished());
        client.answer(Outcome.CREATED);
        assertThat("what was written is refreshed as usual", client.refreshes, hasSize(1));
        client.answerRefresh();
        assertTrue(writer.isFinished());
        assertThat(writer.failure(), nullValue());
        assertThat(writer.stats().total(), equalTo(1L));
        assertThat(writer.stats().created(), equalTo(1L));
    }

    public void testStopHookAfterTheLastPageCutsNothing() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        writer.bind(driverContext);
        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        writer.finish();
        assertFalse("the write is winding down on its own", driverContext.runStopHooks());
        client.answer(Outcome.CREATED);
        assertTrue(writer.isFinished());
        assertThat(writer.stats().created(), equalTo(1L));
    }

    /** Each outstanding request is an async action of the driver, so a cancelled query still waits for it. */
    public void testOutstandingRequestsAreRegisteredWithTheDriver() {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        IntoPageWriter writer = writer(IntoOptions.DEFAULT, COLUMNS);
        writer.bind(driverContext);
        assertFalse(driverContext.hasPendingAsyncActions());

        writer.accept(page(new String[] { "a" }, new double[] { 1 }));
        assertTrue(driverContext.hasPendingAsyncActions());
        writer.finish();
        client.answer(Outcome.CREATED);
        assertTrue("the refresh is outstanding now", driverContext.hasPendingAsyncActions());
        client.answerRefresh();
        assertFalse(driverContext.hasPendingAsyncActions());
    }

    public void testBindingToASecondDriverIsRefused() {
        IntoPageWriter writer = writer(NO_REFRESH, COLUMNS);
        DriverContext first = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        writer.bind(first);
        writer.bind(first);
        expectThrows(
            IllegalStateException.class,
            () -> writer.bind(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null))
        );
    }

    public void testStatsPage() {
        IntoPageWriter writer = writer(new IntoOptions(IntoOptions.Collisions.KEEP, false, true, false), COLUMNS_WITH_ID);
        writer.accept(pageWithIds(new String[] { "1", "2", "3" }, new String[] { "a", "b", "c" }, new double[] { 1, 2, 3 }));
        client.answer(Outcome.CREATED, Outcome.CONFLICT, Outcome.REFUSED);
        writer.finish();

        Page stats = writer.stats().toPage(blockFactory);
        try {
            assertThat(stats.getPositionCount(), equalTo(1));
            assertThat(stats.getBlockCount(), equalTo(IntoStats.ATTRIBUTES.size()));
            assertThat(longAt(stats, 0), equalTo(3L)); // total
            assertThat(longAt(stats, 1), equalTo(1L)); // created
            assertThat(longAt(stats, 2), equalTo(0L)); // updated
            assertThat(longAt(stats, 3), equalTo(1L)); // version_conflicts
            assertThat(longAt(stats, 4), equalTo(1L)); // batches
            assertThat(longAt(stats, 5), equalTo(1L)); // failures
            BytesRefBlock messages = stats.getBlock(6);
            assertThat(messages.getValueCount(0), equalTo(1));
            assertThat(messages.getBytesRef(messages.getFirstValueIndex(0), new BytesRef()).utf8ToString(), containsString("dst/3: "));
        } finally {
            stats.releaseBlocks();
        }

        Page empty = new IntoStats().toPage(blockFactory);
        try {
            assertThat(longAt(empty, 0), equalTo(0L));
            assertTrue("no failures means a null, not an empty list", empty.getBlock(6).isNull(0));
        } finally {
            empty.releaseBlocks();
        }
    }

    private static long longAt(Page page, int column) {
        Block block = page.getBlock(column);
        return ((org.elasticsearch.compute.data.LongBlock) block).getLong(0);
    }
}
