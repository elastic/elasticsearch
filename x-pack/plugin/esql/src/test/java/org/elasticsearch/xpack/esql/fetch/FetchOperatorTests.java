/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.fetch.FetchResponse.ShardResult;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The coordinator's fetch, with a client that answers each request on another thread the way a data node does: the
 * rows of each shard in request order, with values that name their document.
 */
public class FetchOperatorTests extends ComputeTestCase {
    private static final List<ElementType> FETCHED_TYPES = List.of(ElementType.BYTES_REF, ElementType.LONG);
    private static final List<DocOrigin> ORIGINS = List.of(origin("n1", 0), origin("n1", 1), origin("n2", 2), origin("n3", 3));

    private ThreadPool threadPool;
    /** The status of the operator the last {@link #run} drove, read just before it closed. */
    private FetchOperator.Status lastStatus;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    /**
     * The rows keep their order and their columns, and get the values of their own documents appended. Each page of the
     * cut sends one request to each node that holds some of its documents.
     */
    public void testAppendsTheFetchedColumnsInTheOrderOfTheCut() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        List<List<Ref>> input = randomInput();
        List<Page> output = run(blockFactory, client, input);
        try {
            assertThat(rows(output), equalTo(input.stream().flatMap(List::stream).toList()));
            assertThat(output.size(), equalTo(input.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        List<String> nodes = new ArrayList<>();
        for (List<Ref> page : input) {
            page.stream().map(ref -> ref.origin().nodeId()).distinct().forEach(nodes::add);
        }
        assertThat(
            "one request per node and page",
            client.requests.stream().map(Request::nodeId).toList(),
            containsInAnyOrder(nodes.toArray())
        );
    }

    /**
     * A page of the cut sends its requests as soon as it arrives, without waiting for the rest of the cut.
     */
    public void testFetchesEachPageAsItArrives() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Operator operator = factory(client).get(driverContext);
        try {
            operator.addInput(page(blockFactory, List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2))));
            assertThat(client.requests.stream().map(Request::nodeId).toList(), containsInAnyOrder("n1", "n2"));
            awaitUnblocked(operator);
            Page page = operator.getOutput();
            assertThat(page.getPositionCount(), equalTo(2));
            page.releaseBlocks();
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
    }

    /**
     * Each node can return response headers, like warnings. The operator gives the driver thread the headers of every
     * node, not only those of the node that answered last.
     */
    public void testMergesTheResponseHeadersOfEveryNode() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.responseHeader = "X-Fetched-By";
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            List<List<Ref>> input = List.of(
                List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2), new Ref(ORIGINS.get(3), 0, 3))
            );
            run(blockFactory, client, input).forEach(Page::releaseBlocks);
            assertThat(threadPool.getThreadContext().getResponseHeaders().get("X-Fetched-By"), containsInAnyOrder("n1", "n2", "n3"));
        }
    }

    /**
     * A cut without rows, like the ones {@code EXPLAIN} runs over empty sources, sends nothing and keeps the schema of its
     * pages.
     */
    public void testAnEmptyCutSendsNothing() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        List<Page> output = run(blockFactory, client, List.of(List.of()));
        try {
            assertThat(output.size(), equalTo(1));
            assertThat(output.getFirst().getPositionCount(), equalTo(0));
            assertThat(output.getFirst().getBlockCount(), equalTo(1 + FETCHED_TYPES.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(run(blockFactory, client, List.of()), empty());
        assertThat(client.requests, empty());
    }

    /**
     * For now any failure of a fetch fails the query: a shard whose context is gone, a request a node rejected, a node
     * that left.
     */
    public void testAnyFailureFailsTheFetch() throws Exception {
        BlockFactory blockFactory = blockFactory();
        List<List<Ref>> input = List.of(List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2)));

        FakeClient missingShard = new FakeClient(blockFactory);
        missingShard.failingShard = ORIGINS.get(2).contextId();
        Exception e = expectThrows(Exception.class, () -> run(blockFactory, missingShard, input));
        assertThat(e, instanceOf(SearchContextMissingException.class));

        FakeClient rejected = new FakeClient(blockFactory);
        rejected.failingNode = "n1";
        e = expectThrows(Exception.class, () -> run(blockFactory, rejected, input));
        assertThat(e.getMessage(), equalTo("rejected by [n1]"));

        FakeClient nodeLeft = new FakeClient(blockFactory);
        nodeLeft.goneNode = "n2";
        e = expectThrows(Exception.class, () -> run(blockFactory, nodeLeft, input));
        assertThat(e, instanceOf(NoNodeAvailableException.class));

        FakeClient notSent = new FakeClient(blockFactory);
        notSent.throwingNode = "n1";
        e = expectThrows(Exception.class, () -> run(blockFactory, notSent, input));
        assertThat(e.getMessage(), equalTo("can't send to [n1]"));
        assertThat("the query fails, so the other nodes get nothing to load", notSent.requests, empty());

        FakeClient missingRows = new FakeClient(blockFactory);
        missingRows.shortNode = "n2";
        e = expectThrows(Exception.class, () -> run(blockFactory, missingRows, input));
        assertThat(e.getMessage(), equalTo("node [n2] returned [0] rows for [1] documents"));
        // the test case checks that the failures released every page
    }

    /**
     * A driver that asks for output once the responses completed gets the failure, not the rows of the other nodes.
     */
    public void testTheFailureComesBeforeTheRows() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.failingNode = "n2";
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Operator operator = factory(client).get(driverContext);
        try {
            operator.addInput(page(blockFactory, List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2))));
            operator.finish();
            awaitUnblocked(operator);
            Exception e = expectThrows(Exception.class, operator::getOutput);
            assertThat(e.getMessage(), equalTo("rejected by [n2]"));
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
    }

    /**
     * When a listener throws, the transport completes it again with the failure. The second completion changes nothing.
     */
    public void testASecondCompletionIsIgnored() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.completeTwice = true;
        List<List<Ref>> input = List.of(List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2)));
        List<Page> output = run(blockFactory, client, input);
        try {
            assertThat(rows(output), equalTo(input.getFirst()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A query that ends before the driver gathers, because something else failed, releases what the nodes returned.
     */
    public void testClosingBeforeTheGatherReleasesTheResponses() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Operator operator = factory(client).get(driverContext);
        operator.addInput(page(blockFactory, List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2))));
        operator.finish();
        awaitUnblocked(operator);

        operator.close();
        awaitResponses(driverContext);
        // the test case checks that closing released the pages
    }

    /**
     * A response that arrives after the operator closed, for example because the query was cancelled, releases its
     * own pages.
     */
    public void testALateResponseReleasesItsPages() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        CountDownLatch release = new CountDownLatch(1);
        client.holdResponses = release;
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Operator operator = factory(client).get(driverContext);
        operator.addInput(page(blockFactory, List.of(new Ref(ORIGINS.get(0), 0, 1))));
        operator.finish();
        assertFalse(operator.isBlocked().listener().isDone());

        operator.close();
        release.countDown();
        awaitResponses(driverContext);
        // the test case checks that the response released its pages
    }

    /**
     * The status shows how the cut became requests, and the time each node reported.
     */
    public void testStatusShowsTheRequests() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        List<List<Ref>> input = List.of(
            List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(1), 0, 2), new Ref(ORIGINS.get(0), 0, 1)),
            List.of(new Ref(ORIGINS.get(2), 3, 4))
        );
        run(blockFactory, client, input).forEach(Page::releaseBlocks);

        assertThat(lastStatus.receivedPages(), equalTo(2L));
        assertThat(lastStatus.completedPages(), equalTo(2L));
        assertThat(lastStatus.rowsReceived(), equalTo(4L));
        assertThat("rows of the same document fetch it once", lastStatus.documents(), equalTo(3L));
        assertThat(lastStatus.pagesEmitted(), equalTo(2));
        assertThat(lastStatus.requests().stream().map(FetchOperator.NodeRequest::node).toList(), containsInAnyOrder("n1", "n2"));
        for (FetchOperator.NodeRequest request : lastStatus.requests()) {
            assertThat(request.shards(), equalTo(request.node().equals("n1") ? 2 : 1));
            assertThat(request.documents(), equalTo(request.node().equals("n1") ? 2 : 1));
            assertThat(request.tookNanos(), equalTo(FakeClient.TOOK_NANOS));
            assertThat(request.setupNanos(), equalTo(FakeClient.SETUP_NANOS));
            assertThat(request.requestNanos(), greaterThan(0L));
        }
        assertThat(lastStatus.processNanos(), greaterThan(0L));
        assertThat(lastStatus.planNanos(), greaterThan(0L));
        assertThat(lastStatus.gatherNanos(), greaterThan(0L));
    }

    public void testDescribe() {
        assertThat(
            factory(new FakeClient(blockFactory())).describe(),
            equalTo("FetchOperator[docChannel=0, fetchedTypes=[BYTES_REF, LONG]]")
        );
    }

    /**
     * Runs the cut through the operator and returns its output, like a driver does: it adds a page while the operator
     * takes one, waits while it is blocked, and finishes it after the last page.
     */
    private List<Page> run(BlockFactory blockFactory, FakeClient client, List<List<Ref>> input) throws Exception {
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Page> output = new ArrayList<>();
        Operator operator = factory(client).get(driverContext);
        try {
            Iterator<List<Ref>> pages = input.iterator();
            while (operator.isFinished() == false) {
                if (pages.hasNext() && operator.needsInput()) {
                    operator.addInput(page(blockFactory, pages.next()));
                    continue;
                }
                if (pages.hasNext() == false) {
                    operator.finish();
                }
                IsBlockedResult blocked = operator.isBlocked();
                if (blocked.listener().isDone() == false) {
                    PlainActionFuture<Void> unblocked = new PlainActionFuture<>();
                    blocked.listener().addListener(unblocked);
                    unblocked.actionGet(TimeValue.timeValueSeconds(10));
                    continue;
                }
                Page page = operator.getOutput();
                if (page != null) {
                    output.add(page);
                }
            }
            return output;
        } catch (Exception e) {
            output.forEach(Page::releaseBlocks);
            throw e;
        } finally {
            lastStatus = (FetchOperator.Status) operator.status();
            operator.close();
            awaitResponses(driverContext);
        }
    }

    private static void awaitUnblocked(Operator operator) {
        PlainActionFuture<Void> unblocked = new PlainActionFuture<>();
        operator.isBlocked().listener().addListener(unblocked);
        unblocked.actionGet(TimeValue.timeValueSeconds(10));
    }

    /**
     * Waits for the responses still on their way, like a driver does before it completes.
     */
    private static void awaitResponses(DriverContext driverContext) {
        driverContext.finish();
        PlainActionFuture<Void> responded = new PlainActionFuture<>();
        driverContext.waitForAsyncActions(responded);
        responded.actionGet(TimeValue.timeValueSeconds(10));
    }

    private static FetchOperator.Factory factory(FetchOperator.Client client) {
        return new FetchOperator.Factory(0, FETCHED_TYPES, null, between(1, FetchOperator.MAX_OUTSTANDING_PAGES), client);
    }

    private record Ref(DocOrigin origin, int segment, int doc) {
        String id() {
            return "doc-" + origin.shardId().id() + "-" + segment + "-" + doc;
        }

        long value() {
            return origin.shardId().id() * 1_000_000L + segment * 1_000L + doc;
        }
    }

    private record Request(String nodeId, List<FetchRequest.ShardDocs> shards) {}

    /**
     * Answers like a data node, on another thread.
     */
    private class FakeClient implements FetchOperator.Client {
        static final long TOOK_NANOS = 5_000;
        static final long SETUP_NANOS = 1_000;
        private final BlockFactory blockFactory;
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private ShardSearchContextId failingShard;
        private String failingNode;
        private String goneNode;
        /** The request to this node fails before it is sent. */
        private String throwingNode;
        /** This node returns no row for the first shard it loads. */
        private String shortNode;
        private boolean completeTwice;
        private CountDownLatch holdResponses;
        /** Each node answers with this response header, its id as the value. */
        private String responseHeader;

        FakeClient(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        public ThreadContext threadContext() {
            return threadPool.getThreadContext();
        }

        @Override
        public DiscoveryNode node(String clusterAlias, String nodeId) {
            return nodeId.equals(goneNode) ? null : DiscoveryNodeUtils.builder(nodeId).name(nodeId).build();
        }

        @Override
        public void fetch(
            DiscoveryNode node,
            String clusterAlias,
            List<FetchRequest.ShardDocs> shards,
            PhysicalPlan fetchPlan,
            ActionListener<FetchResponse> listener
        ) {
            if (node.getId().equals(throwingNode)) {
                throw new IllegalStateException("can't send to [" + node.getId() + "]");
            }
            requests.add(new Request(node.getId(), shards));
            threadPool.generic().execute(() -> {
                try {
                    if (holdResponses != null) {
                        holdResponses.await(10, TimeUnit.SECONDS);
                    }
                    if (responseHeader != null) {
                        threadPool.getThreadContext().addResponseHeader(responseHeader, node.getId());
                    }
                    if (node.getId().equals(failingNode)) {
                        listener.onFailure(new IllegalStateException("rejected by [" + node.getId() + "]"));
                        return;
                    }
                    FetchResponse response = respond(shards, node.getId().equals(shortNode));
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                    if (completeTwice) {
                        listener.onFailure(new IllegalStateException("a second completion"));
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private FetchResponse respond(List<FetchRequest.ShardDocs> shards, boolean dropFirstShard) {
            List<ShardResult> results = new ArrayList<>();
            List<Page> pages = new ArrayList<>();
            for (FetchRequest.ShardDocs shard : shards) {
                if (dropFirstShard && results.isEmpty()) {
                    results.add(ShardResult.succeeded(shard.shardId(), 0));
                    continue;
                }
                if (shard.contextId().equals(failingShard)) {
                    results.add(ShardResult.failed(shard.shardId(), new SearchContextMissingException(shard.contextId())));
                    continue;
                }
                DocOrigin origin = ORIGINS.stream().filter(o -> o.contextId().equals(shard.contextId())).findFirst().orElseThrow();
                try (
                    BytesRefBlock.Builder ids = blockFactory.newBytesRefBlockBuilder(shard.docCount());
                    LongBlock.Builder values = blockFactory.newLongBlockBuilder(shard.docCount())
                ) {
                    for (int d = 0; d < shard.docCount(); d++) {
                        Ref ref = new Ref(origin, shard.segments()[d], shard.docs()[d]);
                        ids.appendBytesRef(new BytesRef(ref.id()));
                        values.appendLong(ref.value());
                    }
                    pages.add(new Page(ids.build(), values.build()));
                }
                results.add(ShardResult.succeeded(shard.shardId(), shard.docCount()));
            }
            return new FetchResponse(blockFactory, results, pages, DriverCompletionInfo.EMPTY, TOOK_NANOS, SETUP_NANOS);
        }
    }

    private static List<List<Ref>> randomInput() {
        List<List<Ref>> input = new ArrayList<>();
        int pages = between(1, 4);
        for (int p = 0; p < pages; p++) {
            List<Ref> refs = new ArrayList<>();
            int rows = between(0, 40);
            for (int r = 0; r < rows; r++) {
                refs.add(new Ref(randomFrom(ORIGINS), between(0, 3), between(0, 500)));
            }
            input.add(refs);
        }
        return input;
    }

    private static Page page(BlockFactory blockFactory, List<Ref> refs) {
        List<DocOrigin> origins = new ArrayList<>();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, refs.size())) {
            for (Ref ref : refs) {
                if (origins.contains(ref.origin()) == false) {
                    origins.add(ref.origin());
                }
                builder.appendShard(origins.indexOf(ref.origin())).appendSegment(ref.segment()).appendDoc(ref.doc());
            }
            // rows may share a document
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins)).mayContainDuplicates()));
        }
    }

    /**
     * The document of each output row, read from the fetched columns, after checking that they agree with its document.
     */
    private static List<Ref> rows(List<Page> pages) {
        List<Ref> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            DocVector docs = page.<DocBlock>getBlock(0).asVector();
            BytesRefBlock ids = page.getBlock(1);
            LongBlock values = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                Ref ref = new Ref(docs.origin(p), docs.segments().getInt(p), docs.docs().getInt(p));
                assertThat(ids.getBytesRef(ids.getFirstValueIndex(p), scratch).utf8ToString(), equalTo(ref.id()));
                assertThat(values.getLong(values.getFirstValueIndex(p)), equalTo(ref.value()));
                rows.add(ref);
            }
        }
        return Collections.unmodifiableList(rows);
    }

    private static DocOrigin origin(String nodeId, int shard) {
        return new DocOrigin("", nodeId, new ShardId("index", "uuid", shard), new ShardSearchContextId("session", shard));
    }
}
