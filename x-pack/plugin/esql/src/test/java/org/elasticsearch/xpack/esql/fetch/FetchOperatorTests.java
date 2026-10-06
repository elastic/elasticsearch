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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The coordinator's fetch, with a client that answers each request on another thread the way a data node does: the
 * rows of each shard in request order, with values that name their document.
 */
public class FetchOperatorTests extends ComputeTestCase {
    private static final List<ElementType> FETCHED_TYPES = List.of(ElementType.BYTES_REF, ElementType.LONG);
    private static final List<DocRefOrigin> ORIGINS = List.of(origin("n1", 0), origin("n1", 1), origin("n2", 2), origin("n3", 3));

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    /**
     * The rows keep their order and their columns, and get the values of their own documents appended.
     */
    public void testAppendsTheFetchedColumnsInTheOrderOfTheCut() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        List<List<Ref>> input = randomInput();
        List<Page> output = run(blockFactory, client, true, input);
        try {
            assertThat(rows(output), equalTo(input.stream().flatMap(List::stream).toList()));
            assertThat(output.size(), equalTo(input.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        Set<String> nodes = new HashSet<>();
        input.forEach(page -> page.forEach(ref -> nodes.add(ref.origin().nodeId())));
        assertThat("one request per node", client.requests.stream().map(Request::nodeId).toList(), containsInAnyOrder(nodes.toArray()));
    }

    /**
     * The last fetch of a query frees the contexts it read once each node answered. An earlier one leaves them for the
     * fetch that follows.
     */
    public void testOnlyTheLastStageFreesTheContexts() throws Exception {
        for (boolean finalStage : new boolean[] { true, false }) {
            BlockFactory blockFactory = blockFactory();
            FakeClient client = new FakeClient(blockFactory);
            List<Page> output = run(
                blockFactory,
                client,
                finalStage,
                List.of(List.of(new Ref(ORIGINS.get(0), 1, 1), new Ref(ORIGINS.get(1), 0, 3)))
            );
            output.forEach(Page::releaseBlocks);
            Request request = client.requests.getFirst();
            List<ShardSearchContextId> expected = finalStage
                ? request.shards().stream().map(FetchRequest.ShardDocs::contextId).toList()
                : List.of();
            assertThat(request.releaseAfter(), equalTo(expected));
        }
    }

    /**
     * A cut without rows, like the ones {@code EXPLAIN} runs over empty sources, sends nothing and keeps the schema of its
     * pages.
     */
    public void testAnEmptyCutSendsNothing() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        List<Page> output = run(blockFactory, client, true, List.of(List.of()));
        try {
            assertThat(output.size(), equalTo(1));
            assertThat(output.getFirst().getPositionCount(), equalTo(0));
            assertThat(output.getFirst().getBlockCount(), equalTo(1 + FETCHED_TYPES.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(run(blockFactory, client, true, List.of()), empty());
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
        Exception e = expectThrows(Exception.class, () -> run(blockFactory, missingShard, true, input));
        assertThat(e, instanceOf(SearchContextMissingException.class));

        FakeClient rejected = new FakeClient(blockFactory);
        rejected.failingNode = "n1";
        e = expectThrows(Exception.class, () -> run(blockFactory, rejected, true, input));
        assertThat(e.getMessage(), equalTo("rejected by [n1]"));

        FakeClient nodeLeft = new FakeClient(blockFactory);
        nodeLeft.goneNode = "n2";
        e = expectThrows(Exception.class, () -> run(blockFactory, nodeLeft, true, input));
        assertThat(e, instanceOf(NoNodeAvailableException.class));

        FakeClient notSent = new FakeClient(blockFactory);
        notSent.throwingNode = "n1";
        e = expectThrows(Exception.class, () -> run(blockFactory, notSent, true, input));
        assertThat(e.getMessage(), equalTo("can't send to [n1]"));
        assertThat("the query fails, so the other nodes get nothing to load", notSent.requests, empty());

        FakeClient missingRows = new FakeClient(blockFactory);
        missingRows.shortNode = "n2";
        e = expectThrows(Exception.class, () -> run(blockFactory, missingRows, true, input));
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
        Operator operator = factory(client, true).get(driverContext);
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
        List<Page> output = run(blockFactory, client, true, input);
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
        Operator operator = factory(client, true).get(driverContext);
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
        Operator operator = factory(client, true).get(driverContext);
        operator.addInput(page(blockFactory, List.of(new Ref(ORIGINS.get(0), 0, 1))));
        operator.finish();
        assertFalse(operator.isBlocked().listener().isDone());

        operator.close();
        release.countDown();
        awaitResponses(driverContext);
        // the test case checks that the response released its pages
    }

    public void testDescribe() {
        assertThat(
            factory(new FakeClient(blockFactory()), true).describe(),
            equalTo("FetchOperator[docRefChannel=0, fetchedTypes=[BYTES_REF, LONG]]")
        );
    }

    /**
     * Runs the cut through the operator and returns its output, waiting for the responses like a driver does.
     */
    private List<Page> run(BlockFactory blockFactory, FakeClient client, boolean finalStage, List<List<Ref>> input) throws Exception {
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Page> output = new ArrayList<>();
        Operator operator = factory(client, finalStage).get(driverContext);
        try {
            for (List<Ref> refs : input) {
                assertTrue(operator.needsInput());
                operator.addInput(page(blockFactory, refs));
            }
            operator.finish();
            assertFalse(operator.needsInput());
            while (operator.isFinished() == false) {
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

    private static FetchOperator.Factory factory(FetchOperator.Client client, boolean finalStage) {
        return new FetchOperator.Factory(0, FETCHED_TYPES, null, finalStage, client);
    }

    private record Ref(DocRefOrigin origin, int segment, int doc) {
        String id() {
            return "doc-" + origin.shardId().id() + "-" + segment + "-" + doc;
        }

        long value() {
            return origin.shardId().id() * 1_000_000L + segment * 1_000L + doc;
        }
    }

    private record Request(String nodeId, List<FetchRequest.ShardDocs> shards, List<ShardSearchContextId> releaseAfter) {}

    /**
     * Answers like a data node, on another thread.
     */
    private class FakeClient implements FetchOperator.Client {
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

        FakeClient(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        public DiscoveryNode node(String clusterAlias, String nodeId) {
            return nodeId.equals(goneNode) ? null : DiscoveryNodeUtils.create(nodeId);
        }

        @Override
        public void fetch(
            DiscoveryNode node,
            String clusterAlias,
            List<FetchRequest.ShardDocs> shards,
            PhysicalPlan fetchPlan,
            List<ShardSearchContextId> releaseAfter,
            ActionListener<FetchResponse> listener
        ) {
            if (node.getId().equals(throwingNode)) {
                throw new IllegalStateException("can't send to [" + node.getId() + "]");
            }
            requests.add(new Request(node.getId(), shards, releaseAfter));
            threadPool.generic().execute(() -> {
                try {
                    if (holdResponses != null) {
                        holdResponses.await(10, TimeUnit.SECONDS);
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
                DocRefOrigin origin = ORIGINS.stream().filter(o -> o.contextId().equals(shard.contextId())).findFirst().orElseThrow();
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
            return new FetchResponse(blockFactory, results, pages, DriverCompletionInfo.EMPTY, 0, 0);
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
        List<DocRefOrigin> origins = new ArrayList<>();
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, refs.size())) {
            for (Ref ref : refs) {
                if (origins.contains(ref.origin()) == false) {
                    origins.add(ref.origin());
                    builder.addOrigin(ref.origin());
                }
            }
            for (Ref ref : refs) {
                builder.append(origins.indexOf(ref.origin()), ref.segment(), ref.doc());
            }
            return new Page(builder.build());
        }
    }

    /**
     * The document of each output row, read from the fetched columns, after checking that they agree with its reference.
     */
    private static List<Ref> rows(List<Page> pages) {
        List<Ref> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            DocRefBlock refs = page.getBlock(0);
            BytesRefBlock ids = page.getBlock(1);
            LongBlock values = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                DocRefOrigin origin = refs.asVector().origin(p);
                Ref ref = new Ref(origin, refs.asVector().segments().getInt(p), refs.asVector().docs().getInt(p));
                assertThat(ids.getBytesRef(ids.getFirstValueIndex(p), scratch).utf8ToString(), equalTo(ref.id()));
                assertThat(values.getLong(values.getFirstValueIndex(p)), equalTo(ref.value()));
                rows.add(ref);
            }
        }
        return Collections.unmodifiableList(rows);
    }

    private static DocRefOrigin origin(String nodeId, int shard) {
        return new DocRefOrigin("", nodeId, new ShardId("index", "uuid", shard), new ShardSearchContextId("session", shard));
    }
}
