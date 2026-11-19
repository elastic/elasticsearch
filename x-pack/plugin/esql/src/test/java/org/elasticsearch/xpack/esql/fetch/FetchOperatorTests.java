/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * The coordinator's fetch over transport. The operator sends its requests through {@link QueryFetchClient} and
 * {@link FetchService#sendFetch} to three data nodes, each a {@link MockTransportService} that answers like
 * {@link FetchService}: the rows of each shard in request order, with values that name their document, and the warnings of
 * its fetch driver. The tracker of the execution records the contexts it frees instead of sending the free requests.
 */
public class FetchOperatorTests extends ComputeTestCase {
    private static final List<ElementType> FETCHED_TYPES = List.of(ElementType.BYTES_REF, ElementType.LONG);
    private static final List<Attribute> FETCHED = List.of(attribute("id", DataType.KEYWORD), attribute("value", DataType.LONG));
    private static final long TOOK_NANOS = 5_000;
    private static final long SETUP_NANOS = 1_000;
    private static final String EVERY_NODE_WARNING = "a warning of every node";

    private ThreadPool threadPool;
    private BlockFactory blockFactory;
    private MockTransportService coordinator;
    private final List<DataNode> dataNodes = new ArrayList<>();
    private ClusterService clusterService;
    private final CancellableTask rootTask = new CancellableTask(
        1,
        "transport",
        "indices:data/read/esql",
        "",
        TaskId.EMPTY_TASK_ID,
        Map.of()
    );
    /**
     * The contexts the trackers freed, in order.
     */
    private final List<ShardSearchContextId> freed = new CopyOnWriteArrayList<>();
    /**
     * The tracker of the execution of the last {@link #client}. Each execution gets its own, which ends one cut.
     */
    private FetchContextTracker tracker;
    private final List<FetchContextTracker> trackers = new ArrayList<>();
    private final QueryDrivers drivers = new QueryDrivers();
    /**
     * The documents of the tests live on two shards of the first data node and one shard of each of the others.
     */
    private List<DocOrigin> origins;

    /** The status of the operator the last {@link #run} drove, read just before it closed. */
    private FetchOperator.Status lastStatus;
    /** The warnings the driver of the last {@link #run} collected. */
    private List<String> lastWarnings;

    @Before
    public void startNodes() {
        threadPool = new TestThreadPool(getTestName());
        blockFactory = blockFactory();
        coordinator = startTransport("coordinator");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(coordinator.getLocalNode())
            .localNodeId(coordinator.getLocalNode().getId());
        for (int n = 1; n <= 3; n++) {
            DataNode dataNode = new DataNode("n" + n);
            dataNodes.add(dataNode);
            nodes.add(dataNode.transport.getLocalNode());
            AbstractSimpleTransportTestCase.connectToNode(coordinator, dataNode.transport.getLocalNode());
        }
        clusterService = ClusterServiceUtils.createClusterService(threadPool, coordinator.getLocalNode());
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
        origins = List.of(origin(0, 0), origin(0, 1), origin(1, 2), origin(2, 3));
    }

    @After
    public void stopNodes() throws Exception {
        // a response that still waits would hold its transport thread
        dataNodes.forEach(DataNode::release);
        trackers.forEach(FetchContextTracker::close);
        List<Closeable> closeables = new ArrayList<>();
        closeables.add(clusterService);
        closeables.add(coordinator);
        dataNodes.forEach(n -> closeables.add(n.transport));
        IOUtils.close(closeables);
        terminate(threadPool);
    }

    /**
     * The rows keep their order and their columns, and get the values of their own documents appended.
     */
    public void testAppendsTheFetchedColumnsInTheOrderOfTheCut() throws Exception {
        List<List<Ref>> input = randomInput();
        List<Page> output = run(client(), input);
        try {
            assertThat(rows(output), equalTo(input.stream().flatMap(List::stream).toList()));
            assertThat(output.size(), equalTo(input.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Nothing goes out before the input ends. Then each node that holds documents of the cut gets one request, with all of
     * its documents, the index expressions of the query and the attributes to load. No other request reads its contexts,
     * so it asks the node to free them once it answered.
     */
    public void testSendsOneRequestPerNodeOnceTheInputEnded() throws Exception {
        DriverContext driverContext = driverContext();
        Operator operator = factory(client()).get(driverContext);
        try {
            List<Ref> first = List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2));
            List<Ref> second = List.of(new Ref(origins.get(1), 1, 3), new Ref(origins.get(0), 0, 4));
            operator.addInput(trackedPage(first));
            operator.addInput(trackedPage(second));
            assertTrue(operator.needsInput());
            assertThat(allRequests(), empty());

            operator.finish();
            assertFalse(operator.needsInput());
            assertThat(releasedRows(takeOutput(operator, 2)), equalTo(concat(first, second)));

            assertThat(dataNodes.get(0).requests, hasSize(1));
            assertThat(dataNodes.get(1).requests, hasSize(1));
            assertThat(dataNodes.get(2).requests, empty());
            FetchRequest request = dataNodes.get(0).requests.getFirst();
            assertThat(documents(request), equalTo(Set.of(first.get(0), second.get(0), second.get(1))));
            assertThat(request.shards().stream().map(FetchRequest.ShardDocs::freeAfterFetch).toList(), everyItem(equalTo(true)));
            assertThat(request.sessionId(), equalTo("query-session"));
            assertThat(request.indices(), equalTo(new String[] { "index*" }));
            assertThat(request.attributes().stream().map(Attribute::name).toList(), equalTo(List.of("id", "value")));
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
    }

    /**
     * Rows that share a document ask for it once, and each gets its values.
     */
    public void testRowsThatShareADocumentAskForItOnce() throws Exception {
        Ref shared = new Ref(origins.get(2), 0, 7);
        List<List<Ref>> input = List.of(List.of(shared, new Ref(origins.get(2), 0, 8)), List.of(shared));
        List<Page> output = run(client(), input);
        assertThat(releasedRows(output), equalTo(concat(input.get(0), input.get(1))));
        assertThat(documents(dataNodes.get(1).requests.getFirst()), equalTo(Set.of(shared, new Ref(origins.get(2), 0, 8))));
        assertThat(lastStatus.documents(), equalTo(2L));
    }

    /**
     * The first page goes out only once every node answered.
     */
    public void testWaitsForEveryNodeBeforeTheFirstPage() throws Exception {
        DataNode n2 = dataNodes.get(1);
        n2.hold();
        DriverContext driverContext = driverContext();
        Operator operator = factory(client()).get(driverContext);
        try {
            List<Ref> refs = List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2));
            operator.addInput(trackedPage(refs));
            operator.finish();
            assertBusy(() -> assertThat(n2.requests, hasSize(1)));
            assertBusy(() -> assertThat(dataNodes.get(0).requests, hasSize(1)));
            assertNull(operator.getOutput());
            assertFalse(operator.isFinished());

            n2.release();
            assertThat(releasedRows(takeOutput(operator, 1)), equalTo(refs));
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
    }

    /**
     * The end of the input frees the contexts that none of the rows read. Each node frees the contexts of its request once
     * it answered, so the tracker frees no others.
     */
    public void testTheEndOfTheInputFreesTheContextsNoRowReads() throws Exception {
        DocOrigin unread = origins.get(3);
        List<Ref> refs = List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2));
        DriverContext driverContext = driverContext();
        Operator operator = factory(client()).get(driverContext);
        try {
            Page page = pageNamingAnOriginNoRowReads(refs, unread);
            tracker.track(page);
            operator.addInput(page);
            operator.finish();
            assertThat(freed, contains(unread.contextId()));
            assertThat(releasedRows(takeOutput(operator, 1)), equalTo(refs));
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
        assertThat(freed, contains(unread.contextId()));
    }

    /**
     * A request that failed may never have reached its node, so its contexts go once the operator closes.
     */
    public void testTheContextsOfAFailedRequestGoWhenTheOperatorCloses() {
        dataNodes.get(1).rejects = true;
        List<List<Ref>> input = List.of(List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2)));
        expectThrows(IllegalStateException.class, () -> run(client(), input));
        assertThat(freed, hasItem(origins.get(2).contextId()));
    }

    /**
     * An operator that closes before its input ended sends nothing, and every context goes.
     */
    public void testClosingBeforeTheInputEndedSendsNothing() {
        DriverContext driverContext = driverContext();
        Operator operator = factory(client()).get(driverContext);
        try {
            operator.addInput(trackedPage(List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2))));
        } finally {
            operator.close();
            awaitResponses(driverContext);
        }
        assertThat(allRequests(), empty());
        assertThat(freed, containsInAnyOrder(origins.get(0).contextId(), origins.get(2).contextId()));
    }

    /**
     * The operator releases the responses that arrived when it closes before it gathered, and the response that arrives
     * after it closed.
     */
    public void testClosingBeforeTheGatherReleasesTheResponses() throws Exception {
        DataNode n2 = dataNodes.get(1);
        n2.hold();
        DriverContext driverContext = driverContext();
        Operator operator = factory(client()).get(driverContext);
        try {
            operator.addInput(trackedPage(List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2))));
            operator.finish();
            assertBusy(() -> assertThat(n2.requests, hasSize(1)));
            assertNull(operator.getOutput());
        } finally {
            operator.close();
            n2.release();
            awaitResponses(driverContext);
        }
    }

    /**
     * Each node can return response headers, like warnings. The operator gives the driver thread the headers of every
     * node, not only those of the node that answered last.
     */
    public void testMergesTheResponseHeadersOfEveryNode() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            List<List<Ref>> input = List.of(
                List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2), new Ref(origins.get(3), 0, 3))
            );
            run(client(), input).forEach(Page::releaseBlocks);
            assertThat(threadContext.getResponseHeaders().get("X-Fetched-By"), containsInAnyOrder("n1", "n2", "n3"));
        }
    }

    /**
     * The fetch driver of a node returns its warnings, like a value that failed to load, in the completion info of its
     * response rather than in its headers. The operator adds the warnings of every node to its own driver, each one once.
     */
    public void testAddsTheWarningsOfEveryNodeToTheDriver() throws Exception {
        List<List<Ref>> input = List.of(
            List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2), new Ref(origins.get(3), 0, 3))
        );
        run(client(), input).forEach(Page::releaseBlocks);
        assertThat(lastWarnings, containsInAnyOrder(EVERY_NODE_WARNING, warningOf("n1"), warningOf("n2"), warningOf("n3")));
    }

    /**
     * A cut without rows, like the ones {@code EXPLAIN} runs over empty sources, sends nothing and keeps the schema of its
     * pages.
     */
    public void testAnEmptyCutSendsNothing() throws Exception {
        List<Page> output = run(client(), List.of(List.of()));
        try {
            assertThat(output.size(), equalTo(1));
            assertThat(output.getFirst().getPositionCount(), equalTo(0));
            assertThat(output.getFirst().getBlockCount(), equalTo(1 + FETCHED_TYPES.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(run(client(), List.of()), empty());
        assertThat(allRequests(), empty());
    }

    /**
     * Any failure of a fetch fails the query: a shard whose context is gone, which fails the request of its node, a request
     * a node rejected, a node that left, and a request that can't be sent. Each request completes its drivers once.
     */
    public void testAnyFailureFailsTheFetch() {
        List<List<Ref>> input = List.of(List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(2), 0, 2)));

        dataNodes.get(1).missingContext = origins.get(2).contextId();
        Exception e = expectThrows(Exception.class, () -> run(client(), input));
        assertThat(e, instanceOf(SearchContextMissingException.class));
        dataNodes.get(1).missingContext = null;

        dataNodes.get(0).rejects = true;
        e = expectThrows(Exception.class, () -> run(client(), input));
        assertThat(e.getMessage(), equalTo("rejected by [n1]"));
        dataNodes.get(0).rejects = false;

        String left = dataNodes.get(1).id();
        Supplier<DiscoveryNodes> withoutN2 = () -> DiscoveryNodes.builder(clusterService.state().nodes()).remove(left).build();
        e = expectThrows(Exception.class, () -> run(client(withoutN2, this::send), input));
        assertThat(e, instanceOf(NoNodeAvailableException.class));

        String throwing = dataNodes.get(0).id();
        QueryFetchClient.Sender cannotSend = (target, request, parentTask, listener) -> {
            if (target.getId().equals(throwing)) {
                throw new IllegalStateException("can't send to [n1]");
            }
            send(target, request, parentTask, listener);
        };
        e = expectThrows(Exception.class, () -> run(client(() -> clusterService.state().nodes(), cannotSend), input));
        assertThat(e.getMessage(), equalTo("can't send to [n1]"));

        // each request completed its drivers once, also the one that never reached its node
        assertThat(drivers.completed, hasSize(allRequests().size() + 1));
    }

    /**
     * The status shows the cut, the documents the requests asked for, and the fetch from each node.
     */
    public void testStatusShowsTheFetchOfEachNode() throws Exception {
        List<List<Ref>> input = List.of(
            List.of(new Ref(origins.get(0), 0, 1), new Ref(origins.get(1), 0, 2)),
            List.of(new Ref(origins.get(2), 0, 3), new Ref(origins.get(0), 0, 1))
        );
        run(client(), input).forEach(Page::releaseBlocks);

        assertThat(lastStatus.pagesReceived(), equalTo(2));
        assertThat(lastStatus.rowsReceived(), equalTo(4L));
        assertThat(lastStatus.documents(), equalTo(3L));
        assertThat(lastStatus.pagesEmitted(), equalTo(2));
        List<FetchOperator.NodeFetchStats> nodes = lastStatus.nodes();
        assertThat(nodes.stream().map(FetchOperator.NodeFetchStats::node).toList(), containsInAnyOrder("n1", "n2"));
        for (FetchOperator.NodeFetchStats node : nodes) {
            assertThat(node.shards(), equalTo(node.node().equals("n1") ? 2 : 1));
            assertThat(node.documents(), equalTo(node.node().equals("n1") ? 2 : 1));
            assertThat(node.requestNanos(), greaterThan(0L));
            assertThat(node.tookNanos(), equalTo(TOOK_NANOS));
            assertThat(node.setupNanos(), lessThan(node.tookNanos()));
        }
        assertThat(drivers.completed, hasSize(2));
    }

    /**
     * Runs the cut through the operator and returns its output, like a driver does: the exchange shows each page to the
     * tracker, the operator takes every page, and once its input finished, the driver waits while it is blocked.
     */
    private List<Page> run(QueryFetchClient client, List<List<Ref>> input) throws Exception {
        DriverContext driverContext = driverContext();
        List<Page> output = new ArrayList<>();
        Operator operator = factory(client).get(driverContext);
        try {
            for (List<Ref> refs : input) {
                operator.addInput(trackedPage(refs));
            }
            operator.finish();
            while (operator.isFinished() == false) {
                awaitUnblocked(operator);
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
            lastWarnings = driverContext.warnings();
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

    /**
     * Takes the next {@code count} pages out of the operator, and waits for each one like a driver does.
     */
    private static List<Page> takeOutput(Operator operator, int count) {
        List<Page> output = new ArrayList<>();
        boolean success = false;
        try {
            while (output.size() < count) {
                awaitUnblocked(operator);
                Page page = operator.getOutput();
                if (page != null) {
                    output.add(page);
                }
            }
            success = true;
            return output;
        } finally {
            if (success == false) {
                output.forEach(Page::releaseBlocks);
            }
        }
    }

    private DriverContext driverContext() {
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private FetchOperator.Factory factory(QueryFetchClient client) {
        return new FetchOperator.Factory(0, FETCHED_TYPES, FETCHED, client);
    }

    private QueryFetchClient client() {
        return client(() -> clusterService.state().nodes(), this::send);
    }

    /**
     * The client of a new execution, with its own tracker, which records the contexts it frees instead of sending the
     * free requests.
     */
    private QueryFetchClient client(Supplier<DiscoveryNodes> nodes, QueryFetchClient.Sender sender) {
        tracker = new FetchContextTracker(new SearchTransportService(coordinator, null, null) {
            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                // the tracker doesn't wait for the answer, so the test doesn't send one
                freed.add(contextId);
            }
        }, clusterService);
        trackers.add(tracker);
        return new QueryFetchClient(
            sender,
            threadPool.getThreadContext(),
            nodes,
            new QueryFetchScope(rootTask, "query-session", EsqlTestUtils.TEST_CFG, drivers, tracker),
            new OriginalIndices(new String[] { "index*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
        );
    }

    /**
     * Sends a request the way {@link FetchService} does, reading the response into the block factory of the driver.
     */
    private void send(DiscoveryNode target, FetchRequest request, Task parentTask, ActionListener<FetchResponse> listener) {
        FetchService.sendFetch(coordinator, blockFactory, target, request, parentTask, listener);
    }

    private MockTransportService startTransport(String name) {
        Settings settings = Settings.builder().put("node.name", name).build();
        List<NamedWriteableRegistry.Entry> writeables = new ArrayList<>();
        writeables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        // the data nodes read the attributes of the requests
        writeables.addAll(new EsqlPlugin().getNamedWriteables());
        MockTransportService transport = MockTransportService.createNewService(
            settings,
            MockTransportService.newMockTransport(settings, TransportVersion.current(), threadPool, new NamedWriteableRegistry(writeables)),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Set.of()
        );
        transport.start();
        transport.acceptIncomingRequests();
        return transport;
    }

    private List<FetchRequest> allRequests() {
        return dataNodes.stream().flatMap(n -> n.requests.stream()).toList();
    }

    /**
     * A data node that answers fetch requests like {@link FetchService}.
     */
    private class DataNode {
        private final String name;
        private final MockTransportService transport;
        private final BlockFactory responseBlocks = blockFactory();
        private final List<FetchRequest> requests = new CopyOnWriteArrayList<>();
        /** Its responses wait for this latch. */
        private volatile CountDownLatch held;
        /** A request that names this context fails as a whole, like a node fails it when the context is gone. */
        private volatile ShardSearchContextId missingContext;
        /** Its requests fail with a rejection. */
        private volatile boolean rejects;

        DataNode(String name) {
            this.name = name;
            this.transport = startTransport(name);
            transport.registerRequestHandler(
                FetchService.FETCH_ACTION_NAME,
                threadPool.generic(),
                FetchRequest::new,
                (request, channel, task) -> answer(request, channel)
            );
        }

        String id() {
            return transport.getLocalNode().getId();
        }

        void hold() {
            held = new CountDownLatch(1);
        }

        void release() {
            CountDownLatch latch = held;
            if (latch != null) {
                latch.countDown();
            }
        }

        private void answer(FetchRequest request, TransportChannel channel) throws Exception {
            requests.add(request);
            CountDownLatch latch = held;
            if (latch != null) {
                assertTrue(latch.await(10, TimeUnit.SECONDS));
            }
            threadPool.getThreadContext().addResponseHeader("X-Fetched-By", name);
            if (rejects) {
                channel.sendResponse(new IllegalStateException("rejected by [" + name + "]"));
                return;
            }
            for (FetchRequest.ShardDocs shard : request.shards()) {
                if (shard.contextId().equals(missingContext)) {
                    channel.sendResponse(new SearchContextMissingException(shard.contextId()));
                    return;
                }
            }
            FetchResponse response = respond(request);
            try {
                channel.sendResponse(response);
            } finally {
                response.decRef();
            }
        }

        /**
         * The rows of each shard in request order, with values that name their document.
         */
        private FetchResponse respond(FetchRequest request) {
            List<Page> pages = new ArrayList<>();
            for (FetchRequest.ShardDocs shard : request.shards()) {
                try (
                    BytesRefBlock.Builder ids = responseBlocks.newBytesRefBlockBuilder(shard.docCount());
                    LongBlock.Builder values = responseBlocks.newLongBlockBuilder(shard.docCount())
                ) {
                    for (int d = 0; d < shard.docCount(); d++) {
                        ids.appendBytesRef(new BytesRef(docId(shard.shardId(), shard.segments()[d], shard.docs()[d])));
                        values.appendLong(value(shard.shardId(), shard.segments()[d], shard.docs()[d]));
                    }
                    pages.add(new Page(ids.build(), values.build()));
                }
            }
            DriverCompletionInfo completionInfo = DriverCompletionInfo.EMPTY.withAdditionalWarnings(
                List.of(EVERY_NODE_WARNING, warningOf(name))
            );
            return new FetchResponse(pages, completionInfo, TOOK_NANOS, SETUP_NANOS);
        }
    }

    private static String warningOf(String nodeName) {
        return "a warning of " + nodeName;
    }

    private static String docId(ShardId shardId, int segment, int doc) {
        return "doc-" + shardId.id() + "-" + segment + "-" + doc;
    }

    private static long value(ShardId shardId, int segment, int doc) {
        return shardId.id() * 1_000_000L + segment * 1_000L + doc;
    }

    /**
     * Stands in for the compute listener of the query, which counts the drivers of each listener it hands out.
     */
    private static class QueryDrivers implements Supplier<ActionListener<DriverCompletionInfo>> {
        private final List<DriverCompletionInfo> completed = new CopyOnWriteArrayList<>();

        @Override
        public ActionListener<DriverCompletionInfo> get() {
            return ActionListener.assertOnce(ActionListener.wrap(completed::add, e -> { throw new AssertionError(e); }));
        }
    }

    private record Ref(DocOrigin origin, int segment, int doc) {}

    private DocOrigin origin(int dataNode, int shard) {
        return new DocOrigin(
            "",
            dataNodes.get(dataNode).id(),
            new ShardId("index", "uuid", shard),
            new ShardSearchContextId("session", shard)
        );
    }

    private List<List<Ref>> randomInput() {
        List<List<Ref>> input = new ArrayList<>();
        int pages = between(1, 4);
        for (int p = 0; p < pages; p++) {
            List<Ref> refs = new ArrayList<>();
            int rows = between(0, 40);
            for (int r = 0; r < rows; r++) {
                refs.add(new Ref(randomFrom(origins), between(0, 3), between(0, 500)));
            }
            input.add(refs);
        }
        return input;
    }

    /**
     * A page of {@code refs}, which the tracker saw arrive, as the exchange shows it every page.
     */
    private Page trackedPage(List<Ref> refs) {
        Page page = page(refs, null);
        tracker.track(page);
        return page;
    }

    /**
     * A page of {@code refs} whose origins also name {@code unread}, an origin that none of its rows reads.
     */
    private Page pageNamingAnOriginNoRowReads(List<Ref> refs, DocOrigin unread) {
        assertFalse("no row may read it", refs.stream().anyMatch(r -> r.origin().equals(unread)));
        return page(refs, unread);
    }

    private Page page(List<Ref> refs, DocOrigin extraOrigin) {
        List<DocOrigin> pageOrigins = new ArrayList<>();
        for (Ref ref : refs) {
            if (pageOrigins.contains(ref.origin()) == false) {
                pageOrigins.add(ref.origin());
            }
        }
        if (extraOrigin != null) {
            pageOrigins.add(extraOrigin);
        }
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, refs.size())) {
            for (Ref ref : refs) {
                builder.appendShard(pageOrigins.indexOf(ref.origin())).appendSegment(ref.segment()).appendDoc(ref.doc());
            }
            // rows may share a document
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(pageOrigins)).mayContainDuplicates()));
        }
    }

    /**
     * The documents one request asked for.
     */
    private Set<Ref> documents(FetchRequest request) {
        Set<Ref> documents = new HashSet<>();
        for (FetchRequest.ShardDocs shard : request.shards()) {
            DocOrigin origin = origins.stream().filter(o -> o.contextId().equals(shard.contextId())).findFirst().orElseThrow();
            for (int d = 0; d < shard.docCount(); d++) {
                documents.add(new Ref(origin, shard.segments()[d], shard.docs()[d]));
            }
        }
        return documents;
    }

    @SafeVarargs
    private static List<Ref> concat(List<Ref>... pages) {
        List<Ref> rows = new ArrayList<>();
        for (List<Ref> page : pages) {
            rows.addAll(page);
        }
        return rows;
    }

    /**
     * The rows of {@code pages}, which it releases.
     */
    private static List<Ref> releasedRows(List<Page> pages) {
        try {
            return rows(pages);
        } finally {
            pages.forEach(Page::releaseBlocks);
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
                DocOrigin origin = docs.origin(p);
                int segment = docs.segments().getInt(p);
                int doc = docs.docs().getInt(p);
                assertThat(
                    ids.getBytesRef(ids.getFirstValueIndex(p), scratch).utf8ToString(),
                    equalTo(docId(origin.shardId(), segment, doc))
                );
                assertThat(values.getLong(values.getFirstValueIndex(p)), equalTo(value(origin.shardId(), segment, doc)));
                rows.add(new Ref(origin, segment, doc));
            }
        }
        return Collections.unmodifiableList(rows);
    }

    private static Attribute attribute(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
