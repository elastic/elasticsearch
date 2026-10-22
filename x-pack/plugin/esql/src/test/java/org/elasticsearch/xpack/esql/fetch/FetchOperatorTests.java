/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
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
import static org.hamcrest.Matchers.sameInstance;

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
     * Without partial results, any failure of a fetch fails the query: a shard whose context is gone, a request a node
     * rejected, a node that left.
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
     * With partial results, a shard whose fetch failed loses its rows. The other rows of the page keep their order and get
     * their fetched columns, and the query records the shard as failed.
     */
    public void testAFailedShardDropsItsRowsWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        DocOrigin failed = ORIGINS.get(1);
        client.failingShard = failed.contextId();
        List<Ref> input = List.of(
            new Ref(ORIGINS.get(0), 0, 1),
            new Ref(failed, 0, 2),
            new Ref(ORIGINS.get(2), 0, 3),
            new Ref(failed, 1, 4),
            new Ref(ORIGINS.get(0), 2, 5)
        );
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, List.of(input), failures);
        try {
            assertThat(rows(output), equalTo(without(input, failed)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        EsqlExecutionInfo.Cluster reported = reported(failures);
        assertThat(reported.getFailedShards(), equalTo(1));
        assertThat(listedShards(reported), equalTo(List.of(failed.shardId())));
        assertThat(reported.getFailures().getFirst().getCause(), instanceOf(SearchContextMissingException.class));
    }

    /**
     * With partial results, a request a node rejected drops the rows of every shard of that node in the page. The other
     * nodes of the page still load theirs.
     */
    public void testARejectedRequestDropsTheRowsOfItsNodeWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.failingNode = "n1";
        List<Ref> input = List.of(
            new Ref(ORIGINS.get(0), 0, 1),
            new Ref(ORIGINS.get(2), 0, 2),
            new Ref(ORIGINS.get(1), 0, 3),
            new Ref(ORIGINS.get(3), 0, 4)
        );
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, List.of(input), failures);
        try {
            assertThat(rows(output), equalTo(without(input, ORIGINS.get(0), ORIGINS.get(1))));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        EsqlExecutionInfo.Cluster reported = reported(failures);
        assertThat(reported.getFailedShards(), equalTo(2));
        assertThat("the shards of the node failed with one exception", reported.getFailures().size(), equalTo(1));
        assertThat(reported.getFailures().getFirst().getCause().getMessage(), equalTo("rejected by [n1]"));
    }

    /**
     * With partial results, a node that left before the request drops the rows of its shards, and the page doesn't wait
     * for it.
     */
    public void testANodeThatLeftDropsItsRowsWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.goneNode = "n2";
        List<Ref> input = List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2), new Ref(ORIGINS.get(1), 0, 3));
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, List.of(input), failures);
        try {
            assertThat(rows(output), equalTo(without(input, ORIGINS.get(2))));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(client.requests.stream().map(Request::nodeId).toList(), equalTo(List.of("n1")));
        EsqlExecutionInfo.Cluster reported = reported(failures);
        assertThat(listedShards(reported), equalTo(List.of(ORIGINS.get(2).shardId())));
        assertThat(reported.getFailures().getFirst().getCause(), instanceOf(NoNodeAvailableException.class));
    }

    /**
     * With partial results, a request that fails before it is sent drops the rows of its node, and the other nodes still
     * get their requests.
     */
    public void testARequestThatCantBeSentDropsTheRowsOfItsNodeWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.throwingNode = "n1";
        List<Ref> input = List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2));
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, List.of(input), failures);
        try {
            assertThat(rows(output), equalTo(without(input, ORIGINS.get(0))));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(client.requests.stream().map(Request::nodeId).toList(), equalTo(List.of("n2")));
        EsqlExecutionInfo.Cluster reported = reported(failures);
        assertThat(listedShards(reported), equalTo(List.of(ORIGINS.get(0).shardId())));
        assertThat(reported.getFailures().getFirst().getCause().getMessage(), equalTo("can't send to [n1]"));
    }

    /**
     * With partial results, a page whose fetches all failed comes out without rows, with the columns of the cut and of the
     * fetch.
     */
    public void testAPageWhoseFetchesAllFailedComesOutEmpty() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        client.failingNode = "n1";
        client.goneNode = "n2";
        List<Ref> input = List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2), new Ref(ORIGINS.get(1), 0, 3));
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, List.of(input), failures);
        try {
            assertThat(output.size(), equalTo(1));
            assertThat(output.getFirst().getPositionCount(), equalTo(0));
            assertThat(output.getFirst().getBlockCount(), equalTo(1 + FETCHED_TYPES.size()));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(reported(failures).getFailedShards(), equalTo(3));
    }

    /**
     * Rows that share a document of a failed shard all drop, and rows that share a document of a shard that answered all
     * keep its values.
     */
    public void testRowsThatShareADocumentDropTogether() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        DocOrigin failed = ORIGINS.get(1);
        client.failingShard = failed.contextId();
        Ref ofTheFailedShard = new Ref(failed, 0, 7);
        Ref answered = new Ref(ORIGINS.get(0), 0, 3);
        List<Ref> input = List.of(ofTheFailedShard, answered, ofTheFailedShard, new Ref(ORIGINS.get(2), 1, 1), answered);
        List<Page> output = run(blockFactory, client, List.of(input), new FetchFailures(true));
        try {
            assertThat(rows(output), equalTo(List.of(answered, new Ref(ORIGINS.get(2), 1, 1), answered)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A shard whose fetch fails in several pages of the cut counts once in the failures of the query.
     */
    public void testAShardThatFailsInSeveralPagesCountsOnce() throws Exception {
        BlockFactory blockFactory = blockFactory();
        FakeClient client = new FakeClient(blockFactory);
        DocOrigin failed = ORIGINS.get(2);
        client.failingShard = failed.contextId();
        List<List<Ref>> input = new ArrayList<>();
        int pages = between(2, 4);
        for (int p = 0; p < pages; p++) {
            input.add(List.of(new Ref(ORIGINS.get(0), p, 1), new Ref(failed, p, 2), new Ref(ORIGINS.get(3), p, 3)));
        }
        FetchFailures failures = new FetchFailures(true);
        List<Page> output = run(blockFactory, client, input, failures);
        try {
            assertThat(rows(output), equalTo(without(input.stream().flatMap(List::stream).toList(), failed)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        EsqlExecutionInfo.Cluster reported = reported(failures);
        assertThat(reported.getFailedShards(), equalTo(1));
        assertThat(listedShards(reported), equalTo(List.of(failed.shardId())));
    }

    /**
     * Even with partial results, a cancellation, a circuit breaker trip or a security failure fails the fetch, whether a
     * shard or a whole request fails with it.
     */
    public void testFailuresBeyondTheShardsFailTheFetchWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        List<List<Ref>> input = List.of(List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(2), 0, 2)));
        List<Exception> fatal = List.of(
            new TaskCancelledException("cancelled"),
            new CircuitBreakingException("over the limit", CircuitBreaker.Durability.TRANSIENT),
            new ElasticsearchSecurityException("action denied", RestStatus.FORBIDDEN)
        );
        for (Exception failure : fatal) {
            FakeClient ofAShard = new FakeClient(blockFactory);
            ofAShard.failingShard = ORIGINS.get(2).contextId();
            ofAShard.shardFailure = failure;
            FetchFailures shardFailures = new FetchFailures(true);
            Exception e = expectThrows(Exception.class, () -> run(blockFactory, ofAShard, input, shardFailures));
            assertThat(e, sameInstance(failure));
            assertThat("the query fails, so it reports no failed shard", reported(shardFailures).getFailedShards(), equalTo(0));

            FakeClient ofARequest = new FakeClient(blockFactory);
            ofARequest.failingNode = "n2";
            ofARequest.nodeFailure = failure;
            FetchFailures requestFailures = new FetchFailures(true);
            e = expectThrows(Exception.class, () -> run(blockFactory, ofARequest, input, requestFailures));
            assertThat(e, sameInstance(failure));
            assertThat("the query fails, so it reports no failed shard", reported(requestFailures).getFailedShards(), equalTo(0));
        }
    }

    /**
     * Even with partial results, a response that doesn't match its request fails the fetch: an answer for another shard,
     * for another number of shards, or rows that don't add up to the documents of the shards that answered.
     */
    public void testAResponseThatDoesntMatchItsRequestFailsTheFetchWithPartialResults() throws Exception {
        BlockFactory blockFactory = blockFactory();
        List<List<Ref>> input = List.of(
            List.of(new Ref(ORIGINS.get(0), 0, 1), new Ref(ORIGINS.get(1), 0, 2), new Ref(ORIGINS.get(2), 0, 3))
        );

        FakeClient anotherShard = new FakeClient(blockFactory);
        anotherShard.wrongShardNode = "n2";
        Exception e = expectThrows(IllegalStateException.class, () -> run(blockFactory, anotherShard, input, new FetchFailures(true)));
        assertThat(e.getMessage(), equalTo("node [n2] answered for " + FakeClient.OTHER_SHARD + " instead of " + ORIGINS.get(2).shardId()));

        FakeClient missingShard = new FakeClient(blockFactory);
        missingShard.missingResultNode = "n1";
        e = expectThrows(IllegalStateException.class, () -> run(blockFactory, missingShard, input, new FetchFailures(true)));
        assertThat(e.getMessage(), equalTo("node [n1] answered for [1] shards out of [2]"));

        // n1 loads shard 0 first and returns no row for it, while shard 1 fails, which the page could go on without
        FakeClient missingRows = new FakeClient(blockFactory);
        missingRows.shortNode = "n1";
        missingRows.failingShard = ORIGINS.get(1).contextId();
        e = expectThrows(IllegalStateException.class, () -> run(blockFactory, missingRows, input, new FetchFailures(true)));
        assertThat(e.getMessage(), equalTo("node [n1] returned [0] rows for [1] documents"));
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
        return run(blockFactory, client, input, new FetchFailures(false));
    }

    private List<Page> run(BlockFactory blockFactory, FakeClient client, List<List<Ref>> input, FetchFailures failures) throws Exception {
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Page> output = new ArrayList<>();
        Operator operator = factory(client, failures).get(driverContext);
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
        return factory(client, new FetchFailures(false));
    }

    private static FetchOperator.Factory factory(FetchOperator.Client client, FetchFailures failures) {
        return new FetchOperator.Factory(0, FETCHED_TYPES, null, between(1, FetchOperator.MAX_OUTSTANDING_PAGES), client, failures);
    }

    /**
     * The rows of {@code refs} whose documents aren't on the shards of {@code failed}, in order.
     */
    private static List<Ref> without(List<Ref> refs, DocOrigin... failed) {
        List<DocOrigin> failedOrigins = List.of(failed);
        return refs.stream().filter(ref -> failedOrigins.contains(ref.origin()) == false).toList();
    }

    /**
     * The local cluster the query reports once its fetches are done, after a query phase that succeeded on every shard.
     */
    private static EsqlExecutionInfo.Cluster reported(FetchFailures failures) {
        EsqlExecutionInfo.Cluster cluster = new EsqlExecutionInfo.Cluster(
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            null,
            "index",
            false,
            EsqlExecutionInfo.Cluster.Status.RUNNING,
            ORIGINS.size(),
            ORIGINS.size(),
            0,
            0,
            List.of(),
            null
        );
        EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(cluster);
        failures.applyTo(builder, cluster);
        return builder.build();
    }

    private static List<ShardId> listedShards(EsqlExecutionInfo.Cluster cluster) {
        return cluster.getFailures().stream().map(failure -> failure.shard().getShardId()).toList();
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
        /** A shard that no request asks for. */
        static final ShardId OTHER_SHARD = new ShardId("index", "uuid", 99);
        private final BlockFactory blockFactory;
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private ShardSearchContextId failingShard;
        /** The failure of {@link #failingShard}, a missing context when {@code null}. */
        private Exception shardFailure;
        private String failingNode;
        /** The failure of the request to {@link #failingNode}, a rejection when {@code null}. */
        private Exception nodeFailure;
        private String goneNode;
        /** The request to this node fails before it is sent. */
        private String throwingNode;
        /** This node returns no row for the first shard it loads. */
        private String shortNode;
        /** This node answers for {@link #OTHER_SHARD} instead of the first shard it loads. */
        private String wrongShardNode;
        /** This node leaves the result of the last shard it loads out of its response. */
        private String missingResultNode;
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
                        listener.onFailure(
                            nodeFailure != null ? nodeFailure : new IllegalStateException("rejected by [" + node.getId() + "]")
                        );
                        return;
                    }
                    FetchResponse response = respond(node.getId(), shards);
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

        private FetchResponse respond(String nodeId, List<FetchRequest.ShardDocs> shards) {
            List<ShardResult> results = new ArrayList<>();
            List<Page> pages = new ArrayList<>();
            for (FetchRequest.ShardDocs shard : shards) {
                if (nodeId.equals(shortNode) && results.isEmpty()) {
                    results.add(ShardResult.succeeded(shard.shardId(), 0));
                    continue;
                }
                if (shard.contextId().equals(failingShard)) {
                    Exception failure = shardFailure != null ? shardFailure : new SearchContextMissingException(shard.contextId());
                    results.add(ShardResult.failed(shard.shardId(), failure));
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
            if (nodeId.equals(wrongShardNode)) {
                ShardResult first = results.getFirst();
                results.set(0, new ShardResult(OTHER_SHARD, first.rows(), first.failure()));
            }
            if (nodeId.equals(missingResultNode)) {
                results.removeLast();
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
