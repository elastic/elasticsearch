/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.QueryScopeRegistry;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The exchange lease: the service acquires the query's scope, creating it, when it opens a sink handler, and keeps it
 * until the handler is dropped; a sink opened and never computed on is reaped and releases; a response polled from the
 * buffer keeps the scope until its last reference goes; a remote sink and each of its fetches hold the scope on the
 * fetching node, past the compute's completion and through a failed fetch; without a resolver everything stays on the
 * node's factory.
 */
public class ExchangeScopeLeaseTests extends ComputeTestCase {

    private static final String EXECUTOR = "esql_test_executor";

    private TestThreadPool threadPool;

    @Before
    public void startPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, EXECUTOR, between(1, 4), 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void stopPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private QueryScopeRegistry registry(BlockFactory node) {
        return new QueryScopeRegistry(
            (queryId, onClosing) -> QueryScope.create(queryId, node, ByteSizeValue.ofMb(4).getBytes(), 1024, null, onClosing)
        );
    }

    private static boolean closed(QueryScope scope) {
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        scope.addCloseListener(done);
        return done.isDone();
    }

    public void testASinkHandlerAcquiresTheScopeAndHoldsItUntilTheServiceDropsIt() {
        BlockFactory node = blockFactory();
        QueryScopeRegistry registry = registry(node);
        try (ExchangeService exchange = new ExchangeService(Settings.EMPTY, threadPool, EXECUTOR, node)) {
            exchange.setScopeResolver(registry::acquire);
            assertThat(registry.size(), equalTo(0));
            ExchangeSinkHandler sink = randomBoolean() ? exchange.createSinkHandler("q", 3) : exchange.getOrCreateSinkHandler("q", 3);
            assertThat("the sink created the scope", registry.size(), equalTo(1));
            QueryScope.Lease observer = registry.resolve("q");
            assertThat(observer, notNullValue());
            assertThat(observer.scope().leaseCount(), equalTo(2));
            observer.close();
            assertFalse("the sink keeps the scope", closed(observer.scope()));
            sink.createExchangeSink(() -> {}).finish();
            exchange.finishSinkHandler("q", null);
            assertTrue(closed(observer.scope()));
            assertThat(registry.size(), equalTo(0));
        }
    }

    public void testASinkOpenedWithoutAComputeIsReapedAndReleases() throws Exception {
        BlockFactory node = blockFactory();
        QueryScopeRegistry registry = registry(node);
        Settings settings = Settings.builder().put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, "100ms").build();
        try (ExchangeService exchange = new ExchangeService(settings, threadPool, EXECUTOR, node)) {
            exchange.setScopeResolver(registry::acquire);
            // the observer goes first, so a reaper that runs between the two calls cannot take the scope from under it
            QueryScope.Lease observer = registry.acquire("q");
            // the coordinator opened the exchange, then went away before any compute reached this node
            exchange.createSinkHandler("q", 3);
            assertThat(observer.scope().leaseCount(), equalTo(2));
            observer.close();
            assertBusy(() -> assertTrue(closed(observer.scope())));
            assertThat(registry.size(), equalTo(0));
        }
    }

    public void testAPolledResponseKeepsTheScopeUntilItsLastReference() throws Exception {
        BlockFactory node = blockFactory();
        QueryScopeRegistry registry = registry(node);
        try (ExchangeService exchange = new ExchangeService(Settings.EMPTY, threadPool, EXECUTOR, node)) {
            exchange.setScopeResolver(registry::acquire);
            ExchangeSinkHandler sinkHandler = exchange.createSinkHandler("q", 3);
            QueryScope.Lease observer = registry.resolve("q");
            QueryScope scope = observer.scope();
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            Page page = new Page(scope.blockFactory().newConstantIntBlockWith(1, 10));
            sink.addPage(page);
            sink.finish();
            PlainActionFuture<ExchangeResponse> fetched = new PlainActionFuture<>();
            // the handler releases its reference right after the listener returns; the transport keeps one while it sends
            sinkHandler.fetchPageAsync(false, fetched.map(r -> {
                r.incRef();
                return r;
            }));
            ExchangeResponse response = fetched.get(10, TimeUnit.SECONDS);
            assertThat("the response was polled from the buffer and holds a lease of its own", scope.leaseCount(), equalTo(3));
            // the remote keeps fetching until it gets the end marker, which is what finishes the buffer; that response
            // takes and releases a lease of its own within the call
            PlainActionFuture<ExchangeResponse> end = new PlainActionFuture<>();
            sinkHandler.fetchPageAsync(false, end);
            assertTrue(end.get(10, TimeUnit.SECONDS).finished());
            assertThat(scope.leaseCount(), equalTo(3));
            exchange.finishSinkHandler("q", null);
            observer.close();
            assertFalse("the page in flight keeps the scope", closed(scope));
            Page taken = response.takePage();
            assertThat(taken, sameInstance(page));
            taken.releaseBlocks();
            response.decRef();
            assertTrue(closed(scope));
            assertThat(node.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testWithoutAResolverTheNodeFactoryIsUsed() {
        BlockFactory node = blockFactory();
        try (ExchangeService exchange = new ExchangeService(Settings.EMPTY, threadPool, EXECUTOR, node)) {
            ExchangeSinkHandler sinkHandler = exchange.createSinkHandler("q", 3);
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            sink.finish();
            exchange.finishSinkHandler("q", null);
            sinkHandler.releaseScope(); // harmless without a lease
        }
    }

    /**
     * The fetching node and the sink node of a remote exchange, with the scope registry on the fetching node.
     */
    private final class RemoteExchange implements AutoCloseable {
        final BlockFactory node0 = blockFactory();
        final BlockFactory node1 = blockFactory();
        final QueryScopeRegistry registry0 = registry(node0);
        final MockTransportService transport0 = newTransportService();
        final MockTransportService transport1 = newTransportService();
        final ExchangeService exchange0 = new ExchangeService(Settings.EMPTY, threadPool, EXECUTOR, node0);
        final ExchangeService exchange1 = new ExchangeService(Settings.EMPTY, threadPool, EXECUTOR, node1);

        RemoteExchange() {
            exchange0.setScopeResolver(registry0::acquire);
            exchange0.registerTransportHandler(transport0);
            exchange1.registerTransportHandler(transport1);
            AbstractSimpleTransportTestCase.connectToNode(transport0, transport1.getLocalNode());
        }

        RemoteSink remoteSink(String exchangeId) {
            Task task = new Task(1, "", "", "", null, Collections.emptyMap());
            return exchange0.newRemoteSink(task, exchangeId, transport0, transport0.getConnection(transport1.getLocalNode()));
        }

        @Override
        public void close() {
            exchange0.close();
            exchange1.close();
            transport0.close();
            transport1.close();
        }
    }

    private List<Page> drain(ExchangeSource source) throws Exception {
        List<Page> received = new ArrayList<>();
        while (true) {
            Page page = source.pollPage();
            if (page != null) {
                received.add(page);
                continue;
            }
            if (source.isFinished()) {
                return received;
            }
            PlainActionFuture<Void> ready = new PlainActionFuture<>();
            source.waitForReading().listener().addListener(ready);
            ready.get(10, TimeUnit.SECONDS);
        }
    }

    public void testARemoteSinkAndItsFetchesHoldTheScopeOnTheFetchingNode() throws Exception {
        try (RemoteExchange remote = new RemoteExchange()) {
            String exchangeId = "q";
            ExchangeSinkHandler sinkHandler = remote.exchange1.createSinkHandler(exchangeId, 3);
            RemoteSink remoteSink = remote.remoteSink(exchangeId);
            QueryScope.Lease observer = remote.registry0.resolve(exchangeId);
            QueryScope scope = observer.scope();
            assertThat("the remote sink created the scope and holds it", scope.leaseCount(), equalTo(2));
            ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(3, threadPool.executor(EXECUTOR));
            PlainActionFuture<Void> sinkDone = new PlainActionFuture<>();
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, 1, sinkDone);
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            int pages = between(1, 20);
            for (int i = 0; i < pages; i++) {
                sink.addPage(new Page(remote.node1.newConstantIntBlockWith(i, between(1, 100))));
            }
            sink.finish();
            ExchangeSource source = sourceHandler.createExchangeSource();
            List<Page> received = drain(source);
            assertThat(received.size(), equalTo(pages));
            for (Page page : received) {
                assertThat(
                    "pages are decoded into the query's factory",
                    page.getBlock(0).blockFactory(),
                    sameInstance(scope.blockFactory())
                );
                page.releaseBlocks();
            }
            source.finish();
            sinkDone.get(10, TimeUnit.SECONDS);
            assertBusy(() -> assertThat("the remote sink and every fetch released", scope.leaseCount(), equalTo(1)));
            observer.close();
            assertTrue(closed(scope));
            assertThat(remote.node0.breaker().getUsed(), equalTo(0L));
            assertThat(remote.node1.breaker().getUsed(), equalTo(0L));
        }
    }

    /**
     * A fetch in flight keeps the scope after the compute that issued it has completed, and its lease goes only once
     * the response has been handed on; a fetch that fails closes the sink and releases everything.
     */
    public void testAFetchInFlightOutlivesTheComputeAndAFailedFetchReleases() throws Exception {
        try (RemoteExchange remote = new RemoteExchange()) {
            String exchangeId = "q";
            ExchangeSinkHandler sinkHandler = remote.exchange1.createSinkHandler(exchangeId, 3);
            // hold the first exchange request on the sink node; the fetch stays in flight until the test lets it through
            AtomicReference<Runnable> held = new AtomicReference<>();
            AtomicReference<TransportChannel> heldChannel = new AtomicReference<>();
            boolean failTheFetch = randomBoolean();
            remote.transport1.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
                if (held.compareAndSet(null, () -> dispatch(handler, request, channel, task))) {
                    heldChannel.set(channel);
                    return;
                }
                handler.messageReceived(request, channel, task);
            });
            RemoteSink remoteSink = remote.remoteSink(exchangeId);
            QueryScope.Lease compute = remote.registry0.resolve(exchangeId);
            QueryScope scope = compute.scope();
            ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(3, threadPool.executor(EXECUTOR));
            PlainActionFuture<Void> sinkDone = new PlainActionFuture<>();
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, 1, sinkDone);
            assertBusy(() -> assertThat(held.get(), notNullValue()));
            assertThat("the sink, the fetch in flight and the compute", scope.leaseCount(), equalTo(3));
            // the compute completes while the fetch is still out
            compute.close();
            assertFalse(closed(scope));
            assertThat(scope.leaseCount(), equalTo(2));
            ExchangeSink sink = sinkHandler.createExchangeSink(() -> {});
            if (failTheFetch) {
                heldChannel.get().sendResponse(new TaskCancelledException("cancelled"));
                expectThrows(Exception.class, () -> sinkDone.get(10, TimeUnit.SECONDS));
                sink.finish();
                remote.exchange1.finishSinkHandler(exchangeId, null);
            } else {
                sink.addPage(new Page(remote.node1.newConstantIntBlockWith(1, 10)));
                sink.finish();
                held.get().run();
                ExchangeSource source = sourceHandler.createExchangeSource();
                for (Page page : drain(source)) {
                    assertThat(page.getBlock(0).blockFactory(), sameInstance(scope.blockFactory()));
                    page.releaseBlocks();
                }
                source.finish();
                sinkDone.get(10, TimeUnit.SECONDS);
            }
            assertBusy(() -> assertTrue("every lease went once the fetch and the sink were done", closed(scope)));
            assertThat(remote.registry0.size(), equalTo(0));
            assertThat(remote.node0.breaker().getUsed(), equalTo(0L));
            assertThat(remote.node1.breaker().getUsed(), equalTo(0L));
        }
    }

    private static void dispatch(
        TransportRequestHandler<TransportRequest> handler,
        TransportRequest request,
        TransportChannel channel,
        Task task
    ) {
        try {
            handler.messageReceived(request, channel, task);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testAFailedFetchSurfacesTheCancellation() throws Exception {
        try (RemoteExchange remote = new RemoteExchange()) {
            remote.exchange1.createSinkHandler("q", 3);
            remote.transport1.addRequestHandlingBehavior(
                ExchangeService.EXCHANGE_ACTION_NAME,
                (handler, request, channel, task) -> channel.sendResponse(new TaskCancelledException("cancelled"))
            );
            RemoteSink remoteSink = remote.remoteSink("q");
            QueryScope.Lease observer = remote.registry0.resolve("q");
            ExchangeSourceHandler sourceHandler = new ExchangeSourceHandler(3, threadPool.executor(EXECUTOR));
            PlainActionFuture<Void> sinkDone = new PlainActionFuture<>();
            sourceHandler.addRemoteSink(remoteSink, true, () -> {}, 1, sinkDone);
            Exception e = expectThrows(Exception.class, () -> sinkDone.get(10, TimeUnit.SECONDS));
            assertThat(e.getCause() == null ? e : e.getCause(), instanceOf(TaskCancelledException.class));
            assertBusy(() -> assertThat(observer.scope().leaseCount(), equalTo(1)));
            observer.close();
            assertTrue(closed(observer.scope()));
            remote.exchange1.finishSinkHandler("q", new TaskCancelledException("cancelled"));
        }
    }

    private MockTransportService newTransportService() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(ClusterModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        MockTransportService service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, TransportVersion.current(), threadPool, namedWriteableRegistry),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet()
        );
        service.getTaskManager().setTaskCancellationService(new TaskCancellationService(service));
        service.start();
        service.acceptIncomingRequests();
        return service;
    }
}
