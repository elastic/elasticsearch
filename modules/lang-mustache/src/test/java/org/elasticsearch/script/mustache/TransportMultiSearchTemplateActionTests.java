/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchTemplateActionTests extends ESTestCase {

    // ── Helper circuit breakers ──────────────────────────────────────────────

    /**
     * Trips on the first {@code addEstimateBytesAndMaybeBreak} call, regardless of size.
     */
    private static final class TrippingCircuitBreaker extends NoopCircuitBreaker {
        TrippingCircuitBreaker() {
            super("test");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            throw new CircuitBreakingException("test breaker tripped", getDurability());
        }

        @Override
        public void addWithoutBreaking(long bytes) {}

        @Override
        public long getUsed() {
            return 0;
        }
    }

    /**
     * Trips when a single charge exceeds {@code limitBytes}.
     * Small renders pass; renders larger than the threshold trip the breaker, proving size matters.
     */
    private static final class ThresholdCircuitBreaker extends NoopCircuitBreaker {
        private final long limitBytes;

        ThresholdCircuitBreaker(long limitBytes) {
            super("test");
            this.limitBytes = limitBytes;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (bytes > limitBytes) {
                throw new CircuitBreakingException("threshold breaker tripped: " + bytes + " > " + limitBytes, getDurability());
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {}
    }

    /**
     * Never trips; tracks the cumulative net bytes charged across all breaker calls.
     * Positive bytes come from {@code addEstimateBytesAndMaybeBreak} charges;
     * negative bytes come from {@code addWithoutBreaking} release calls.
     * After correct cleanup, {@link #getUsed()} returns {@code 0}.
     * {@link #getPeakNetBytes()} records the highest {@code netBytes} value seen, which is always
     * positive when at least one charge was issued — allowing tests to distinguish "released after
     * charging" from "nothing was charged at all".
     *
     * <p>Pass {@code tripOnAnyCharge=true} to simulate a render-phase CBE while still
     * tracking the release calls that follow.
     */
    private static final class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private long netBytes = 0;
        private long peakNetBytes = 0;
        private final boolean tripOnAnyCharge;

        TrackingCircuitBreaker() {
            this(false);
        }

        TrackingCircuitBreaker(boolean tripOnAnyCharge) {
            super("test");
            this.tripOnAnyCharge = tripOnAnyCharge;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (tripOnAnyCharge) {
                throw new CircuitBreakingException("tracking breaker tripped", getDurability());
            }
            netBytes += bytes;
            peakNetBytes = Math.max(peakNetBytes, netBytes);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            netBytes += bytes;
            peakNetBytes = Math.max(peakNetBytes, netBytes);
        }

        @Override
        public long getUsed() {
            return netBytes;
        }

        public long getPeakNetBytes() {
            return peakNetBytes;
        }
    }

    /**
     * Passes render-phase charges (label {@code "msearch[render]"}); trips on response-phase
     * charges (label {@code "msearch[response]"}). Allows all {@code addWithoutBreaking} release
     * calls through. Used to verify that a hit-side CBE fills remaining slots without aborting
     * before {@code client.multiSearch()} is called.
     */
    private static final class ResponsePhaseTrippingBreaker extends NoopCircuitBreaker {
        ResponsePhaseTrippingBreaker() {
            super("test");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (label != null && label.contains("[response]")) {
                throw new CircuitBreakingException("response phase tripped", getDurability());
            }
            // [render] charges pass through — that is the point of this breaker.
            // [failure] charges (e.g. addEstimateBytesAndMaybeBreak for failure-item bytes) also pass
            // through; in testResponsePhaseCbeFillsRemainingSlots all inner searches succeed so no
            // failure-item path is exercised and this case does not arise.
        }

        @Override
        public void addWithoutBreaking(long bytes) {}
    }

    // ── Helper factories ─────────────────────────────────────────────────────

    /**
     * Builds a minimal ScriptService mock where all templates render to {@code {"size": 0}}.
     * This parses correctly with NamedXContentRegistry.EMPTY since SearchSourceBuilder
     * handles "size" natively without requiring named XContent entries.
     */
    private static ScriptService allSuccessScriptService() {
        ScriptService scriptService = mock(ScriptService.class);
        TemplateScript.Factory factory = params -> new TemplateScript(params) {
            @Override
            public String execute() {
                return "{\"size\": 0}";
            }
        };
        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenReturn(factory);
        return scriptService;
    }

    private static SearchTemplateRequest templateRequest() {
        SearchTemplateRequest req = new SearchTemplateRequest();
        req.setRequest(new SearchRequest("test"));
        req.setScriptType(ScriptType.INLINE);
        req.setScript("{\"size\": 0}");
        return req;
    }

    private static TransportMultiSearchTemplateAction buildAction(
        ThreadPool threadPool,
        NodeClient client,
        ClusterService clusterService,
        CircuitBreaker breaker
    ) {
        return buildAction(threadPool, client, clusterService, breaker, allSuccessScriptService());
    }

    private static TransportMultiSearchTemplateAction buildAction(
        ThreadPool threadPool,
        NodeClient client,
        ClusterService clusterService,
        CircuitBreaker breaker,
        ScriptService scriptService
    ) {
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ba -> DiscoveryNodeUtils.builder("local").applySettings(Settings.EMPTY).address(ba.publishAddress()).build(),
            null,
            Collections.emptySet()
        );
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), any())).thenReturn(false);
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return new TransportMultiSearchTemplateAction(
            Settings.EMPTY,
            transportService,
            actionFilters,
            scriptService,
            NamedXContentRegistry.EMPTY,
            client,
            new UsageService(),
            clusterService,
            featureService,
            breakerService
        );
    }

    private static ClusterService clusterServiceWithDataNodes(int numDataNodes) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(DiscoveryNodeUtils.builder("master").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build());
        nodesBuilder.localNodeId("master");
        for (int i = 0; i < numDataNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("data-" + i).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build());
        }
        ClusterState state = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /**
     * Verifies that all non-simulate requests are sent in a single {@code client.multiSearch()} call
     * with {@code maxConcurrentSearchRequests} propagated from the template request.
     */
    public void testAllRequestsSentInSingleMultiSearch() throws Exception {
        int maxConcurrent = 50;
        int numRequests = 150;
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicInteger multiSearchCalls = new AtomicInteger();
        AtomicReference<Integer> observedSize = new AtomicReference<>();
        AtomicReference<Integer> observedConcurrency = new AtomicReference<>();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    observedSize.set(request.requests().size());
                    observedConcurrency.set(request.maxConcurrentSearchRequests());
                    MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
                    for (int i = 0; i < items.length; i++) {
                        SearchResponse sr = SearchResponseUtils.response().build();
                        items[i] = new MultiSearchResponse.Item(sr, null);
                    }
                    MultiSearchResponse response = new MultiSearchResponse(items, 1L);
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            request.maxConcurrentSearchRequests(maxConcurrent);
            for (int i = 0; i < numRequests; i++) {
                request.add(templateRequest());
            }

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, new NoopCircuitBreaker("test"));
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(numRequests));
                for (MultiSearchTemplateResponse.Item item : response) {
                    assertFalse(item.isFailure());
                }
                l.onResponse(null);
            }));
            future.get();

            assertThat("all requests must go in one multiSearch call", multiSearchCalls.get(), equalTo(1));
            assertThat("all non-simulate requests included", observedSize.get(), equalTo(numRequests));
            assertThat("maxConcurrentSearchRequests propagated", observedConcurrency.get(), equalTo(maxConcurrent));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that simulate-only requests never trigger a multiSearch call —
     * they are rendered but not searched, so they bypass the inner msearch entirely.
     */
    public void testSimulateOnlyRequestsSkipMultiSearch() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicInteger multiSearchCalls = new AtomicInteger();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    listener.onFailure(new RuntimeException("multiSearch should not be called for simulate-only requests"));
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            int numRequests = randomIntBetween(1, 20);
            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                SearchTemplateRequest req = templateRequest();
                req.setSimulate(true);
                request.add(req);
            }

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, new NoopCircuitBreaker("test"));
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(numRequests));
                for (MultiSearchTemplateResponse.Item item : response) {
                    assertFalse(item.isFailure());
                    assertThat(item.getResponse(), not(nullValue()));
                    // Simulate-only: source is rendered but no inner SearchResponse
                    assertThat(item.getResponse().getResponse(), nullValue());
                    assertThat(item.getResponse().getSource(), not(nullValue()));
                }
                l.onResponse(null);
            }));
            future.get();

            assertThat("multiSearch must not be called for simulate-only requests", multiSearchCalls.get(), equalTo(0));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that a circuit-breaker trip during the render phase produces partial CBE results
     * and never reaches {@code client.multiSearch()} — the breaker trips before any search is issued.
     */
    public void testCircuitBreakerTripDuringRenderSkipsMultiSearch() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicInteger multiSearchCalls = new AtomicInteger();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    listener.onFailure(new RuntimeException("multiSearch must not be called after render CBE"));
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            int numRequests = 3;
            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                request.add(templateRequest());
            }

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, new TrippingCircuitBreaker());
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                // Must return a response object (partial results), not a top-level failure
                assertThat(response.getResponses().length, equalTo(numRequests));
                for (MultiSearchTemplateResponse.Item item : response) {
                    assertTrue("expected CBE failure for every item when breaker trips on first render", item.isFailure());
                    assertThat(item.getFailure(), instanceOf(CircuitBreakingException.class));
                }
                l.onResponse(null);
            }));
            future.get();

            assertThat("multiSearch must not be called when render phase trips the breaker", multiSearchCalls.get(), equalTo(0));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that a large rendered source trips the circuit breaker based on its size, not
     * unconditionally. The {@link ThresholdCircuitBreaker} passes small renders (e.g., {@code {"size":0}})
     * but trips when the estimate for a ~300 KB source exceeds the 50 KB limit. This proves that the
     * render estimate is proportional to source size, unlike {@link TrippingCircuitBreaker} (which would
     * trip regardless of size and thus cannot distinguish the two cases).
     */
    public void testLargeRenderedSourceTripsBreakerBeforeSearch() throws Exception {
        // Build ~300 KB of valid SearchSourceBuilder JSON via stored_fields (a plain string array).
        // stored_fields is a built-in field that parses with NamedXContentRegistry.EMPTY.
        StringBuilder sb = new StringBuilder("{\"size\":0,\"stored_fields\":[");
        for (int j = 0; j < 30_000; j++) {
            if (j > 0) sb.append(",");
            sb.append("\"f").append(j).append("\"");
        }
        sb.append("]}");
        String largeBody = sb.toString();

        ScriptService largeSrcScriptService = mock(ScriptService.class);
        TemplateScript.Factory factory = params -> new TemplateScript(params) {
            @Override
            public String execute() {
                return largeBody;
            }
        };
        when(largeSrcScriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenReturn(factory);

        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicInteger multiSearchCalls = new AtomicInteger();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    listener.onFailure(new RuntimeException("multiSearch must not be called after render CBE"));
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            // 50 KB threshold: the ~300 KB source produces a render estimate >> 50 KB (trips);
            // a small {"size":0} would produce an estimate << 50 KB (passes), proving size matters.
            TransportMultiSearchTemplateAction action = buildAction(
                threadPool,
                client,
                clusterService,
                new ThresholdCircuitBreaker(50 * 1024),
                largeSrcScriptService
            );

            int numRequests = 5;
            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                request.add(templateRequest());
            }

            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(numRequests));
                for (MultiSearchTemplateResponse.Item item : response) {
                    assertTrue("large render must produce CBE item failure, not OOM", item.isFailure());
                    assertThat(item.getFailure(), instanceOf(CircuitBreakingException.class));
                }
                l.onResponse(null);
            }));
            future.get();

            assertThat("multiSearch must not be called when render trips the breaker", multiSearchCalls.get(), equalTo(0));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that the circuit breaker's net usage returns to zero after a successful request
     * (render, search, response — all succeed). The {@link TrackingCircuitBreaker} accumulates
     * positive bytes on charges and negative bytes on releases; the final balance must be zero
     * after the {@code runAfter} cleanup fires.
     */
    public void testBreakerNetZeroAfterSuccess() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        TrackingCircuitBreaker tracker = new TrackingCircuitBreaker();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
                    for (int i = 0; i < items.length; i++) {
                        items[i] = new MultiSearchResponse.Item(SearchResponseUtils.response().build(), null);
                    }
                    MultiSearchResponse response = new MultiSearchResponse(items, 1L);
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            int numRequests = 3;
            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                request.add(templateRequest());
            }

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, tracker);
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, r) -> l.onResponse(null)));
            future.get();

            // By the time future.get() returns, the runAfter cleanup has already fired
            // (single-threaded execution: listener completes, then runAfter releases bytes).
            assertThat("bytes must have been charged (not a no-op breaker)", tracker.getPeakNetBytes(), not(equalTo(0L)));
            assertThat("breaker must be fully released after a successful request", tracker.getUsed(), equalTo(0L));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that the circuit breaker's net usage returns to zero after a render-phase CBE.
     * Only the failure-substitute bytes are charged to the breaker (via {@code addWithoutBreaking});
     * those same bytes must be released in the {@code runAfter} cleanup.
     */
    public void testBreakerNetZeroAfterRenderCbe() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        // tripOnAnyCharge=true: trips on first addEstimateBytesAndMaybeBreak (render charge),
        // but still tracks addWithoutBreaking calls so net can be verified.
        TrackingCircuitBreaker tracker = new TrackingCircuitBreaker(true);
        AtomicInteger multiSearchCalls = new AtomicInteger();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    listener.onFailure(new RuntimeException("multiSearch must not be called after render CBE"));
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < 3; i++) {
                request.add(templateRequest());
            }

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, tracker);
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, r) -> l.onResponse(null)));
            future.get();

            assertThat("multiSearch must not be called when render phase trips the breaker", multiSearchCalls.get(), equalTo(0));
            // peakNetBytes > 0 proves that failure-substitute bytes were charged (the addWithoutBreaking
            // calls after the render CBE): the test is not trivially net-zero because nothing was ever charged.
            assertThat("failure-substitute bytes must have been charged", tracker.getPeakNetBytes(), not(equalTo(0L)));
            assertThat("breaker must be fully released after a render-phase CBE", tracker.getUsed(), equalTo(0L));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that a {@code convert()} failure on one slot does not abort later slots.
     * Slot 1's template compile throws a {@link RuntimeException}; slots 0 and 2 succeed and
     * get real inner search responses. Slot 1 receives a non-CBE item failure.
     */
    public void testConvertErrorDoesNotAbortOtherSlots() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);

        TemplateScript.Factory successFactory = params -> new TemplateScript(params) {
            @Override
            public String execute() {
                return "{\"size\": 0}";
            }
        };
        ScriptService scriptService = mock(ScriptService.class);
        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenReturn(successFactory)
            .thenThrow(new RuntimeException("slot 1 compile error"))
            .thenReturn(successFactory);

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            // Slot 1 is a convert failure → only 2 searches are sent to multiSearch (slots 0 and 2).
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    assertThat("only slots 0 and 2 reach multiSearch", request.requests().size(), equalTo(2));
                    MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[2];
                    items[0] = new MultiSearchResponse.Item(SearchResponseUtils.response().build(), null);
                    items[1] = new MultiSearchResponse.Item(SearchResponseUtils.response().build(), null);
                    MultiSearchResponse response = new MultiSearchResponse(items, 1L);
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            request.add(templateRequest()); // slot 0 — success
            request.add(templateRequest()); // slot 1 — compile throws
            request.add(templateRequest()); // slot 2 — success

            TransportMultiSearchTemplateAction action = buildAction(
                threadPool,
                client,
                clusterService,
                new NoopCircuitBreaker("test"),
                scriptService
            );
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(3));

                MultiSearchTemplateResponse.Item item0 = response.getResponses()[0];
                assertFalse("slot 0 must succeed", item0.isFailure());
                assertTrue("slot 0 must have an inner search response", item0.getResponse().hasResponse());

                MultiSearchTemplateResponse.Item item1 = response.getResponses()[1];
                assertTrue("slot 1 must be a failure", item1.isFailure());
                assertThat("slot 1 must be a non-CBE compile error", item1.getFailure(), not(instanceOf(CircuitBreakingException.class)));

                MultiSearchTemplateResponse.Item item2 = response.getResponses()[2];
                assertFalse("slot 2 must succeed despite slot 1 failure", item2.isFailure());
                assertTrue("slot 2 must have an inner search response", item2.getResponse().hasResponse());

                l.onResponse(null);
            }));
            future.get();
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that a CBE during the response phase (charging for hit bytes) fills remaining
     * slots with CBE item failures, but only AFTER {@code client.multiSearch()} has been called —
     * unlike a render-phase CBE, which skips the inner search entirely.
     */
    public void testResponsePhaseCbeFillsRemainingSlots() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicInteger multiSearchCalls = new AtomicInteger();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            int numRequests = 3;
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    multiSearchCalls.incrementAndGet();
                    MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
                    for (int i = 0; i < items.length; i++) {
                        items[i] = new MultiSearchResponse.Item(SearchResponseUtils.response().build(), null);
                    }
                    MultiSearchResponse response = new MultiSearchResponse(items, 1L);
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local";
                }
            };

            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            for (int i = 0; i < numRequests; i++) {
                request.add(templateRequest());
            }

            // Render charges (label "msearch[render]") pass; first response charge (label "msearch[response]") trips.
            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, new ResponsePhaseTrippingBreaker());
            Task task = request.createTask(1L, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, response) -> {
                assertThat(response.getResponses().length, equalTo(numRequests));
                for (MultiSearchTemplateResponse.Item item : response) {
                    assertTrue("response-phase CBE must produce CBE item failure for every slot", item.isFailure());
                    assertThat(item.getFailure(), instanceOf(CircuitBreakingException.class));
                }
                l.onResponse(null);
            }));
            future.get();

            assertThat(
                "multiSearch must be called before the response-phase CBE fires (unlike render-phase)",
                multiSearchCalls.get(),
                equalTo(1)
            );
        } finally {
            assertTrue(terminate(threadPool));
        }
    }

    /**
     * Verifies that the inner {@link org.elasticsearch.action.search.MultiSearchRequest} has its
     * parent task set to the outer msearch/template task. This ensures the inner searches appear
     * as children in the tasks API and are cancelled when the parent task is cancelled.
     */
    public void testParentTaskSetOnInnerMultiSearch() throws Exception {
        ClusterService clusterService = clusterServiceWithDataNodes(1);
        AtomicReference<org.elasticsearch.tasks.TaskId> observedParentTask = new AtomicReference<>();

        Settings settings = Settings.builder().put("node.name", getTestName()).build();
        ThreadPool threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        try {
            NodeClient client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
                @Override
                public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                    observedParentTask.set(request.getParentTask());
                    MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
                    for (int i = 0; i < items.length; i++) {
                        items[i] = new MultiSearchResponse.Item(SearchResponseUtils.response().build(), null);
                    }
                    MultiSearchResponse response = new MultiSearchResponse(items, 1L);
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                }

                @Override
                public String getLocalNodeId() {
                    return "local-node";
                }
            };

            MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
            request.add(templateRequest());

            TransportMultiSearchTemplateAction action = buildAction(threadPool, client, clusterService, new NoopCircuitBreaker("test"));
            long taskId = randomNonNegativeLong();
            Task task = request.createTask(taskId, "type", "action", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            action.execute(task, request, future.delegateFailure((l, r) -> l.onResponse(null)));
            future.get();

            org.elasticsearch.tasks.TaskId parentTask = observedParentTask.get();
            assertNotNull("inner multiSearch must have a parent task set", parentTask);
            assertThat("parent task node ID must match getLocalNodeId()", parentTask.getNodeId(), equalTo("local-node"));
            assertThat("parent task ID must match the outer task", parentTask.getId(), equalTo(taskId));
        } finally {
            assertTrue(terminate(threadPool));
        }
    }
}
