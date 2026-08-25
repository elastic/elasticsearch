/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.elasticsearch.action.search.MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT;
import static org.elasticsearch.script.mustache.TransportSearchTemplateAction.convert;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMultiSearchTemplateAction.class);

    private static final String MSEARCH_TEMPLATE_RENDER_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_MSEARCH + "[render]";
    private static final String MSEARCH_TEMPLATE_RESPONSE_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_MSEARCH + "[response]";
    private static final String MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_MSEARCH + "[failure]";

    /**
     * Heap-overhead factor applied to serialised byte counts.
     * Mirrors {@code TransportMultiSearchAction.SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR}.
     */
    private static final long RENDER_HEAP_OVERHEAD_FACTOR = 2L;

    /**
     * Fixed per-render overhead for the {@link SearchTemplateResponse} wrapper object.
     * Mirrors {@code TransportMultiSearchAction.BASE_RESPONSE_OVERHEAD}.
     */
    private static final long RENDER_BASE_OVERHEAD = 512L;

    /**
     * Wire-byte fallback used when {@link SearchSourceBuilder} serialisation raises an exception.
     * Mirrors {@code TransportMultiSearchAction.SERIALISED_BYTES_FAILURE_FALLBACK}.
     */
    private static final long SERIALISED_BYTES_FAILURE_FALLBACK = 1024L;

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final Predicate<NodeFeature> clusterSupportsFeature;
    private final NodeClient client;
    private final SearchUsageHolder searchUsageHolder;
    private final int allocatedProcessors;
    private final ClusterService clusterService;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportMultiSearchTemplateAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client,
        UsageService usageService,
        ClusterService clusterService,
        FeatureService featureService,
        CircuitBreakerService circuitBreakerService
    ) {
        super(
            MustachePlugin.MULTI_SEARCH_TEMPLATE_ACTION.name(),
            transportService,
            actionFilters,
            MultiSearchTemplateRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.client = client;
        this.searchUsageHolder = usageService.getSearchUsageHolder();
        this.allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        this.clusterService = clusterService;
        this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
    }

    @Override
    protected void doExecute(Task task, MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        int maxConcurrent = request.maxConcurrentSearchRequests() != MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT
            ? request.maxConcurrentSearchRequests()
            : defaultMaxConcurrentSearches();

        int n = request.requests().size();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[n];
        // Three separate accumulators — each released under the same label it was charged with so
        // that ChildMemoryCircuitBreaker's per-category gauge stays balanced.
        long[] renderBytesCharged = { 0L };    // charged/released under [render]
        long[] responseBytesCharged = { 0L };  // charged/released under [response] (success hits only)
        long[] failureBytesCharged = { 0L };   // charged/released under [failure] (errors and substitutes)
        long startTimeNanos = System.nanoTime();

        ActionListener<MultiSearchTemplateResponse> breakerReleasingListener = ActionListener.runAfter(listener, () -> {
            if (renderBytesCharged[0] > 0) {
                circuitBreaker.addWithoutBreaking(-renderBytesCharged[0], MSEARCH_TEMPLATE_RENDER_BREAKER_LABEL);
            }
            if (responseBytesCharged[0] > 0) {
                circuitBreaker.addWithoutBreaking(-responseBytesCharged[0], MSEARCH_TEMPLATE_RESPONSE_BREAKER_LABEL);
            }
            if (failureBytesCharged[0] > 0) {
                circuitBreaker.addWithoutBreaking(-failureBytesCharged[0], MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL);
            }
        });
        // Dedicated (non-wrapping) listener: onResponse forwards only; onFailure decRefs held responses.
        // Using ActionListener.wrap here would be wrong — if downstream onResponse threw, wrap would
        // route to the cleanup onFailure, which decRefs items[], and then respondAndRelease would
        // also decRef the outer MultiSearchTemplateResponse via closeInternal, double-freeing them.
        ActionListener<MultiSearchTemplateResponse> safeListener = new ActionListener<>() {
            @Override
            public void onResponse(MultiSearchTemplateResponse r) {
                breakerReleasingListener.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                for (int i = 0; i < items.length; i++) {
                    if (items[i] != null && items[i].getResponse() != null) {
                        items[i].getResponse().decRef();
                        items[i] = null;
                    }
                }
                breakerReleasingListener.onFailure(e);
            }
        };

        // Render all templates. Simulate-only and render-error slots are filled here;
        // searchable slots collect their SearchRequest for the single multiSearch call below.
        // Rendered source bytes are charged to the circuit breaker per item so that 30 000 × 1 MB
        // of retained JSON cannot accumulate in items[] without bound until the HTTP response is sent.
        List<Integer> searchSlots = new ArrayList<>(n);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(request.indicesOptions());
        multiSearchRequest.maxConcurrentSearchRequests(maxConcurrent);

        // One CountingStreamOutput reused across all renders — no per-item buffer allocation.
        CountingStreamOutput counter = new CountingStreamOutput();

        CircuitBreakingException renderCbe = null; // set on first render-phase CBE; fills subsequent slots
        for (int i = 0; i < n; i++) {
            if (renderCbe != null) {
                // A prior item's render tripped the breaker — fill remaining slots without further work.
                items[i] = new MultiSearchTemplateResponse.Item(null, renderCbe);
                continue;
            }

            SearchTemplateRequest templateRequest = request.requests().get(i);
            SearchTemplateResponse templateResp = new SearchTemplateResponse();
            SearchRequest searchReq;
            try {
                searchReq = convert(
                    templateRequest,
                    templateResp,
                    scriptService,
                    xContentRegistry,
                    clusterSupportsFeature,
                    searchUsageHolder
                );
            } catch (Exception e) {
                templateResp.decRef();
                items[i] = new MultiSearchTemplateResponse.Item(null, e);
                if (ExceptionsHelper.status(e).getStatus() >= 500 && ExceptionsHelper.isNodeOrShardUnavailableTypeException(e) == false) {
                    logger.warn("MultiSearchTemplate convert failure", e);
                }
                continue;
            }

            // Charge for the rendered source and parsed builder retained in items[] until the response
            // is sent. Simulate-only items keep their source for the full response lifetime; real-search
            // items have their source cleared after the inner search, but the charge is released together
            // with all other bytes when the outer listener completes.
            long renderBytes = estimateRenderBytes(templateResp, searchReq, counter);
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(renderBytes, MSEARCH_TEMPLATE_RENDER_BREAKER_LABEL);
                renderBytesCharged[0] += renderBytes;
            } catch (CircuitBreakingException cbe) {
                templateResp.decRef();
                items[i] = new MultiSearchTemplateResponse.Item(null, cbe);
                long subBytes = TransportMultiSearchAction.estimateFailureBytes(cbe);
                circuitBreaker.addWithoutBreaking(subBytes, MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL);
                failureBytesCharged[0] += subBytes;
                // Abort: all search slots queued so far cannot run — replace them with CBE.
                fillRemainingWithCbe(items, searchSlots, 0, cbe);
                searchSlots.clear();
                renderCbe = cbe;
                continue;
            }

            items[i] = new MultiSearchTemplateResponse.Item(templateResp, null);
            if (searchReq != null) {
                multiSearchRequest.add(searchReq);
                searchSlots.add(i);
            }
        }

        if (searchSlots.isEmpty()) {
            finishResponse(items, startTimeNanos, safeListener);
            return;
        }

        multiSearchRequest.setParentTask(client.getLocalNodeId(), task.getId());
        // Non-wrapping listener for the same reason as safeListener: if finishResponse propagates an
        // exception from a downstream listener (e.g., the REST layer throws inside respondAndRelease),
        // ActionListener.wrap would catch it and route to safeListener.onFailure, decRefing items[]
        // that respondAndRelease has already freed via closeInternal. A plain listener lets the
        // exception propagate up the stack instead.
        client.multiSearch(multiSearchRequest, new ActionListener<>() {
            @Override
            public void onResponse(MultiSearchResponse multiSearchResp) {
                // NOTE: the inner _msearch still holds its own REQUEST-breaker reservation for these
                // responses during this callback — its runAfter releases only after our listener
                // returns. We therefore add our own charge for the same bytes we are about to incRef,
                // causing a transient ~2× peak. This is accepted: after the callback the inner
                // release drops the duplicate, leaving only our charge until the outer listener
                // completes. The alternative (handing off the inner reservation) would require
                // coupling to TransportMultiSearchAction internals.
                for (int i = 0; i < multiSearchResp.getResponses().length; i++) {
                    MultiSearchResponse.Item item = multiSearchResp.getResponses()[i];
                    int slot = searchSlots.get(i);
                    if (item.isFailure()) {
                        if (items[slot].getResponse() != null) {
                            items[slot].getResponse().decRef();
                        }
                        items[slot] = new MultiSearchTemplateResponse.Item(null, item.getFailure());
                        long failureBytes = TransportMultiSearchAction.estimateFailureBytes(item.getFailure());
                        try {
                            circuitBreaker.addEstimateBytesAndMaybeBreak(failureBytes, MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL);
                            failureBytesCharged[0] += failureBytes;
                        } catch (CircuitBreakingException cbe) {
                            items[slot] = new MultiSearchTemplateResponse.Item(null, cbe);
                            long subBytes = TransportMultiSearchAction.estimateFailureBytes(cbe);
                            circuitBreaker.addWithoutBreaking(subBytes, MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL);
                            failureBytesCharged[0] += subBytes;
                            fillRemainingWithCbe(items, searchSlots, i + 1, cbe);
                            break;
                        }
                    } else {
                        // Charge breaker BEFORE incRef/setResponse so cleanup is safe if breaker throws.
                        long responseBytes = TransportMultiSearchAction.estimateActualBytes(item.getResponse());
                        try {
                            circuitBreaker.addEstimateBytesAndMaybeBreak(responseBytes, MSEARCH_TEMPLATE_RESPONSE_BREAKER_LABEL);
                        } catch (CircuitBreakingException cbe) {
                            items[slot].getResponse().decRef();
                            items[slot] = new MultiSearchTemplateResponse.Item(null, cbe);
                            long subBytes = TransportMultiSearchAction.estimateFailureBytes(cbe);
                            circuitBreaker.addWithoutBreaking(subBytes, MSEARCH_TEMPLATE_FAILURE_BREAKER_LABEL);
                            failureBytesCharged[0] += subBytes;
                            fillRemainingWithCbe(items, searchSlots, i + 1, cbe);
                            break;
                        }
                        responseBytesCharged[0] += responseBytes;
                        item.getResponse().incRef(); // incRef before storing so the reference is always reachable
                        items[slot].getResponse().setResponse(item.getResponse());
                        items[slot].getResponse().setSource(null); // rendered JSON no longer needed after search
                    }
                }
                finishResponse(items, startTimeNanos, safeListener);
            }

            @Override
            public void onFailure(Exception e) {
                safeListener.onFailure(e);
            }
        });
    }

    /**
     * Estimates heap bytes retained by one rendered item while it sits in {@code items[]} waiting
     * to be sent as the HTTP response.
     * <ul>
     *   <li>{@link #RENDER_BASE_OVERHEAD} — fixed wrapper overhead for the {@link SearchTemplateResponse}.</li>
     *   <li>{@code source.length()} — raw bytes of the retained {@link BytesReference}; counted once
     *       since it is already a byte array view, not expanded into Java objects.</li>
     *   <li>For a real search: {@link #RENDER_HEAP_OVERHEAD_FACTOR} × serialised
     *       {@link SearchSourceBuilder} size, matching the 2× factor used by
     *       {@link TransportMultiSearchAction#estimateActualBytes}. The builder is the in-memory
     *       parsed representation of the source and is retained until the inner search executes.</li>
     *   <li>For simulate-only (no builder): {@link #RENDER_HEAP_OVERHEAD_FACTOR} × {@code source.length()}
     *       as a proxy, since there is no builder to serialise.</li>
     * </ul>
     *
     * @param counter a reusable {@link CountingStreamOutput}; reset before each use, never allocated
     */
    private static long estimateRenderBytes(SearchTemplateResponse templateResp, SearchRequest searchReq, CountingStreamOutput counter) {
        long bytes = RENDER_BASE_OVERHEAD;
        BytesReference source = templateResp.getSource();
        if (source != null) {
            bytes += source.length();
        }
        SearchSourceBuilder ssb = searchReq != null ? searchReq.source() : null;
        if (ssb != null) {
            bytes += RENDER_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(counter, ssb);
        } else if (source != null) {
            bytes += RENDER_HEAP_OVERHEAD_FACTOR * source.length();
        }
        return bytes;
    }

    /**
     * Resets {@code counter} before writing and reads {@link CountingStreamOutput#position()} after.
     * Returns {@link #SERIALISED_BYTES_FAILURE_FALLBACK} and logs a warning if serialisation raises
     * an exception; in that case {@code counter} is reset so the next call starts from a clean state.
     */
    private static long tryCountSerializedBytes(CountingStreamOutput counter, SearchSourceBuilder ssb) {
        try {
            counter.reset();
            ssb.writeTo(counter);
            return counter.position();
        } catch (Exception e) {
            counter.reset();
            logger.warn("Failed to count serialised bytes for SearchSourceBuilder render estimate", e);
            return SERIALISED_BYTES_FAILURE_FALLBACK;
        }
    }

    /**
     * Fills remaining unprocessed search slots (from {@code fromSearchIdx} onward in {@code searchSlots})
     * with the given {@link CircuitBreakingException}, decRefing any templateResp they hold.
     */
    private static void fillRemainingWithCbe(
        MultiSearchTemplateResponse.Item[] items,
        List<Integer> searchSlots,
        int fromSearchIdx,
        CircuitBreakingException cbe
    ) {
        for (int i = fromSearchIdx; i < searchSlots.size(); i++) {
            int slot = searchSlots.get(i);
            if (items[slot] != null && items[slot].getResponse() != null) {
                items[slot].getResponse().decRef();
            }
            items[slot] = new MultiSearchTemplateResponse.Item(null, cbe);
        }
    }

    private static void finishResponse(
        MultiSearchTemplateResponse.Item[] items,
        long startTimeNanos,
        ActionListener<MultiSearchTemplateResponse> safeListener
    ) {
        long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        ActionListener.respondAndRelease(safeListener, new MultiSearchTemplateResponse(items, tookMillis));
    }

    /**
     * Default search concurrency: mirrors the {@code defaultMaxConcurrentSearches} formula from
     * {@link TransportMultiSearchAction}.
     */
    private int defaultMaxConcurrentSearches() {
        int numDataNodes = clusterService.state().getNodes().getDataNodes().size();
        int defaultSearchThreadPoolSize = Math.min(ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors), 10);
        return Math.max(1, numDataNodes * defaultSearchThreadPoolSize);
    }
}
