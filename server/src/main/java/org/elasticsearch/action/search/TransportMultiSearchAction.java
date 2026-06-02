/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.CountingStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.Text;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

public class TransportMultiSearchAction extends HandledTransportAction<MultiSearchRequest, MultiSearchResponse> {

    public static final String NAME = "indices:data/read/msearch";
    public static final ActionType<MultiSearchResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportMultiSearchAction.class);

    /**
     * Fixed per-response overhead charged against the circuit breaker for every sub-search
     * response, regardless of the number of hits.
     * <p>
     * Covers the following objects (64-bit JVM, compressed oops):
     * <ul>
     *   <li>{@code SearchResponse} shell — 16 B header + ~10 reference fields ≈ 56 B</li>
     *   <li>{@code SearchHits} wrapper — 16 B header + hits array ref + 3 primitive fields ≈ 48 B</li>
     *   <li>{@code TotalHits} — 16 B header + long + enum ref ≈ 32 B</li>
     *   <li>{@code Clusters} metadata object + its internal maps ≈ 80 B</li>
     *   <li>Empty {@code ShardSearchFailure[]} array ≈ 16 B</li>
     *   <li>{@code SimpleRefCounted}, {@code LeakTracker} wrapper, and release-tracking lists ≈ 120 B</li>
     *   <li>Miscellaneous padding and amortised per-response allocations ≈ 140 B</li>
     * </ul>
     * Rounds to 512 B as a conservative upper bound; intentionally over-estimates the structural
     * shell to account for amortised per-response allocations not sized individually.
     */
    static final long BASE_RESPONSE_OVERHEAD = 512L;

    /**
     * Per-hit structural overhead charged against the circuit breaker for each {@link SearchHit},
     * excluding source bytes and field entries (both measured separately).
     * <p>
     * Covers the following objects (64-bit JVM, compressed oops):
     * <ul>
     *   <li>{@code SearchHit} shell — 16 B header + ~10 reference/primitive fields ≈ 96 B</li>
     *   <li>{@code _id} String object + its {@code char[]} backing array (avg 16-char id) ≈ 72 B</li>
     *   <li>{@code documentFields} {@code HashMap} shell + internal {@code Node[]} array ≈ 48 B</li>
     *   <li>{@code metaFields} {@code HashMap} shell + internal {@code Node[]} array ≈ 48 B</li>
     *   <li>{@code SearchShardTarget} + {@code ShardId} + index-name String (amortised) ≈ 96 B</li>
     *   <li>Miscellaneous per-hit padding ≈ 40 B</li>
     * </ul>
     * Source bytes are measured precisely via {@link SearchHit#rawSourceLength()}.
     * Field entries are counted separately via {@link #PER_FIELD_OVERHEAD}.
     * Sort values ({@link org.elasticsearch.search.SearchSortValues}) are charged separately in
     * {@link #estimateHitBytes} when present.
     * Matched queries ({@code matched_queries}) and nested identity ({@code _nested}) are not
     * estimated — both are typically small but represent known omissions.
     * Rounds to 400 B as a conservative upper bound for the shell components listed above.
     */
    static final long PER_HIT_OBJECT_OVERHEAD = 400L;

    /**
     * Per-field-entry structural overhead charged against the circuit breaker for each entry
     * across {@link SearchHit#getDocumentFields()} and {@link SearchHit#getMetadataFields()}.
     * Does not include the stored values inside the field (e.g. a large {@code keyword} value).
     * <p>
     * Covers the following objects (64-bit JVM, compressed oops):
     * <ul>
     *   <li>{@code HashMap.Node} shell — 16 B header + hash int + 3 refs ≈ 32 B</li>
     *   <li>Field-name {@code String} object + its {@code char[]} backing array (avg 16-char name) ≈ 72 B</li>
     *   <li>{@code DocumentField} shell — 16 B header + name ref + values ref ≈ 24 B</li>
     *   <li>{@code ArrayList} for values — 16 B header + elementData ref + size int ≈ 32 B</li>
     *   <li>Minimum {@code Object[]} backing array (capacity 1) ≈ 24 B</li>
     * </ul>
     * Rounds to 200 B as a conservative upper bound. Stored field values are sized separately
     * via {@link #estimateValueBytes} for each value in {@link DocumentField#getValues()}.
     */
    static final long PER_FIELD_OVERHEAD = 200L;

    /**
     * Multiplier applied to wire-serialised byte counts (from {@link #tryCountSerializedBytes})
     * to approximate coordinator heap. The heap form of structured objects is typically 2–5×
     * larger than the wire form: exceptions carry {@code StackTraceElement[]} arrays (~80 B per
     * frame), suggest and profile nodes carry Java collection overhead, and highlight
     * {@link Text} objects add object shells beyond their string content. Using 2× is a
     * conservative lower bound within the safe over-estimate range for circuit-breaker purposes.
     * <p>
     * Residual OOM risk: responses dominated by deep stack traces (many shard failures with
     * 50+ frame traces) or large profile trees may reach 4–5× wire size on heap. In those cases
     * this multiplier under-estimates, and the REQUEST breaker may not trip until the parent
     * real-memory breaker intervenes.
     */
    static final long SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR = 2L;

    private final int allocatedProcessors;
    private final ClusterService clusterService;
    private final LongSupplier relativeTimeProvider;
    private final NodeClient client;
    private final ProjectResolver projectResolver;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public TransportMultiSearchAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        NodeClient client,
        ProjectResolver projectResolver,
        CircuitBreakerService circuitBreakerService
    ) {
        this(
            actionFilters,
            transportService,
            clusterService,
            EsExecutors.allocatedProcessors(settings),
            System::nanoTime,
            client,
            projectResolver,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
    }

    TransportMultiSearchAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ClusterService clusterService,
        int allocatedProcessors,
        LongSupplier relativeTimeProvider,
        NodeClient client,
        ProjectResolver projectResolver,
        CircuitBreaker circuitBreaker
    ) {
        super(TYPE.name(), transportService, actionFilters, MultiSearchRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.allocatedProcessors = allocatedProcessors;
        this.relativeTimeProvider = relativeTimeProvider;
        this.client = client;
        this.projectResolver = projectResolver;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    protected void doExecute(Task task, MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        final long relativeStartTime = relativeTimeProvider.getAsLong();

        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(projectResolver.getProjectId(), ClusterBlockLevel.READ);

        int maxConcurrentSearches = request.maxConcurrentSearchRequests();
        if (maxConcurrentSearches == MultiSearchRequest.MAX_CONCURRENT_SEARCH_REQUESTS_DEFAULT) {
            maxConcurrentSearches = defaultMaxConcurrentSearches(allocatedProcessors, clusterState);
        }

        Queue<SearchRequestSlot> searchRequestSlots = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < request.requests().size(); i++) {
            SearchRequest searchRequest = request.requests().get(i);
            searchRequest.setParentTask(client.getLocalNodeId(), task.getId());
            searchRequest.setBufferSubSearchResponseForMultiSearch(true);
            searchRequestSlots.add(new SearchRequestSlot(searchRequest, i));
        }

        int numRequests = request.requests().size();
        final AtomicArray<MultiSearchResponse.Item> responses = new AtomicArray<>(numRequests);
        final AtomicInteger responseCounter = new AtomicInteger(numRequests);
        // Each completed sub-search stays in {@code responses} until the last one finishes. Incremental bytes (hits,
        // suggest, etc.) are reserved here; query-phase aggregation bytes are handed off from {@link QueryPhaseResultConsumer}
        // and released together when the combined {@link MultiSearchResponse} is delivered.
        final MultiSearchBreakerAccounting breakerAccounting = new MultiSearchBreakerAccounting();
        final ActionListener<MultiSearchResponse> breakerReleasingListener = ActionListener.runAfter(
            listener,
            breakerAccounting::releaseAll
        );
        int numConcurrentSearches = Math.min(numRequests, maxConcurrentSearches);
        for (int i = 0; i < numConcurrentSearches; i++) {
            executeSearch(searchRequestSlots, responses, responseCounter, breakerReleasingListener, relativeStartTime, breakerAccounting);
        }
    }

    /*
     * This is not perfect and makes a big assumption, that all nodes have the same thread pool size / have the number of processors and
     * that shard of the indices the search requests go to are more or less evenly distributed across all nodes in the cluster. But I think
     * it is a good enough default for most cases, if not then the default should be overwritten in the request itself.
     */
    static int defaultMaxConcurrentSearches(final int allocatedProcessors, final ClusterState state) {
        int numDateNodes = state.getNodes().getDataNodes().size();
        // we bound the default concurrency to preserve some search thread pool capacity for other searches
        final int defaultSearchThreadPoolSize = Math.min(ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors), 10);
        return Math.max(1, numDateNodes * defaultSearchThreadPoolSize);
    }

    /**
     * Estimates coordinator heap for a single sub-search {@link SearchResponse} while it is buffered in the msearch
     * {@link AtomicArray}. Used with {@link CircuitBreaker#REQUEST} in post-receipt reservation: the estimate is taken
     * when the response arrives, not from request metadata alone.
     * <p>
     * Included:
     * <ul>
     *   <li>Response shell ({@link #BASE_RESPONSE_OVERHEAD}).</li>
     *   <li>Top-level and nested hits ({@link #estimateHitBytes}).</li>
     *   <li>Merged aggregations when {@link SearchResponse#getQueryPhaseAggregationBreakerBytes()} is zero
     *       (no handoff from {@link QueryPhaseResultConsumer}), via
     *       {@link DelayableWriteable#getUncompressedSerializedSize}. When handoff bytes are non-zero, aggregations
     *       are already on the request breaker from query-phase reduce and are not counted again here.
     *       A {@code top_hits} aggregation embeds hits in the agg tree, so the same document bytes may be
     *       counted both there and in top-level hits — a known over-estimate.</li>
     *   <li>Shard failures, suggest results ({@link Suggest}), profile results
     *       ({@link SearchProfileResults}), CCS clusters metadata ({@link SearchResponse.Clusters}),
     *       highlight fragments ({@link HighlightField}), and explanations
     *       ({@link org.apache.lucene.search.Explanation}) — each serialised via
     *       {@link CountingStreamOutput} then multiplied by {@link #SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR}
     *       to account for Java object overhead beyond the wire bytes (e.g. {@code StackTraceElement[]}
     *       arrays in exceptions, collection nodes in suggest/profile structures).
     *       CCS clusters are only counted when per-cluster tracking is active
     *       ({@link SearchResponse.Clusters#hasClusterObjects()} is true); for non-CCS searches the
     *       Clusters object holds only three integers already absorbed by {@link #BASE_RESPONSE_OVERHEAD}.
     *       For CCS, per-cluster {@link ShardSearchFailure} entries may overlap with top-level shard
     *       failures, resulting in a known over-estimate.</li>
     *   <li>Stored {@link DocumentField} values — sized via {@link #estimateValueBytes}
     *       per value per hit; complements the structural overhead in {@link #PER_FIELD_OVERHEAD}.</li>
     * </ul>
     * Estimates use stored {@link SearchHit#rawSourceLength()} and never call
     * {@link SearchHit#getSourceRef()}, which can decompress and mutate {@code _source}.
     */
    static long estimateActualBytes(SearchResponse response) {
        long bytes = BASE_RESPONSE_OVERHEAD;

        // One CountingStreamOutput for the entire estimation: reset() before each use.
        CountingStreamOutput counter = new CountingStreamOutput();
        counter.setTransportVersion(TransportVersion.current());

        for (SearchHit hit : response.getHits().getHits()) {
            bytes += estimateHitBytes(hit, counter);
        }

        if (response.hasAggregations() && response.getQueryPhaseAggregationBreakerBytes() == 0) {
            try {
                bytes += DelayableWriteable.getUncompressedSerializedSize(response.getAggregations());
            } catch (UncheckedIOException e) {
                logger.warn("msearch circuit breaker: failed to estimate aggregation bytes", e);
            }
        }

        ShardSearchFailure[] failures = response.getShardFailures();
        if (failures.length > 0) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                out -> out.writeArray(failures),
                "msearch circuit breaker: failed to estimate shard failure bytes"
            );
        }

        Suggest suggest = response.getSuggest();
        if (suggest != null) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                suggest,
                "msearch circuit breaker: failed to estimate suggest bytes"
            );
        }

        SearchProfileResults profileResults = response.getSearchProfileResults();
        if (profileResults != null) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                profileResults,
                "msearch circuit breaker: failed to estimate profile bytes"
            );
        }

        // CCS only: for non-CCS responses the Clusters object holds just three integers, already
        // absorbed by BASE_RESPONSE_OVERHEAD. Only serialize when per-cluster tracking is active.
        SearchResponse.Clusters clusters = response.getClusters();
        if (clusters.hasClusterObjects()) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                clusters,
                "msearch circuit breaker: failed to estimate cluster bytes"
            );
        }

        return bytes;
    }

    /**
     * Counts bytes that {@code writeable} would occupy when serialised to the transport wire format.
     * Resets {@code counter} before writing and reads {@link CountingStreamOutput#position()} after.
     * Returns {@code 0} and logs a warning if serialisation raises an unexpected exception;
     * in that case {@code counter} is reset so the next call starts from a clean state.
     * <p>
     * Callers must multiply the result by {@link #SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR} to convert
     * wire bytes to a heap estimate. Returning {@code 0} on failure means the component is uncharged,
     * so the breaker may under-protect for that specific field when serialisation fails.
     */
    private static long tryCountSerializedBytes(CountingStreamOutput counter, Writeable writeable, String logMessage) {
        try {
            counter.reset();
            writeable.writeTo(counter);
            return counter.position();
        } catch (Exception e) {
            counter.reset();
            logger.warn(logMessage, e);
            return 0L;
        }
    }

    /**
     * Estimates the heap cost of a single {@link SearchHit}, including its stored source bytes,
     * doc-value / stored field entry shells and their values, sort values, highlight fragments,
     * explanations, and any nested {@code inner_hits} (recursive).
     * Uses {@link SearchHit#getDocumentFields()} and {@link SearchHit#getMetadataFields()} rather than
     * {@link SearchHit#getFields()}, which allocates a new {@code HashMap} on every call.
     */
    private static long estimateHitBytes(SearchHit hit, CountingStreamOutput counter) {
        long bytes = PER_HIT_OBJECT_OVERHEAD;
        bytes += hit.rawSourceLength();
        bytes += (long) (hit.getDocumentFields().size() + hit.getMetadataFields().size()) * PER_FIELD_OVERHEAD;
        for (DocumentField field : hit.getDocumentFields().values()) {
            for (Object value : field.getValues()) {
                bytes += estimateValueBytes(value);
            }
        }
        for (DocumentField field : hit.getMetadataFields().values()) {
            for (Object value : field.getValues()) {
                bytes += estimateValueBytes(value);
            }
        }
        Object[] sortVals = hit.getSortValues();
        if (sortVals.length > 0) {
            // SearchSortValues shell (~32 B) + two Object[] headers (16 B each) + ref slots (8 B per entry).
            // Both the formatted and raw arrays are iterated below; when DocValueFormat.RAW is used the two
            // arrays hold the same objects, so value bytes are charged twice — a known over-estimate.
            bytes += 64L + (long) sortVals.length * 8;
            for (Object sv : sortVals) {
                bytes += estimateValueBytes(sv);
            }
            for (Object sv : hit.getRawSortValues()) {
                bytes += estimateValueBytes(sv);
            }
        }
        Map<String, HighlightField> highlights = hit.getHighlightFields();
        if (highlights.isEmpty() == false) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                out -> out.writeCollection(highlights.values()),
                "msearch circuit breaker: failed to estimate highlight bytes"
            );
        }
        if (hit.getExplanation() != null) {
            bytes += SERIALISED_BYTES_HEAP_OVERHEAD_FACTOR * tryCountSerializedBytes(
                counter,
                out -> writeExplanation(out, hit.getExplanation()),
                "msearch circuit breaker: failed to estimate explanation bytes"
            );
        }
        Map<String, SearchHits> innerHits = hit.getInnerHits();
        if (innerHits != null) {
            for (SearchHits innerHitsValue : innerHits.values()) {
                for (SearchHit innerHit : innerHitsValue.getHits()) {
                    bytes += estimateHitBytes(innerHit, counter);
                }
            }
        }
        return bytes;
    }

    /**
     * Estimates the heap cost of a single value stored in a {@link DocumentField}.
     * Uses type-based sizing rather than serialisation to avoid encoding overhead on
     * the response path.
     * <ul>
     *   <li>{@code String}: 32 B object shell + 1 B per char (accurate for Latin-1 compact strings;
     *       under-counts non-Latin-1 characters which cost 2 B each)</li>
     *   <li>{@code byte[]}: 16 B array header + the byte count</li>
     *   <li>{@link Text}: 32 B object shell (header + 2 refs + 2 ints); if the string form is cached,
     *       adds a {@code String} shell (32 B) plus char count; otherwise adds the underlying
     *       {@link org.elasticsearch.common.bytes.BytesReference} byte count</li>
     *   <li>{@code Number} ({@code Long}, {@code Double}, {@code Integer}, etc.): 24 B
     *       (over-counts small types like {@code Integer} at 16 B; {@code Long}/{@code Double} are 24 B)</li>
     *   <li>Other types ({@code Boolean}, {@code GeoPoint}, {@code ZonedDateTime}, {@code Map}, etc.):
     *       32 B conservative lower bound; complex objects may be significantly larger</li>
     * </ul>
     */
    static long estimateValueBytes(Object value) {
        if (value instanceof String s) {
            return 32L + s.length();
        }
        if (value instanceof byte[] b) {
            return 16L + b.length;
        }
        if (value instanceof Text t) {
            // 32 B for the Text object shell (header + bytes ref + string ref + 2 ints, padded);
            // then size the available form without forcing materialisation.
            return t.hasString() ? 32L + 32L + t.string().length() : 32L + t.bytes().length();
        }
        if (value instanceof Number) {
            // Long/Double pad to 24 B; use 24 B as a conservative upper bound for all numeric types.
            return 24L;
        }
        // Boolean singletons, GeoPoint, ZonedDateTime, Map, List, and other reference types:
        // 32 B is a conservative lower bound; complex objects may be significantly larger.
        return 32L;
    }

    /**
     * Executes a single request from the queue of requests. When a request finishes, another request is taken from the queue. When a
     * request is executed, a permit is taken on the specified semaphore, and released as each request completes.
     *
     * @param requests the queue of multi-search requests to execute
     * @param responses atomic array to hold the responses corresponding to each search request slot
     * @param responseCounter incremented on each response
     * @param listener the listener attached to the multi-search request
     */
    void executeSearch(
        final Queue<SearchRequestSlot> requests,
        final AtomicArray<MultiSearchResponse.Item> responses,
        final AtomicInteger responseCounter,
        final ActionListener<MultiSearchResponse> listener,
        final long relativeStartTime,
        final MultiSearchBreakerAccounting breakerAccounting
    ) {
        /*
         * The number of times that we poll an item from the queue here is the minimum of the number of requests and the maximum number
         * of concurrent requests. At first glance, it appears that we should never poll from the queue and not obtain a request given
         * that we only poll here no more times than the number of requests. However, this is not the only consumer of this queue as
         * earlier requests that have already completed will poll from the queue too, and they could complete before later polls are
         * invoked here. Thus, it can be the case that we poll here and the queue was empty.
         */
        SearchRequestSlot request = requests.poll();
        // If we have another request to execute, we execute it. If the execution forked #doExecuteSearch will return false and will
        // recursively call this method again eventually. If it did not fork and was able to execute the search right away #doExecuteSearch
        // will return true, in which case we continue and run the next search request here.
        while (request != null
            && doExecuteSearch(requests, responses, responseCounter, relativeStartTime, request, listener, breakerAccounting)) {
            request = requests.poll();
        }
    }

    private boolean doExecuteSearch(
        Queue<SearchRequestSlot> requests,
        AtomicArray<MultiSearchResponse.Item> responses,
        AtomicInteger responseCounter,
        long relativeStartTime,
        SearchRequestSlot request,
        ActionListener<MultiSearchResponse> listener,
        MultiSearchBreakerAccounting breakerAccounting
    ) {
        final SubscribableListener<MultiSearchResponse.Item> subscribeListener = new SubscribableListener<>();
        // Use map (not safeMap) so unexpected exceptions from estimation route to onFailure and still decrement
        // responseCounter. CircuitBreakingException is caught below and returned as a failure item without throwing.
        client.search(request.request, subscribeListener.map(searchResponse -> {
            long queryPhaseAggHandoff = searchResponse.getQueryPhaseAggregationBreakerBytes();
            long bytes = 0;
            // bytesReserved: addEstimateBytesAndMaybeBreak succeeded — bytes are on the breaker.
            // addedToAccounting: breakerAccounting.add() was called — releaseAll() owns the release.
            // These two flags gate the outer-catch cleanup: if addedToAccounting is true, releaseAll()
            // covers everything; if false but bytesReserved is true, the outer catch must release bytes
            // directly (they are on the breaker but not tracked in breakerAccounting).
            boolean bytesReserved = false;
            boolean addedToAccounting = false;
            try {
                bytes = estimateActualBytes(searchResponse);
                try {
                    circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, "<msearch_response>");
                    bytesReserved = true;
                } catch (CircuitBreakingException e) {
                    if (queryPhaseAggHandoff > 0) {
                        circuitBreaker.addWithoutBreaking(-queryPhaseAggHandoff);
                    }
                    // No mustIncRef() yet — respondAndRelease on the search path will decRef the response.
                    return new MultiSearchResponse.Item(null, e);
                }
                breakerAccounting.add(bytes, queryPhaseAggHandoff);
                addedToAccounting = true;
                searchResponse.mustIncRef();
                return new MultiSearchResponse.Item(searchResponse, null);
            } catch (Exception unexpected) {
                if (addedToAccounting) {
                    // releaseAll() will cover both bytes and handoff via breakerAccounting.
                } else {
                    if (bytesReserved) {
                        circuitBreaker.addWithoutBreaking(-bytes);
                    }
                    if (queryPhaseAggHandoff > 0) {
                        circuitBreaker.addWithoutBreaking(-queryPhaseAggHandoff);
                    }
                }
                throw unexpected;
            }
        }));
        final ActionListener<MultiSearchResponse.Item> responseListener = new ActionListener<>() {
            @Override
            public void onResponse(final MultiSearchResponse.Item searchResponse) {
                handleResponse(request.responseSlot, searchResponse);
            }

            @Override
            public void onFailure(final Exception e) {
                if (ExceptionsHelper.status(e).getStatus() >= 500 && ExceptionsHelper.isNodeOrShardUnavailableTypeException(e) == false) {
                    logger.warn("TransportMultiSearchAction failure", e);
                }
                handleResponse(request.responseSlot, new MultiSearchResponse.Item(null, e));
            }

            private void handleResponse(final int responseSlot, final MultiSearchResponse.Item item) {
                responses.set(responseSlot, item);
                if (responseCounter.decrementAndGet() == 0) {
                    assert requests.isEmpty();
                    finish();
                }
            }

            private void finish() {
                ActionListener.respondAndRelease(
                    listener,
                    new MultiSearchResponse(responses.toArray(new MultiSearchResponse.Item[responses.length()]), buildTookInMillis())
                );
            }

            /**
             * Builds how long it took to execute the msearch.
             */
            private long buildTookInMillis() {
                return TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - relativeStartTime);
            }
        };
        if (subscribeListener.isDone()) {
            subscribeListener.addListener(responseListener);
            return true;
        }
        // we went forked and have to check if there's more searches to execute after we're done with this search
        subscribeListener.addListener(
            ActionListener.runAfter(
                responseListener,
                () -> executeSearch(requests, responses, responseCounter, listener, relativeStartTime, breakerAccounting)
            )
        );
        return false;
    }

    record SearchRequestSlot(SearchRequest request, int responseSlot) {

    }

    /**
     * Tracks REQUEST breaker bytes reserved while sub-search responses are buffered: incremental estimates from
     * {@link #estimateActualBytes} plus query-phase aggregation bytes handed off from {@link QueryPhaseResultConsumer}.
     */
    final class MultiSearchBreakerAccounting {
        private final AtomicLong incrementalBytes = new AtomicLong();
        private final AtomicLong queryPhaseAggregationHandoffBytes = new AtomicLong();

        void add(long incremental, long queryPhaseAggregationHandoff) {
            incrementalBytes.addAndGet(incremental);
            queryPhaseAggregationHandoffBytes.addAndGet(queryPhaseAggregationHandoff);
        }

        void releaseAll() {
            long release = incrementalBytes.get() + queryPhaseAggregationHandoffBytes.get();
            if (release > 0) {
                circuitBreaker.addWithoutBreaking(-release);
            }
        }
    }
}
