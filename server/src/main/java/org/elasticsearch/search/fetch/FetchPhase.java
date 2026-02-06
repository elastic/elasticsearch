/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.chunk.FetchPhaseResponseChunk;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankDocShardInfo;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.index.get.ShardGetService.maybeExcludeVectorFields;
import static org.elasticsearch.index.get.ShardGetService.shouldExcludeInferenceFieldsFromSource;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all the matches returned by the query phase
 * Supports both traditional mode (all results in memory) and streaming mode (results sent in chunks).
 */
public final class FetchPhase {

    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    /**
     * Executes the fetch phase without memory checking or streaming.
     *
     * @param context the search context
     * @param docIdsToLoad document IDs to fetch
     * @param rankDocs ranking information
     */
    public void execute(SearchContext context, int[] docIdsToLoad, RankDocShardInfo rankDocs) {
        // Synchronous wrapper for backward compatibility,
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        execute(context, docIdsToLoad, rankDocs, null, null, null, future);
        try {
            future.actionGet();
        } catch (UncategorizedExecutionException e) {
            // PlainActionFuture wraps non-ElasticsearchException failures in UncategorizedExecutionException.
            // Translate to FetchPhaseExecutionException to preserve the expected exception type and cause.
            throw new FetchPhaseExecutionException(context.shardTarget(), "Fetch phase failed", e.getCause());
        }
    }

    /**
     * Executes the fetch phase with optional memory checking and no streaming
     *
     * @param context the search context
     * @param docIdsToLoad document IDs to fetch
     * @param rankDocs ranking information
     * @param memoryChecker optional callback for memory tracking, may be null
     */
    public void execute(SearchContext context, int[] docIdsToLoad, RankDocShardInfo rankDocs, @Nullable IntConsumer memoryChecker) {
        // Synchronous wrapper for backward compatibility,
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        execute(context, docIdsToLoad, rankDocs, memoryChecker, null, null, future);
        try {
            future.actionGet();
        } catch (UncategorizedExecutionException e) {
            // PlainActionFuture wraps non-ElasticsearchException failures in UncategorizedExecutionException.
            // Translate to FetchPhaseExecutionException to preserve the expected exception type and cause.
            throw new FetchPhaseExecutionException(context.shardTarget(), "Fetch phase failed", e.getCause());
        }
    }

    /**
     * Executes the fetch phase with optional memory checking and optional streaming.
     *
     * <p>When {@code writer} is {@code null} (non-streaming), all hits are accumulated in memory and returned at once.
     * When {@code writer} is provided (streaming), hits are emitted in chunks to reduce peak memory usage. In streaming mode,
     * the final completion may be delayed by transport-level acknowledgements, but the fetch build completion is signaled as
     * soon as the fetch work has finished.
     *
     * @param context the search context
     * @param docIdsToLoad document IDs to fetch
     * @param rankDocs ranking information
     * @param memoryChecker optional callback for memory tracking, may be {@code null}
     * @param writer optional chunk writer for streaming mode, may be {@code null}
     * @param buildListener optional listener invoked when the fetch build completes (success/failure). In streaming mode this
     *                      fires when hits are built and chunks are dispatched, without waiting for chunk ACKs.
     * @param listener final completion listener. In streaming mode this is invoked only after all chunks are ACKed; in
     *                 non-streaming mode it is invoked immediately after hits are built.
     *
     * @throws TaskCancelledException if the task is cancelled
     */
    public void execute(
        SearchContext context,
        int[] docIdsToLoad,
        RankDocShardInfo rankDocs,
        @Nullable IntConsumer memoryChecker,
        @Nullable FetchPhaseResponseChunk.Writer writer,
        @Nullable ActionListener<Void> buildListener,
        ActionListener<Void> listener
    ) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        if (docIdsToLoad == null || docIdsToLoad.length == 0) {
            // no individual hits to process, so we shortcut
            context.fetchResult()
                .shardResult(SearchHits.empty(context.queryResult().getTotalHits(), context.queryResult().getMaxScore()), null);
            if (buildListener != null) {
                buildListener.onResponse(null);
            }
            listener.onResponse(null);
            return;
        }

        final Profiler profiler = context.getProfilers() == null
            || (context.request().source() != null && context.request().source().rankBuilder() != null)
                ? Profiler.NOOP
                : Profilers.startProfilingFetchPhase();

        final AtomicReference<Throwable> sendFailure = new AtomicReference<>();

        // buildSearchHits produces SearchHits for non-streaming mode, or dispatches chunks for streaming mode.
        // - buildListener (if present) is notified when the fetch build completes (success/failure).
        // - listener is notified on final completion (after chunk ACKs in streaming mode)
        buildSearchHits(
            context,
            docIdsToLoad,
            profiler,
            rankDocs,
            memoryChecker,
            writer,
            sendFailure,
            buildListener,
            ActionListener.wrap(hitsAndBytes -> {
                // Transfer SearchHits ownership to shardResult
                SearchHits hitsToRelease = hitsAndBytes.hits;
                try {
                    ProfileResult profileResult = profiler.finish();
                    context.fetchResult().shardResult(hitsAndBytes.hits, profileResult);

                    if (writer == null) {
                        // Store circuit breaker bytes for later release after response is sent
                        context.fetchResult().setSearchHitsSizeBytes(hitsAndBytes.searchHitsBytesSize);
                    }

                    hitsToRelease = null; // Ownership transferred
                    listener.onResponse(null);
                } finally {
                    // Release if shardResult() threw an exception before taking ownership.
                    if (hitsToRelease != null) {
                        hitsToRelease.decRef();
                    }
                }
            }, listener::onFailure)
        );
    }

    private static class PreloadedSourceProvider implements SourceProvider {

        Source source;

        @Override
        public Source getSource(LeafReaderContext ctx, int doc) {
            return source;
        }
    }

    // Returning SearchHits async via ActionListener.
    private void buildSearchHits(
        SearchContext context,
        int[] docIdsToLoad,
        Profiler profiler,
        RankDocShardInfo rankDocs,
        IntConsumer memoryChecker,
        FetchPhaseResponseChunk.Writer writer,
        AtomicReference<Throwable> sendFailure,
        @Nullable ActionListener<Void> buildListener,
        ActionListener<SearchHitsWithSizeBytes> listener
    ) {
        var lookup = context.getSearchExecutionContext().getMappingLookup();

        // Optionally remove sparse and dense vector fields early to:
        // - Reduce the in-memory size of the source
        // - Speed up retrieval of the synthetic source
        // Note: These vectors will no longer be accessible via _source for any sub-fetch processors,
        // but they are typically accessed through doc values instead (e.g: re-scorer).
        var res = maybeExcludeVectorFields(
            context.getSearchExecutionContext().getMappingLookup(),
            context.getSearchExecutionContext().getIndexSettings(),
            context.fetchSourceContext(),
            context.fetchFieldsContext()
        );
        if (context.fetchSourceContext() != res.v1()) {
            context.fetchSourceContext(res.v1());
        }

        if (lookup.inferenceFields().isEmpty() == false && shouldExcludeInferenceFieldsFromSource(context.fetchSourceContext()) == false) {
            // Rehydrate the inference fields into the {@code _source} because they were explicitly requested.
            var oldFetchFieldsContext = context.fetchFieldsContext();
            var newFetchFieldsContext = new FetchFieldsContext(new ArrayList<>());
            if (oldFetchFieldsContext != null) {
                newFetchFieldsContext.fields().addAll(oldFetchFieldsContext.fields());
            }
            newFetchFieldsContext.fields().add(new FieldAndFormat(InferenceMetadataFieldsMapper.NAME, null));
            context.fetchFieldsContext(newFetchFieldsContext);
        }

        SourceLoader sourceLoader = context.newSourceLoader(res.v2());
        FetchContext fetchContext = new FetchContext(context, sourceLoader);

        PreloadedSourceProvider sourceProvider = new PreloadedSourceProvider();
        PreloadedFieldLookupProvider fieldLookupProvider = new PreloadedFieldLookupProvider();
        // The following relies on the fact that we fetch sequentially one segment after another, from a single thread
        // This needs to be revised once we add concurrency to the fetch phase, and needs a work-around for situations
        // where we run fetch as part of the query phase, where inter-segment concurrency is leveraged.
        // One problem is the global setLookupProviders call against the shared execution context.
        // Another problem is that the above provider implementations are not thread-safe
        context.getSearchExecutionContext().setLookupProviders(sourceProvider, ctx -> fieldLookupProvider);

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), fetchContext, profiler);
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.build(processors, FetchSubPhaseProcessor::storedFieldsSpec);
        storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(false, false, sourceLoader.requiredStoredFields()));
        // Ideally the required stored fields would be provided as constructor argument a few lines above, but that requires moving
        // the getProcessors call to before the setLookupProviders call, which causes weird issues in InnerHitsPhase.
        // setLookupProviders resets the SearchLookup used throughout the rest of the fetch phase, which StoredValueFetchers rely on
        // to retrieve stored fields, and InnerHitsPhase is the last sub-fetch phase and re-runs the entire fetch phase.
        fieldLookupProvider.setPreloadedStoredFieldNames(storedFieldsSpec.requiredStoredFields());

        StoredFieldLoader storedFieldLoader = profiler.storedFields(StoredFieldLoader.fromSpec(storedFieldsSpec));
        IdLoader idLoader = context.newIdLoader();
        boolean requiresSource = storedFieldsSpec.requiresSource();
        final int[] locallyAccumulatedBytes = new int[1];
        NestedDocuments nestedDocuments = context.getSearchExecutionContext().getNestedDocuments();

        FetchPhaseDocsIterator docsIterator = new FetchPhaseDocsIterator() {

            LeafReaderContext ctx;
            LeafNestedDocuments leafNestedDocuments;
            LeafStoredFieldLoader leafStoredFieldLoader;
            SourceLoader.Leaf leafSourceLoader;
            IdLoader.Leaf leafIdLoader;

            IntConsumer memChecker = memoryChecker != null ? memoryChecker : bytes -> {
                locallyAccumulatedBytes[0] += bytes;
                if (writer == null && context.checkCircuitBreaker(locallyAccumulatedBytes[0], "fetch source")) {
                    addRequestBreakerBytes(locallyAccumulatedBytes[0]);
                    locallyAccumulatedBytes[0] = 0;
                }
            };

            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException {
                Timer timer = profiler.startNextReader();
                try {
                    this.ctx = ctx;
                    this.leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(ctx);
                    this.leafStoredFieldLoader = storedFieldLoader.getLoader(ctx, docsInLeaf);
                    this.leafSourceLoader = sourceLoader.leaf(ctx.reader(), docsInLeaf);
                    this.leafIdLoader = idLoader.leaf(leafStoredFieldLoader, ctx.reader(), docsInLeaf);

                    fieldLookupProvider.setNextReader(ctx);
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(ctx);
                    }
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }
            }

            @Override
            protected SearchHit nextDoc(int doc) throws IOException {
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }

                HitContext hit = prepareHitContext(
                    context,
                    requiresSource,
                    profiler,
                    leafNestedDocuments,
                    leafStoredFieldLoader,
                    doc,
                    ctx,
                    leafSourceLoader,
                    leafIdLoader,
                    rankDocs == null ? null : rankDocs.get(doc)
                );

                boolean success = false;
                try {
                    sourceProvider.source = hit.source();
                    fieldLookupProvider.setPreloadedStoredFieldValues(hit.hit().getId(), hit.loadedFields());
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.process(hit);
                    }

                    if (writer == null) {
                        BytesReference sourceRef = hit.hit().getSourceRef();
                        if (sourceRef != null) {
                            // This is an empirical value that seems to work well.
                            // Deserializing a large source would also mean serializing it to HTTP response later on, so x2 seems reasonable
                            memChecker.accept(sourceRef.length() * 2);
                        }
                    }
                    success = true;
                    return hit.hit();
                } finally {
                    if (success == false) {
                        hit.hit().decRef();
                    }
                }
            }
        };

        if (writer == null) { // Non-streaming mode, synchronous iteration
            SearchHits resultToReturn = null;
            Exception caughtException = null;
            try (
                FetchPhaseDocsIterator.IterateResult result = docsIterator.iterate(
                    context.shardTarget(),
                    context.searcher().getIndexReader(),
                    docIdsToLoad,
                    context.request().allowPartialSearchResults(),
                    context.queryResult()
                )
            ) {
                if (context.isCancelled()) {
                    for (SearchHit hit : result.hits) {
                        if (hit != null) {
                            hit.decRef();
                        }
                    }
                    throw new TaskCancelledException("cancelled");
                }

                TotalHits totalHits = context.getTotalHits();
                resultToReturn = new SearchHits(result.hits, totalHits, context.getMaxScore());
                listener.onResponse(new SearchHitsWithSizeBytes(resultToReturn, docsIterator.getRequestBreakerBytes()));

                resultToReturn = null;
            } catch (Exception e) {
                caughtException = e;
                if (resultToReturn != null) {
                    resultToReturn.decRef();
                }
            } finally {
                if (buildListener != null) {
                    if (caughtException != null) {
                        buildListener.onFailure(caughtException);
                    } else {
                        buildListener.onResponse(null);
                    }
                }

                if (caughtException != null) {
                    listener.onFailure(caughtException);
                }
            }
        } else {  // Streaming mode
            final AtomicReference<ReleasableBytesReference> lastChunkBytesRef = new AtomicReference<>();
            final AtomicLong lastChunkHitCountRef = new AtomicLong(0);
            final AtomicLong lastChunkSequenceStartRef = new AtomicLong(-1);
            final AtomicLong lastChunkByteSizeRef = new AtomicLong(0);

            final int targetChunkBytes = FetchPhaseDocsIterator.DEFAULT_TARGET_CHUNK_BYTES;

            // RefCountingListener tracks chunk ACKs in streaming mode.
            // Each chunk calls acquire() to get a listener, which is completed when the ACK arrives
            // When all acquired listeners complete, the completion callback below runs
            // returning the final SearchHits (last chunk) to the caller
            final RefCountingListener chunkCompletionRefs = new RefCountingListener(listener.delegateFailureAndWrap((l, ignored) -> {
                ReleasableBytesReference lastChunkBytes = lastChunkBytesRef.getAndSet(null);
                try {
                    // Store sequence info in context
                    long seqStart = lastChunkSequenceStartRef.get();
                    if (seqStart >= 0) {
                        context.fetchResult().setLastChunkSequenceStart(seqStart);
                    }

                    // Deserialize and return last chunk as SearchHits
                    long lastSize = lastChunkByteSizeRef.getAndSet(0L);
                    long countLong = lastChunkHitCountRef.get();
                    if (lastChunkBytes != null && countLong > 0) {
                        int hitCount = Math.toIntExact(countLong);
                        context.fetchResult().setLastChunkBytes(lastChunkBytes, hitCount);
                        context.circuitBreaker().addWithoutBreaking(-lastSize);
                        lastChunkBytes = null;
                    }

                    l.onResponse(new SearchHitsWithSizeBytes(SearchHits.empty(context.getTotalHits(), context.getMaxScore()), 0));
                } finally {
                    Releasables.closeWhileHandlingException(lastChunkBytes);
                }
            }));

            // Acquire a listener for the main iteration. This prevents RefCountingListener from
            // completing until we explicitly signal success/failure after iteration finishes.
            final ActionListener<Void> mainBuildListener = chunkCompletionRefs.acquire();

            int maxInFlightChunks = SearchService.FETCH_PHASE_MAX_IN_FLIGHT_CHUNKS.get(
                context.getSearchExecutionContext().getIndexSettings().getSettings()
            );

            docsIterator.iterateAsync(
                context.shardTarget(),
                context.searcher().getIndexReader(),
                docIdsToLoad,
                writer,
                targetChunkBytes,
                chunkCompletionRefs,
                maxInFlightChunks,
                context.circuitBreaker(),
                sendFailure,
                context::isCancelled,
                new ActionListener<>() {
                    @Override
                    public void onResponse(FetchPhaseDocsIterator.IterateResult result) {
                        try (result) {
                            if (context.isCancelled()) {
                                throw new TaskCancelledException("cancelled");
                            }

                            // Take ownership of last chunk bytes
                            if (result.lastChunkBytes != null) {
                                lastChunkBytesRef.set(result.takeLastChunkBytes());
                                lastChunkHitCountRef.set(result.lastChunkHitCount);
                                lastChunkSequenceStartRef.set(result.lastChunkSequenceStart);
                                lastChunkByteSizeRef.set(result.lastChunkByteSize);
                            }

                            // Signal main build listener to decrement RefCountingListener
                            if (buildListener != null) {
                                buildListener.onResponse(null);
                            }

                            // Close RefCountingListener to release initial reference
                            mainBuildListener.onResponse(null);
                            chunkCompletionRefs.close();
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        ReleasableBytesReference lastChunkBytes = lastChunkBytesRef.getAndSet(null);
                        try {
                            Releasables.closeWhileHandlingException(lastChunkBytes);
                        } finally {
                            long bytesSize = lastChunkByteSizeRef.getAndSet(0);
                            if (bytesSize > 0) {
                                context.circuitBreaker().addWithoutBreaking(-bytesSize);
                            }
                        }

                        if (buildListener != null) {
                            buildListener.onFailure(e);
                        }

                        if (mainBuildListener != null) {
                            mainBuildListener.onFailure(e);
                        } else {
                            listener.onFailure(e);
                        }

                        if (chunkCompletionRefs != null) {
                            chunkCompletionRefs.close();
                        }
                    }
                }
            );
        }
    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, FetchContext context, Profiler profiler) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(profiler.profile(fsp.getClass().getSimpleName(), "", processor));
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
        }
    }

    private static HitContext prepareHitContext(
        SearchContext context,
        boolean requiresSource,
        Profiler profiler,
        LeafNestedDocuments nestedDocuments,
        LeafStoredFieldLoader leafStoredFieldLoader,
        int docId,
        LeafReaderContext subReaderContext,
        SourceLoader.Leaf sourceLoader,
        IdLoader.Leaf idLoader,
        RankDoc rankDoc
    ) throws IOException {
        if (nestedDocuments.advance(docId - subReaderContext.docBase) == null) {
            return prepareNonNestedHitContext(
                requiresSource,
                profiler,
                leafStoredFieldLoader,
                docId,
                subReaderContext,
                sourceLoader,
                idLoader,
                rankDoc
            );
        } else {
            return prepareNestedHitContext(
                context,
                requiresSource,
                profiler,
                docId,
                nestedDocuments,
                subReaderContext,
                leafStoredFieldLoader,
                rankDoc
            );
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link HitContext#source()}. This
     *     allows fetch subphases that use the hit context to access the preloaded source.
     */
    private static HitContext prepareNonNestedHitContext(
        boolean requiresSource,
        Profiler profiler,
        LeafStoredFieldLoader leafStoredFieldLoader,
        int docId,
        LeafReaderContext subReaderContext,
        SourceLoader.Leaf sourceLoader,
        IdLoader.Leaf idLoader,
        RankDoc rankDoc
    ) throws IOException {
        int subDocId = docId - subReaderContext.docBase;

        leafStoredFieldLoader.advanceTo(subDocId);

        String id = idLoader.getId(subDocId);
        if (id == null) {
            SearchHit hit = new SearchHit(docId);
            // TODO: can we use real pooled buffers here as well?
            Source source = Source.lazy(lazyStoredSourceLoader(profiler, subReaderContext, subDocId));
            return new HitContext(hit, subReaderContext, subDocId, Map.of(), source, rankDoc);
        } else {
            SearchHit hit = new SearchHit(docId, id);
            Source source;
            if (requiresSource) {
                Timer timer = profiler.startLoadingSource();
                try {
                    source = sourceLoader.source(leafStoredFieldLoader, subDocId);
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }
            } else {
                source = Source.lazy(lazyStoredSourceLoader(profiler, subReaderContext, subDocId));
            }
            return new HitContext(hit, subReaderContext, subDocId, leafStoredFieldLoader.storedFields(), source, rankDoc);
        }
    }

    private static Supplier<Source> lazyStoredSourceLoader(Profiler profiler, LeafReaderContext ctx, int doc) {
        return () -> {
            StoredFieldLoader rootLoader = profiler.storedFields(StoredFieldLoader.create(true, Collections.emptySet()));
            try {
                LeafStoredFieldLoader leafRootLoader = rootLoader.getLoader(ctx, null);
                leafRootLoader.advanceTo(doc);
                return Source.fromBytes(leafRootLoader.source());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * nested document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source, filtering it based on the nested document ID, then
     *     setting it on {@link HitContext#source()}. This allows fetch subphases that
     *     use the hit context to access the preloaded source.
     */
    private static HitContext prepareNestedHitContext(
        SearchContext context,
        boolean requiresSource,
        Profiler profiler,
        int topDocId,
        LeafNestedDocuments nestedInfo,
        LeafReaderContext subReaderContext,
        LeafStoredFieldLoader childFieldLoader,
        RankDoc rankDoc
    ) throws IOException {

        String rootId;
        Source rootSource = Source.empty(XContentType.JSON);

        if (context instanceof InnerHitsContext.InnerHitSubContext innerHitsContext) {
            rootId = innerHitsContext.getRootId();

            if (requiresSource) {
                rootSource = innerHitsContext.getRootLookup();
            }
        } else {
            StoredFieldLoader rootLoader = profiler.storedFields(StoredFieldLoader.create(requiresSource, Collections.emptySet()));
            LeafStoredFieldLoader leafRootLoader = rootLoader.getLoader(subReaderContext, null);
            leafRootLoader.advanceTo(nestedInfo.rootDoc());
            rootId = leafRootLoader.id();

            if (requiresSource) {
                if (leafRootLoader.source() != null) {
                    rootSource = Source.fromBytes(leafRootLoader.source());
                }
            }
        }

        childFieldLoader.advanceTo(nestedInfo.doc());

        SearchHit.NestedIdentity nestedIdentity = nestedInfo.nestedIdentity();
        assert nestedIdentity != null;
        Source nestedSource = nestedIdentity.extractSource(rootSource);

        SearchHit nestedHit = new SearchHit(topDocId, rootId, nestedIdentity);
        return new HitContext(nestedHit, subReaderContext, nestedInfo.doc(), childFieldLoader.storedFields(), nestedSource, rankDoc);
    }

    interface Profiler {
        ProfileResult finish();

        FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor processor);

        StoredFieldLoader storedFields(StoredFieldLoader storedFieldLoader);

        Timer startLoadingSource();

        Timer startNextReader();

        Profiler NOOP = new Profiler() {
            @Override
            public ProfileResult finish() {
                return null;
            }

            @Override
            public StoredFieldLoader storedFields(StoredFieldLoader storedFieldLoader) {
                return storedFieldLoader;
            }

            @Override
            public FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor processor) {
                return processor;
            }

            @Override
            public Timer startLoadingSource() {
                return null;
            }

            @Override
            public Timer startNextReader() {
                return null;
            }

            @Override
            public String toString() {
                return "noop";
            }
        };
    }

    private static class SearchHitsWithSizeBytes {
        final SearchHits hits;
        final long searchHitsBytesSize;

        SearchHitsWithSizeBytes(SearchHits hits, long searchHitsBytesSize) {
            this.hits = hits;
            this.searchHitsBytesSize = searchHitsBytesSize;
        }
    }
}
