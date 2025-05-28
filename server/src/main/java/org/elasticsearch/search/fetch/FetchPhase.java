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
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
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
import java.util.function.Supplier;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all of the matches returned by the query phase
 */
public final class FetchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    public void execute(SearchContext context, int[] docIdsToLoad, RankDocShardInfo rankDocs) {
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
            return;
        }

        Profiler profiler = context.getProfilers() == null
            || (context.request().source() != null && context.request().source().rankBuilder() != null)
                ? Profiler.NOOP
                : Profilers.startProfilingFetchPhase();
        SearchHits hits = null;
        try {
            hits = buildSearchHits(context, docIdsToLoad, profiler, rankDocs);
        } finally {
            try {
                // Always finish profiling
                ProfileResult profileResult = profiler.finish();
                // Only set the shardResults if building search hits was successful
                if (hits != null) {
                    context.fetchResult().shardResult(hits, profileResult);
                    hits = null;
                }
            } finally {
                if (hits != null) {
                    hits.decRef();
                }
            }
        }
    }

    private static class PreloadedSourceProvider implements SourceProvider {

        Source source;

        @Override
        public Source getSource(LeafReaderContext ctx, int doc) {
            return source;
        }
    }

    private SearchHits buildSearchHits(SearchContext context, int[] docIdsToLoad, Profiler profiler, RankDocShardInfo rankDocs) {
        SourceLoader sourceLoader = context.newSourceLoader(null);
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

        NestedDocuments nestedDocuments = context.getSearchExecutionContext().getNestedDocuments();

        FetchPhaseDocsIterator docsIterator = new FetchPhaseDocsIterator() {

            LeafReaderContext ctx;
            LeafNestedDocuments leafNestedDocuments;
            LeafStoredFieldLoader leafStoredFieldLoader;
            SourceLoader.Leaf leafSourceLoader;
            IdLoader.Leaf leafIdLoader;

            @Override
            protected void setNextReader(LeafReaderContext ctx, int[] docsInLeaf) throws IOException {
                Timer timer = profiler.startNextReader();
                this.ctx = ctx;
                this.leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(ctx);
                this.leafStoredFieldLoader = storedFieldLoader.getLoader(ctx, docsInLeaf);
                this.leafSourceLoader = sourceLoader.leaf(ctx.reader(), docsInLeaf);
                this.leafIdLoader = idLoader.leaf(leafStoredFieldLoader, ctx.reader(), docsInLeaf);
                fieldLookupProvider.setNextReader(ctx);
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.setNextReader(ctx);
                }
                if (timer != null) {
                    timer.stop();
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
                    success = true;
                    return hit.hit();
                } finally {
                    if (success == false) {
                        hit.hit().decRef();
                    }
                }
            }
        };

        SearchHit[] hits = docsIterator.iterate(
            context.shardTarget(),
            context.searcher().getIndexReader(),
            docIdsToLoad,
            context.request().allowPartialSearchResults(),
            context.queryResult()
        );

        if (context.isCancelled()) {
            for (SearchHit hit : hits) {
                // release all hits that would otherwise become owned and eventually released by SearchHits below
                hit.decRef();
            }
            throw new TaskCancelledException("cancelled");
        }

        TotalHits totalHits = context.getTotalHits();
        return new SearchHits(hits, totalHits, context.getMaxScore());
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

        SearchHit hit = new SearchHit(topDocId, rootId, nestedIdentity);
        return new HitContext(hit, subReaderContext, nestedInfo.doc(), childFieldLoader.storedFields(), nestedSource, rankDoc);
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
}
