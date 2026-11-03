/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * This is an internal action, that executes msearch requests for enrich indices in a more efficient manner.
 * Currently each search request inside a msearch request is executed as a separate search. If many search requests
 * are targeted to the same shards then there is quite some overhead in executing each search request as a separate
 * search (multiple search contexts, opening of multiple searchers).
 *
 * In case for the enrich processor, searches are always targeting the same single shard indices. This action
 * handles multi search requests targeting enrich indices more efficiently by executing them in a bulk using the same
 * searcher and query shard context.
 *
 * This action (plus some coordination logic in {@link EnrichCoordinatorProxyAction}) can be removed when msearch can
 * execute search requests targeted to the same shard more efficiently in a bulk like style.
 *
 * Note that this 'msearch' implementation only supports executing a query, pagination and source filtering.
 * Other search features are not supported, because the enrich processor isn't using these search features.
 */
public class EnrichShardMultiSearchAction extends ActionType<MultiSearchResponse> {

    public static final EnrichShardMultiSearchAction INSTANCE = new EnrichShardMultiSearchAction();
    private static final String NAME = "indices:data/read/shard_multi_search";

    /** Breaker label for the hit sources held in the response; maps to {@link ChildMemoryCircuitBreaker#CATEGORY_MSEARCH}. */
    static final String BREAKER_LABEL = ChildMemoryCircuitBreaker.CATEGORY_MSEARCH + "[enrich]";

    /** Hit source bytes that may build up before they are charged, so that small lookups make few breaker calls. */
    static final long BREAKER_CHARGE_THRESHOLD_BYTES = ByteSizeValue.ofMb(1).getBytes();

    private EnrichShardMultiSearchAction() {
        super(NAME);
    }

    public static class Request extends SingleShardRequest<Request> {

        private final MultiSearchRequest multiSearchRequest;

        public Request(MultiSearchRequest multiSearchRequest) {
            super(multiSearchRequest.requests().get(0).indices()[0]);
            this.multiSearchRequest = multiSearchRequest;
            assert multiSearchRequest.requests().stream().map(SearchRequest::indices).flatMap(Arrays::stream).distinct().count() == 1
                : "action [" + NAME + "] cannot handle msearch request pointing to multiple indices";
            assert assertSearchSource();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            multiSearchRequest = new MultiSearchRequest(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = validateNonNullIndex();
            if (index.startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE) == false) {
                validationException = ValidateActions.addValidationError(
                    "index [" + index + "] is not an enrich index",
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            multiSearchRequest.writeTo(out);
        }

        MultiSearchRequest getMultiSearchRequest() {
            return multiSearchRequest;
        }

        private boolean assertSearchSource() {
            for (SearchRequest request : multiSearchRequest.requests()) {
                SearchSourceBuilder copy = copy(request.source());

                // validate that only a from, size, query and source filtering has been provided (other features are not supported):
                // (first unset, what is supported and then see if there is anything left)
                copy.query(null);
                copy.from(0);
                copy.size(10);
                copy.fetchSource(null);
                assert EMPTY_SOURCE.equals(copy)
                    : "search request [" + Strings.toString(copy) + "] is using features that is not supported";
            }
            return true;
        }

        private static SearchSourceBuilder copy(SearchSourceBuilder source) {
            NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                source.writeTo(output);
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), registry)) {
                    return new SearchSourceBuilder(in);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static final SearchSourceBuilder EMPTY_SOURCE = new SearchSourceBuilder()
            // can't set -1 to indicate not specified
            .from(0)
            .size(10);
    }

    public static class TransportAction extends TransportSingleShardAction<Request, MultiSearchResponse> {

        private final IndicesService indicesService;
        private final CircuitBreaker circuitBreaker;

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService,
            CircuitBreakerService circuitBreakerService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                projectResolver,
                indexNameExpressionResolver,
                Request::new,
                threadPool.executor(ThreadPool.Names.SEARCH)
            );
            this.indicesService = indicesService;
            this.circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        }

        @Override
        protected Writeable.Reader<MultiSearchResponse> getResponseReader() {
            return MultiSearchResponse::new;
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ProjectState project, InternalRequest request) {
            String index = request.concreteIndex();
            IndexRoutingTable indexRouting = project.routingTable().index(index);
            int numShards = indexRouting.size();
            if (numShards != 1) {
                throw new IllegalStateException("index [" + index + "] should have 1 shard, but has " + numShards + " shards");
            }
            return clusterService.operationRouting()
                .searchShards(project, new String[] { index }, null, Preference.LOCAL.type())
                .getFirst();
        }

        /**
         * Builds the response while charging the hit sources to the request breaker, so that large lookups fail instead of exhausting
         * the heap. Every search response in it holds the charge until it is freed: after serialization for a remote caller, and once
         * the caller has consumed it for a local one.
         */
        @Override
        protected MultiSearchResponse shardOperation(Request request, ShardId shardId) throws IOException {
            final SourceBytesReservation reservation = new SourceBytesReservation(circuitBreaker);
            try {
                return buildResponse(request, shardId, reservation);
            } finally {
                // the search responses hold their own references
                reservation.decRef();
            }
        }

        private MultiSearchResponse buildResponse(Request request, ShardId shardId, SourceBytesReservation reservation) throws IOException {
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indicesService.getShardOrNull(shardId);
            try (Engine.Searcher searcher = indexShard.acquireSearcher("enrich_msearch")) {
                final ShardLookup lookup = new ShardLookup(
                    searcher,
                    newSearchExecutionContext(indexService, shardId, searcher),
                    searcher.storedFields(),
                    new FieldsVisitor(true),
                    reservation
                );
                final List<SearchRequest> searches = request.multiSearchRequest.requests();
                final MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[searches.size()];
                boolean success = false;
                try {
                    for (int i = 0; i < searches.size(); i++) {
                        SearchResponse response = createSearchResponse(searches.get(i).source(), lookup);
                        items[i] = new MultiSearchResponse.Item(response, null);
                    }
                    reservation.reserveRemaining();
                    success = true;
                    return new MultiSearchResponse(items, 1L);
                } finally {
                    if (success == false) {
                        for (MultiSearchResponse.Item item : items) {
                            if (item != null) {
                                item.getResponse().decRef();
                            }
                        }
                    }
                }
            }
        }
    }

    private static BytesReference filterSource(FetchSourceContext fetchSourceContext, BytesReference source) throws IOException {
        if (fetchSourceContext.includes().length == 0 && fetchSourceContext.excludes().length == 0) {
            return source;
        }

        Set<String> includes = Set.of(fetchSourceContext.includes());
        Set<String> excludes = Set.of(fetchSourceContext.excludes());

        XContentBuilder builder = new XContentBuilder(
            XContentType.SMILE.xContent(),
            new BytesStreamOutput(source.length()),
            includes,
            excludes,
            ParsedMediaType.parseMediaType(XContentType.SMILE, emptyMap())
        );
        XContentParser sourceParser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            source,
            XContentType.SMILE
        );
        builder.copyCurrentStructure(sourceParser);
        return BytesReference.bytes(builder);
    }

    private static SearchExecutionContext newSearchExecutionContext(IndexService indexService, ShardId shardId, Engine.Searcher searcher) {
        // enrich policies can't define runtime fields (support could be added later), so the context is built without any
        Map<String, Object> runtimeFields = emptyMap();
        return indexService.newSearchExecutionContext(
            shardId.id(),
            0,
            searcher,
            () -> { throw new UnsupportedOperationException(); },
            null,
            runtimeFields,
            null,
            null
        );
    }

    private static SearchResponse createSearchResponse(SearchSourceBuilder source, ShardLookup lookup) throws IOException {
        Query query = source.query().rewrite(lookup.context()).toQuery(lookup.context());
        TopDocs topDocs = lookup.searcher().search(query, source.from() + source.size(), new Sort(SortField.FIELD_DOC));
        SearchHit[] hits = loadHits(topDocs.scoreDocs, source.fetchSource(), lookup);
        SearchHits searchHits = new SearchHits(hits, topDocs.totalHits, 0);
        try {
            return new ChargedSearchResponse(searchHits, lookup.reservation());
        } finally {
            searchHits.decRef();
        }
    }

    /**
     * Loads and charges the source of every doc before creating any hits, so that a breaker trip leaves no hits to release.
     */
    private static SearchHit[] loadHits(ScoreDoc[] scoreDocs, FetchSourceContext fetchSourceContext, ShardLookup lookup)
        throws IOException {
        FieldsVisitor visitor = lookup.visitor();
        SearchExecutionContext context = lookup.context();
        String[] ids = new String[scoreDocs.length];
        BytesReference[] sources = new BytesReference[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            visitor.reset();
            lookup.storedFields().document(scoreDocs[i].doc, visitor);
            visitor.postProcess(field -> {
                if (context.isFieldMapped(field) == false) {
                    throw new IllegalStateException("Field [" + field + "] exists in the index but not in mappings");
                }
                return context.getFieldType(field);
            });
            ids[i] = visitor.id();
            sources[i] = filterSource(fetchSourceContext, visitor.source());
            lookup.reservation().add(sources[i].length());
        }

        SearchHit[] hits = new SearchHit[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            hits[i] = new SearchHit(scoreDocs[i].doc, ids[i]);
            hits[i].sourceRef(sources[i]);
        }
        return hits;
    }

    /** Created once per request, so that all of its searches reuse one stored fields reader and visitor. */
    private record ShardLookup(
        Engine.Searcher searcher,
        SearchExecutionContext context,
        StoredFields storedFields,
        FieldsVisitor visitor,
        SourceBytesReservation reservation
    ) {}

    /**
     * The hit source bytes of one response, charged to the breaker in steps of at least {@link #BREAKER_CHARGE_THRESHOLD_BYTES}.
     * The builder and every search response it creates hold a reference, so the charge is released when the last response is freed,
     * possibly on another thread. The counters need no synchronization: the building thread writes them before it hands the response
     * over, and the ref count orders that before the release.
     */
    static final class SourceBytesReservation extends AbstractRefCounted {
        private final CircuitBreaker circuitBreaker;
        private long loadedBytes;
        private long reservedBytes;

        SourceBytesReservation(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        /** Records that {@code bytes} more were loaded, and charges them once enough has built up. */
        void add(long bytes) {
            loadedBytes += bytes;
            if (loadedBytes - reservedBytes >= BREAKER_CHARGE_THRESHOLD_BYTES) {
                reserveRemaining();
            }
        }

        /** Charges everything loaded so far that is not charged yet. */
        void reserveRemaining() {
            final long unreserved = loadedBytes - reservedBytes;
            if (unreserved > 0) {
                circuitBreaker.addEstimateBytesAndMaybeBreak(unreserved, BREAKER_LABEL);
                reservedBytes += unreserved;
            }
        }

        @Override
        protected void closeInternal() {
            if (reservedBytes > 0) {
                circuitBreaker.addWithoutBreaking(-reservedBytes, BREAKER_LABEL);
            }
        }
    }

    /**
     * A search response that holds a reference on the reservation, so that its hits stay charged until the response is freed. The
     * reference is per item rather than on the multi-search response, because when a batch spans several enrich indices, the
     * coordinator frees the response of the index that answered first before the processors have used its items.
     */
    private static final class ChargedSearchResponse extends SearchResponse {
        private final SourceBytesReservation reservation;

        ChargedSearchResponse(SearchHits hits, SourceBytesReservation reservation) {
            super(hits, null, null, false, null, null, 0, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
            reservation.mustIncRef();
            this.reservation = reservation;
        }

        @Override
        public boolean decRef() {
            if (super.decRef()) {
                reservation.decRef();
                return true;
            }
            return false;
        }
    }
}
