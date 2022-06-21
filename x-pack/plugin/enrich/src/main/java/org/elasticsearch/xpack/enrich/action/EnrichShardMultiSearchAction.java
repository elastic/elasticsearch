/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.InternalSearchResponse;
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

    private EnrichShardMultiSearchAction() {
        super(NAME, MultiSearchResponse::new);
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

        private SearchSourceBuilder copy(SearchSourceBuilder source) {
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

        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                Request::new,
                ThreadPool.Names.SEARCH
            );
            this.indicesService = indicesService;
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
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            String index = request.concreteIndex();
            IndexRoutingTable indexRouting = state.routingTable().index(index);
            int numShards = indexRouting.size();
            if (numShards != 1) {
                throw new IllegalStateException("index [" + index + "] should have 1 shard, but has " + numShards + " shards");
            }

            GroupShardsIterator<ShardIterator> result = clusterService.operationRouting()
                .searchShards(state, new String[] { index }, null, Preference.LOCAL.type());
            return result.get(0);
        }

        @Override
        protected MultiSearchResponse shardOperation(Request request, ShardId shardId) throws IOException {
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indicesService.getShardOrNull(shardId);
            try (Engine.Searcher searcher = indexShard.acquireSearcher("enrich_msearch")) {
                final FieldsVisitor visitor = new FieldsVisitor(true);
                /*
                 * Enrich doesn't support defining runtime fields in its
                 * configuration. We could add support for that if we'd
                 * like it but, for now at least, you can't configure any
                 * runtime fields so it is safe to build the context without
                 * any.
                 */
                Map<String, Object> runtimeFields = emptyMap();
                final SearchExecutionContext context = indexService.newSearchExecutionContext(
                    shardId.id(),
                    0,
                    searcher,
                    () -> { throw new UnsupportedOperationException(); },
                    null,
                    runtimeFields
                );
                final MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.multiSearchRequest.requests().size()];
                for (int i = 0; i < request.multiSearchRequest.requests().size(); i++) {
                    final SearchSourceBuilder searchSourceBuilder = request.multiSearchRequest.requests().get(i).source();

                    final QueryBuilder queryBuilder = searchSourceBuilder.query();
                    final int from = searchSourceBuilder.from();
                    final int size = searchSourceBuilder.size();
                    final FetchSourceContext fetchSourceContext = searchSourceBuilder.fetchSource();

                    final Query luceneQuery = queryBuilder.rewrite(context).toQuery(context);
                    final int n = from + size;
                    final TopDocs topDocs = searcher.search(luceneQuery, n, new Sort(SortField.FIELD_DOC));

                    final SearchHit[] hits = new SearchHit[topDocs.scoreDocs.length];
                    for (int j = 0; j < topDocs.scoreDocs.length; j++) {
                        final ScoreDoc scoreDoc = topDocs.scoreDocs[j];

                        visitor.reset();
                        searcher.doc(scoreDoc.doc, visitor);
                        visitor.postProcess(field -> {
                            if (context.isFieldMapped(field) == false) {
                                throw new IllegalStateException("Field [" + field + "] exists in the index but not in mappings");
                            }
                            return context.getFieldType(field);
                        });
                        final SearchHit hit = new SearchHit(scoreDoc.doc, visitor.id(), Map.of(), Map.of());
                        hit.sourceRef(filterSource(fetchSourceContext, visitor.source()));
                        hits[j] = hit;
                    }
                    items[i] = new MultiSearchResponse.Item(createSearchResponse(topDocs, hits), null);
                }
                return new MultiSearchResponse(items, 1L);
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

    private static SearchResponse createSearchResponse(TopDocs topDocs, SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, topDocs.totalHits, 0);
        return new SearchResponse(
            new InternalSearchResponse(searchHits, null, null, null, false, null, 0),
            null,
            1,
            1,
            0,
            1L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

}
