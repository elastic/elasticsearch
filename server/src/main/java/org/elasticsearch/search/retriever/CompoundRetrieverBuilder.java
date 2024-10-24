/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * This abstract retriever defines a compound retriever. The idea is that it is not a leaf-retriever, i.e. it does not
 * perform actual searches itself. Instead, it is a container for a set of child retrievers and is responsible for combining
 * the results of the child retrievers according to the implementation of {@code combineQueryPhaseResults}.
 */
public abstract class CompoundRetrieverBuilder<T extends CompoundRetrieverBuilder<T>> extends RetrieverBuilder {

    public record RetrieverSource(RetrieverBuilder retriever, SearchSourceBuilder source) {}

    protected final int rankWindowSize;
    protected final List<RetrieverSource> innerRetrievers;

    protected CompoundRetrieverBuilder(List<RetrieverSource> innerRetrievers, int rankWindowSize) {
        this.rankWindowSize = rankWindowSize;
        this.innerRetrievers = innerRetrievers;
    }

    @SuppressWarnings("unchecked")
    public T addChild(RetrieverBuilder retrieverBuilder) {
        innerRetrievers.add(new RetrieverSource(retrieverBuilder, null));
        return (T) this;
    }

    /**
     * Returns a clone of the original retriever, replacing the sub-retrievers with
     * the provided {@code newChildRetrievers}.
     */
    protected abstract T clone(List<RetrieverSource> newChildRetrievers);

    /**
     * Combines the provided {@code rankResults} to return the final top documents.
     */
    protected abstract RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults);

    @Override
    public final boolean isCompound() {
        return true;
    }

    @Override
    public final RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.getPointInTimeBuilder() == null) {
            throw new IllegalStateException("PIT is required");
        }

        // Rewrite prefilters
        boolean hasChanged = false;
        var newPreFilters = rewritePreFilters(ctx);
        hasChanged |= newPreFilters != preFilterQueryBuilders;

        // Rewrite retriever sources
        List<RetrieverSource> newRetrievers = new ArrayList<>();
        for (var entry : innerRetrievers) {
            RetrieverBuilder newRetriever = entry.retriever.rewrite(ctx);
            if (newRetriever != entry.retriever) {
                newRetrievers.add(new RetrieverSource(newRetriever, null));
                hasChanged |= true;
            } else {
                var sourceBuilder = entry.source != null
                    ? entry.source
                    : createSearchSourceBuilder(ctx.getPointInTimeBuilder(), newRetriever);
                var rewrittenSource = sourceBuilder.rewrite(ctx);
                newRetrievers.add(new RetrieverSource(newRetriever, rewrittenSource));
                hasChanged |= rewrittenSource != entry.source;
            }
        }
        if (hasChanged) {
            return clone(newRetrievers);
        }

        // execute searches
        final SetOnce<RankDoc[]> results = new SetOnce<>();
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (var entry : innerRetrievers) {
            SearchRequest searchRequest = new SearchRequest().source(entry.source);
            // The can match phase can reorder shards, so we disable it to ensure the stable ordering
            searchRequest.setPreFilterShardSize(Integer.MAX_VALUE);
            multiSearchRequest.add(searchRequest);
        }
        ctx.registerAsyncAction((client, listener) -> {
            client.execute(TransportMultiSearchAction.TYPE, multiSearchRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiSearchResponse items) {
                    List<ScoreDoc[]> topDocs = new ArrayList<>();
                    List<Exception> failures = new ArrayList<>();
                    // capture the max status code returned by any of the responses
                    int statusCode = RestStatus.OK.getStatus();
                    List<String> retrieversWithFailures = new ArrayList<>();
                    for (int i = 0; i < items.getResponses().length; i++) {
                        var item = items.getResponses()[i];
                        if (item.isFailure()) {
                            failures.add(item.getFailure());
                            retrieversWithFailures.add(innerRetrievers.get(i).retriever().getName());
                            if (ExceptionsHelper.status(item.getFailure()).getStatus() > statusCode) {
                                statusCode = ExceptionsHelper.status(item.getFailure()).getStatus();
                            }
                        } else {
                            assert item.getResponse() != null;
                            var rankDocs = getRankDocs(item.getResponse());
                            innerRetrievers.get(i).retriever().setRankDocs(rankDocs);
                            topDocs.add(rankDocs);
                        }
                    }
                    if (false == failures.isEmpty()) {
                        assert statusCode != RestStatus.OK.getStatus();
                        final String errMessage = "["
                            + getName()
                            + "] search failed - retrievers '"
                            + retrieversWithFailures
                            + "' returned errors. "
                            + "All failures are attached as suppressed exceptions.";
                        Exception ex = new ElasticsearchStatusException(errMessage, RestStatus.fromCode(statusCode));
                        failures.forEach(ex::addSuppressed);
                        listener.onFailure(ex);
                    } else {
                        results.set(combineInnerRetrieverResults(topDocs));
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        });

        return new RankDocsRetrieverBuilder(
            rankWindowSize,
            newRetrievers.stream().map(s -> s.retriever).toList(),
            results::get,
            newPreFilters
        );
    }

    @Override
    public final QueryBuilder topDocsQuery() {
        throw new IllegalStateException("Should not be called, missing a rewrite?");
    }

    @Override
    public final QueryBuilder explainQuery() {
        throw new IllegalStateException("Should not be called, missing a rewrite?");
    }

    @Override
    public final void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        throw new IllegalStateException("Should not be called, missing a rewrite?");
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        validationException = super.validate(source, validationException, isScroll, allowPartialSearchResults);
        if (source.size() > rankWindowSize) {
            validationException = addValidationError(
                "["
                    + this.getName()
                    + "] requires [rank_window_size: "
                    + rankWindowSize
                    + "]"
                    + " be greater than or equal to [size: "
                    + source.size()
                    + "]",
                validationException
            );
        }
        if (allowPartialSearchResults) {
            validationException = addValidationError(
                "cannot specify [" + getName() + "] and [allow_partial_search_results]",
                validationException
            );
        }
        if (isScroll) {
            validationException = addValidationError("cannot specify [" + getName() + "] and [scroll]", validationException);
        }
        for (RetrieverSource innerRetriever : innerRetrievers) {
            validationException = innerRetriever.retriever().validate(source, validationException, isScroll, allowPartialSearchResults);
        }
        return validationException;
    }

    @Override
    public boolean doEquals(Object o) {
        CompoundRetrieverBuilder<?> that = (CompoundRetrieverBuilder<?>) o;
        return rankWindowSize == that.rankWindowSize && Objects.equals(innerRetrievers, that.innerRetrievers);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(innerRetrievers);
    }

    protected SearchSourceBuilder createSearchSourceBuilder(PointInTimeBuilder pit, RetrieverBuilder retrieverBuilder) {
        var sourceBuilder = new SearchSourceBuilder().pointInTimeBuilder(pit)
            .trackTotalHits(false)
            .storedFields(new StoredFieldsContext(false))
            .size(rankWindowSize);
        // apply the pre-filters downstream once
        if (preFilterQueryBuilders.isEmpty() == false) {
            retrieverBuilder.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
        }
        retrieverBuilder.extractToSearchSourceBuilder(sourceBuilder, true);

        // Record the shard id in the sort result
        List<SortBuilder<?>> sortBuilders = sourceBuilder.sorts() != null ? new ArrayList<>(sourceBuilder.sorts()) : new ArrayList<>();
        if (sortBuilders.isEmpty()) {
            sortBuilders.add(new ScoreSortBuilder());
        }
        sortBuilders.add(new FieldSortBuilder(FieldSortBuilder.SHARD_DOC_FIELD_NAME));
        sourceBuilder.sort(sortBuilders);
        return sourceBuilder;
    }

    private RankDoc[] getRankDocs(SearchResponse searchResponse) {
        int size = searchResponse.getHits().getHits().length;
        RankDoc[] docs = new RankDoc[size];
        for (int i = 0; i < size; i++) {
            var hit = searchResponse.getHits().getAt(i);
            long sortValue = (long) hit.getRawSortValues()[hit.getRawSortValues().length - 1];
            int doc = ShardDocSortField.decodeDoc(sortValue);
            int shardRequestIndex = ShardDocSortField.decodeShardRequestIndex(sortValue);
            docs[i] = new RankDoc(doc, hit.getScore(), shardRequestIndex);
            docs[i].rank = i + 1;
        }
        return docs;
    }
}
