/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An {@link RetrieverBuilder} that is used to retrieve documents based on the rank of the documents.
 */
public class RankDocsRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "rank_docs_retriever";
    final int rankWindowSize;
    final List<RetrieverBuilder> sources;
    final Supplier<RankDoc[]> rankDocs;

    public RankDocsRetrieverBuilder(int rankWindowSize, List<RetrieverBuilder> sources, Supplier<RankDoc[]> rankDocs, Float minScore) {
        this.rankWindowSize = rankWindowSize;
        this.rankDocs = rankDocs;
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("sources must not be null or empty");
        }
        this.sources = sources;
        this.minScore = minScore;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private boolean sourceHasMinScore() {
        return this.minScore != null || sources.stream().anyMatch(x -> x.minScore() != null);
    }

    private boolean sourceShouldRewrite(QueryRewriteContext ctx) throws IOException {
        for (var source : sources) {
            if (source.isCompound()) {
                return true;
            }
            var newSource = source.rewrite(ctx);
            if (newSource != source) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        assert false == sourceShouldRewrite(ctx) : "retriever sources should be rewritten first";
        return this;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        // this is used to fetch all documents form the parent retrievers (i.e. sources)
        // so that we can use all the matched documents to compute aggregations, nested hits etc
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (var retriever : sources) {
            var query = retriever.topDocsQuery();
            if (query != null) {
                if (retriever.retrieverName() != null) {
                    query.queryName(retriever.retrieverName());
                }
                boolQuery.should(query);
            }
        }
        // ignore prefilters of this level, they were already propagated to children
        return boolQuery;
    }

    @Override
    public QueryBuilder explainQuery() {
        var explainQuery = new RankDocsQueryBuilder(
            rankDocs.get(),
            sources.stream().map(RetrieverBuilder::explainQuery).toArray(QueryBuilder[]::new),
            true
        );
        explainQuery.queryName(retrieverName());
        return explainQuery;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        final RankDocsQueryBuilder rankQuery;
        // if we have aggregations we need to compute them based on all doc matches, not just the top hits
        // similarly, for profile and explain we re-run all parent queries to get all needed information
        RankDoc[] rankDocResults = rankDocs.get();
        if (hasAggregations(searchSourceBuilder)
            || isExplainRequest(searchSourceBuilder)
            || isProfileRequest(searchSourceBuilder)
            || shouldTrackTotalHits(searchSourceBuilder)) {
            if (false == isExplainRequest(searchSourceBuilder)) {
                rankQuery = new RankDocsQueryBuilder(
                    rankDocResults,
                    sources.stream().map(RetrieverBuilder::topDocsQuery).toArray(QueryBuilder[]::new),
                    false
                );
            } else {
                rankQuery = new RankDocsQueryBuilder(
                    rankDocResults,
                    sources.stream().map(RetrieverBuilder::explainQuery).toArray(QueryBuilder[]::new),
                    false
                );
            }
        } else {
            rankQuery = new RankDocsQueryBuilder(rankDocResults, null, false);
        }
        rankQuery.queryName(retrieverName());
        // ignore prefilters of this level, they were already propagated to children
        searchSourceBuilder.query(rankQuery);
        if (searchSourceBuilder.size() < 0) {
            searchSourceBuilder.size(rankWindowSize);
        }
        if (sourceHasMinScore()) {
            searchSourceBuilder.minScore(this.minScore == null ? Float.MIN_VALUE : this.minScore);
        }
        if (searchSourceBuilder.size() + searchSourceBuilder.from() > rankDocResults.length) {
            searchSourceBuilder.size(Math.max(0, rankDocResults.length - searchSourceBuilder.from()));
        }
    }

    private boolean hasAggregations(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.aggregations() != null;
    }

    private boolean isExplainRequest(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.explain() != null && searchSourceBuilder.explain();
    }

    private boolean isProfileRequest(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.profile();
    }

    private boolean shouldTrackTotalHits(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.trackTotalHitsUpTo() == null || searchSourceBuilder.trackTotalHitsUpTo() > rankDocs.get().length;
    }

    @Override
    protected boolean doEquals(Object o) {
        RankDocsRetrieverBuilder other = (RankDocsRetrieverBuilder) o;
        return rankWindowSize == other.rankWindowSize
            && Arrays.equals(rankDocs.get(), other.rankDocs.get())
            && sources.equals(other.sources);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.hashCode(), rankWindowSize, Arrays.hashCode(rankDocs.get()), sources);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("toXContent() is not supported for " + this.getClass());
    }
}
