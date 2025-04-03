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

import static org.elasticsearch.index.query.RankDocsQueryBuilder.DEFAULT_MIN_SCORE;

/**
 * An {@link RetrieverBuilder} that is used to retrieve documents based on the rank of the documents.
 */
public class RankDocsRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "rank_docs_retriever";
    final int rankWindowSize;
    final List<RetrieverBuilder> sources;
    final Supplier<RankDoc[]> rankDocs;

    public RankDocsRetrieverBuilder(int rankWindowSize, List<RetrieverBuilder> sources, Supplier<RankDoc[]> rankDocs) {
        this.rankWindowSize = rankWindowSize;
        this.rankDocs = rankDocs;
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("sources must not be null or empty");
        }
        this.sources = sources;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private boolean sourceHasMinScore() {
        return minScore != null || sources.stream().anyMatch(x -> x.minScore() != null);
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
            true,
            this.minScore() != null ? this.minScore() : DEFAULT_MIN_SCORE
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
        float effectiveMinScore = getEffectiveMinScore();

        System.out.println(
            "DEBUG: RankDocsRetrieverBuilder - extractToSearchSourceBuilder with "
                + (rankDocResults != null ? rankDocResults.length : 0)
                + " rank results"
        );
        System.out.println("DEBUG: RankDocsRetrieverBuilder - minScore=" + minScore() + ", effective minScore=" + effectiveMinScore);

        if (hasAggregations(searchSourceBuilder)
            || isExplainRequest(searchSourceBuilder)
            || isProfileRequest(searchSourceBuilder)
            || shouldTrackTotalHits(searchSourceBuilder)) {
            System.out.println(
                "DEBUG: RankDocsRetrieverBuilder - Building with explainQuery="
                    + isExplainRequest(searchSourceBuilder)
                    + ", hasAggs="
                    + hasAggregations(searchSourceBuilder)
                    + ", isProfile="
                    + isProfileRequest(searchSourceBuilder)
                    + ", shouldTrackTotalHits="
                    + shouldTrackTotalHits(searchSourceBuilder)
            );

            if (false == isExplainRequest(searchSourceBuilder)) {
                rankQuery = new RankDocsQueryBuilder(
                    rankDocResults,
                    sources.stream().map(RetrieverBuilder::topDocsQuery).toArray(QueryBuilder[]::new),
                    false,
                    effectiveMinScore
                );
            } else {
                rankQuery = new RankDocsQueryBuilder(
                    rankDocResults,
                    sources.stream().map(RetrieverBuilder::explainQuery).toArray(QueryBuilder[]::new),
                    false,
                    effectiveMinScore
                );
            }
        } else {
            System.out.println("DEBUG: RankDocsRetrieverBuilder - Building with simplified query");
            rankQuery = new RankDocsQueryBuilder(rankDocResults, null, false, effectiveMinScore);
        }

        System.out.println("DEBUG: RankDocsRetrieverBuilder - Created rankQuery with minScore=" + effectiveMinScore);
        rankQuery.queryName(retrieverName());
        // ignore prefilters of this level, they were already propagated to children
        searchSourceBuilder.query(rankQuery);
        if (searchSourceBuilder.size() < 0) {
            searchSourceBuilder.size(rankWindowSize);
        }

        // Set track total hits to equal the number of results, ensuring the correct count is returned
        boolean emptyResults = rankDocResults.length == 0;
        boolean shouldTrack = shouldTrackTotalHits(searchSourceBuilder);

        if (shouldTrack) {
            int hitsToTrack = emptyResults ? Integer.MAX_VALUE : rankDocResults.length;
            System.out.println("DEBUG: RankDocsRetrieverBuilder - Setting trackTotalHitsUpTo to " + hitsToTrack);
            searchSourceBuilder.trackTotalHitsUpTo(hitsToTrack);
        }

        // Always set minScore if it's meaningful (greater than default)
        boolean hasSignificantMinScore = effectiveMinScore > DEFAULT_MIN_SCORE;

        System.out.println(
            "DEBUG: RankDocsRetrieverBuilder - sourceHasMinScore="
                + sourceHasMinScore()
                + ", effectiveMinScore="
                + effectiveMinScore
                + ", hasSignificantMinScore="
                + hasSignificantMinScore
        );

        if (hasSignificantMinScore) {
            // Set minScore on the search source builder - this ensures filtering happens
            searchSourceBuilder.minScore(effectiveMinScore);
        }

        if (searchSourceBuilder.size() + searchSourceBuilder.from() > rankDocResults.length) {
            searchSourceBuilder.size(Math.max(0, rankDocResults.length - searchSourceBuilder.from()));
        }

        System.out.println(
            "DEBUG: RankDocsRetrieverBuilder - Final searchSourceBuilder: "
                + "size="
                + searchSourceBuilder.size()
                + ", minScore="
                + searchSourceBuilder.minScore()
                + ", trackTotalHitsUpTo="
                + searchSourceBuilder.trackTotalHitsUpTo()
        );
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
        // Always track total hits if minScore is being used, since we need to maintain the filtered count
        if (minScore() != null && minScore() > DEFAULT_MIN_SCORE) {
            return true;
        }

        // Check sources for minScore - if any have a significant minScore, we need to track hits
        for (RetrieverBuilder source : sources) {
            Float sourceMinScore = source.minScore();
            if (sourceMinScore != null && sourceMinScore > DEFAULT_MIN_SCORE) {
                return true;
            }
        }

        // Otherwise use default behavior
        return searchSourceBuilder.trackTotalHitsUpTo() == null
            || (rankDocs.get() != null && searchSourceBuilder.trackTotalHitsUpTo() > rankDocs.get().length);
    }

    /**
     * Gets the effective minimum score, either from this builder or from one of its sources.
     * If no minimum score is set, returns the default minimum score.
     */
    private float getEffectiveMinScore() {
        System.out.println("DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - this.minScore=" + minScore);

        if (minScore != null) {
            System.out.println("DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - using this.minScore=" + minScore);
            return minScore;
        }

        // Check if any of the sources have a minScore
        System.out.println("DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - checking " + sources.size() + " sources");
        for (RetrieverBuilder source : sources) {
            Float sourceMinScore = source.minScore();
            System.out.println(
                "DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - source minScore="
                    + sourceMinScore
                    + " for source "
                    + source.getClass().getSimpleName()
            );

            if (sourceMinScore != null && sourceMinScore > DEFAULT_MIN_SCORE) {
                System.out.println("DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - using source minScore=" + sourceMinScore);
                return sourceMinScore;
            }
        }

        System.out.println("DEBUG: RankDocsRetrieverBuilder.getEffectiveMinScore() - using DEFAULT_MIN_SCORE=" + DEFAULT_MIN_SCORE);
        return DEFAULT_MIN_SCORE;
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
