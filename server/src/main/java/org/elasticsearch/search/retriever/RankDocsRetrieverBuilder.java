/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.search.retriever.rankdoc.RankDocsSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
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
    private final int rankWindowSize;
    final List<RetrieverBuilder> sources;
    final Supplier<RankDoc[]> rankDocs;

    public RankDocsRetrieverBuilder(
        int rankWindowSize,
        List<RetrieverBuilder> sources,
        Supplier<RankDoc[]> rankDocs,
        List<QueryBuilder> preFilterQueryBuilders
    ) {
        this.rankWindowSize = rankWindowSize;
        this.rankDocs = rankDocs;
        this.sources = sources;
        this.preFilterQueryBuilders = preFilterQueryBuilders;
    }

    @Override
    public String getName() {
        return NAME;
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
        var rewrittenFilters = rewritePreFilters(ctx);
        if (rewrittenFilters != preFilterQueryBuilders) {
            return new RankDocsRetrieverBuilder(rankWindowSize, sources, rankDocs, rewrittenFilters);
        }
        return this;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        // this is used to fetch all documents form the parent retrievers (i.e. sources)
        // so that we can use all the matched documents to compute aggregations, nested hits etc
        DisMaxQueryBuilder disMax = new DisMaxQueryBuilder().tieBreaker(0f);
        for (var retriever : sources) {
            var query = retriever.topDocsQuery();
            if (query != null) {
                if (retriever.retrieverName() != null) {
                    query.queryName(retriever.retrieverName());
                }
                disMax.add(query);
            }
        }
        // ignore prefilters of this level, they are already propagated to children
        return disMax;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        // here we force a custom sort based on the rank of the documents
        // TODO: should we adjust to account for other fields sort options just for the top ranked docs?
        if (searchSourceBuilder.rescores() == null || searchSourceBuilder.rescores().isEmpty()) {
            searchSourceBuilder.sort(Arrays.asList(new RankDocsSortBuilder(rankDocs.get()), new ScoreSortBuilder()));
        }
        if (searchSourceBuilder.explain() != null && searchSourceBuilder.explain()) {
            searchSourceBuilder.trackScores(true);
        }
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        RankDocsQueryBuilder rankQuery = new RankDocsQueryBuilder(rankDocs.get());
        // if we have aggregations we need to compute them based on all doc matches, not just the top hits
        // so we just post-filter the top hits based on the rank queries we have
        if (searchSourceBuilder.aggregations() != null) {
            boolQuery.should(rankQuery);
            // compute a disjunction of all the query sources that were executed to compute the top rank docs
            QueryBuilder disjunctionOfSources = topDocsQuery();
            if (disjunctionOfSources != null) {
                boolQuery.should(disjunctionOfSources);
            }
            // post filter the results so that the top docs are still the same
            searchSourceBuilder.postFilter(rankQuery);
        } else {
            boolQuery.must(rankQuery);
        }
        // add any prefilters present in the retriever
        for (var preFilterQueryBuilder : preFilterQueryBuilders) {
            boolQuery.filter(preFilterQueryBuilder);
        }
        searchSourceBuilder.query(boolQuery);
    }

    @Override
    protected boolean doEquals(Object o) {
        RankDocsRetrieverBuilder other = (RankDocsRetrieverBuilder) o;
        return Arrays.equals(rankDocs.get(), other.rankDocs.get())
            && sources.equals(other.sources)
            && rankWindowSize == other.rankWindowSize;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(rankDocs.get()), sources, rankWindowSize);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("toXContent() is not supported for " + this.getClass());
    }
}
