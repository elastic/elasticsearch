/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RankDocsRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.rules.RuleQueryBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query rule retriever applies query rules defined in one or more rulesets to the underlying retriever.
 */
public final class QueryRuleRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "rule";
    public static final NodeFeature QUERY_RULE_RETRIEVERS_SUPPORTED = new NodeFeature("query_rule_retriever_supported");

    public static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    public static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<QueryRuleRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "rule",
        args -> {
            List<String> rulesetIds = (List<String>) args[0];
            Map<String, Object> matchCriteria = (Map<String, Object>) args[1];
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[2];
            int rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[3];
            return new QueryRuleRetrieverBuilder(rulesetIds, matchCriteria, retrieverBuilder, rankWindowSize);
        }
    );

    static {
        PARSER.declareStringArray(constructorArg(), RULESET_IDS_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> p.namedObject(RetrieverBuilder.class, n, c), RETRIEVER_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static QueryRuleRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(QUERY_RULE_RETRIEVERS_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        if (EnterpriseSearch.QUERY_RULES_RETRIEVER_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Query Rules");
        }
        try {
            return PARSER.apply(parser, context);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    private final List<String> rulesetIds;
    private final Map<String, Object> matchCriteria;
    private final CompoundRetrieverBuilder.RetrieverSource subRetriever;
    private final int rankWindowSize;
    private boolean executed = false;

    public QueryRuleRetrieverBuilder(
        List<String> rulesetIds,
        Map<String, Object> matchCriteria,
        RetrieverBuilder subRetriever,
        int rankWindowSize
    ) {
        this(rulesetIds, matchCriteria, new CompoundRetrieverBuilder.RetrieverSource(subRetriever, null), rankWindowSize);
    }

    private QueryRuleRetrieverBuilder(
        List<String> rulesetIds,
        Map<String, Object> matchCriteria,
        CompoundRetrieverBuilder.RetrieverSource subRetriever,
        int rankWindowSize
    ) {
        this.subRetriever = subRetriever;
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
        this.rankWindowSize = rankWindowSize;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isCompound() {
        return executed == false;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.getPointInTimeBuilder() == null) {
            throw new IllegalStateException("PIT is required");
        }

        if (executed) {
            return this;
        }

        // Rewrite prefilters
        var newPreFilters = rewritePreFilters(ctx);
        if (newPreFilters != preFilterQueryBuilders) {
            var ret = new QueryRuleRetrieverBuilder(rulesetIds, matchCriteria, subRetriever, rankWindowSize);
            ret.preFilterQueryBuilders = newPreFilters;
            return ret;
        }

        // Rewrite retriever sources
        var newRetriever = subRetriever.retriever().rewrite(ctx);
        if (newRetriever != subRetriever.retriever()) {
            return new QueryRuleRetrieverBuilder(rulesetIds, matchCriteria, newRetriever, rankWindowSize);
        } else {
            var newSource = subRetriever.source() != null
                ? subRetriever.source()
                : createSearchSourceBuilder(ctx.getPointInTimeBuilder(), newRetriever);
            var rewrittenSource = newSource.rewrite(ctx);
            if (rewrittenSource != subRetriever.source()) {
                return new QueryRuleRetrieverBuilder(
                    rulesetIds,
                    matchCriteria,
                    new CompoundRetrieverBuilder.RetrieverSource(newRetriever, rewrittenSource),
                    rankWindowSize
                );
            }
        }

        // execute searches
        final SetOnce<RankDoc[]> results = new SetOnce<>();
        final SearchRequest searchRequest = new SearchRequest().source(subRetriever.source());
        // The can match phase can reorder shards, so we disable it to ensure the stable ordering
        searchRequest.setPreFilterShardSize(Integer.MAX_VALUE);
        ctx.registerAsyncAction((client, listener) -> {
            client.execute(TransportSearchAction.TYPE, searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse resp) {
                    var rankDocs = getRankDocs(resp);
                    results.set(rankDocs);
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        });

        executed = true;
        return new RankDocsRetrieverBuilder(rankWindowSize, List.of(this), results::get, newPreFilters);
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        throw new IllegalStateException("Should not be called, missing a rewrite?");
    }

    protected SearchSourceBuilder createSearchSourceBuilder(PointInTimeBuilder pit, RetrieverBuilder retrieverBuilder) {
        var sourceBuilder = new SearchSourceBuilder().pointInTimeBuilder(pit)
            .trackTotalHits(false)
            .storedFields(new StoredFieldsContext(false))
            .size(rankWindowSize);
        retrieverBuilder.extractToSearchSourceBuilder(sourceBuilder, true);
        // TODO: ensure that the inner sort is by relevance and throw an error otherwise.

        QueryBuilder query = sourceBuilder.query();
        if (query != null && query instanceof RuleQueryBuilder == false) {
            QueryBuilder ruleQuery = new RuleQueryBuilder(query, matchCriteria, rulesetIds);
            sourceBuilder.query(ruleQuery);
        }

        // Record the shard id in the sort result
        List<SortBuilder<?>> sortBuilders = sourceBuilder.sorts() != null ? new ArrayList<>(sourceBuilder.sorts()) : new ArrayList<>();
        if (sortBuilders.isEmpty()) {
            sortBuilders.add(new ScoreSortBuilder());
        }
        sortBuilders.add(new FieldSortBuilder(FieldSortBuilder.SHARD_DOC_FIELD_NAME));
        sourceBuilder.sort(sortBuilders);

        return sourceBuilder;
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array(RULESET_IDS_FIELD.getPreferredName(), rulesetIds.toArray());
        builder.startObject(MATCH_CRITERIA_FIELD.getPreferredName());
        builder.mapContents(matchCriteria);
        builder.endObject();
    }

    @Override
    public QueryBuilder topDocsQuery() {
        QueryBuilder topDocsQuery = subRetriever.source().query();
        if (preFilterQueryBuilders.isEmpty()) {
            topDocsQuery.queryName(this.retrieverName);
            return topDocsQuery;
        }
        var ret = new BoolQueryBuilder().filter(topDocsQuery).queryName(this.retrieverName);
        preFilterQueryBuilders.stream().forEach(ret::filter);
        return subRetriever.source().query();
    }

    @Override
    public boolean doEquals(Object o) {
        QueryRuleRetrieverBuilder that = (QueryRuleRetrieverBuilder) o;
        return Objects.equals(rulesetIds, that.rulesetIds)
            && Objects.equals(matchCriteria, that.matchCriteria)
            && subRetriever.equals(that.subRetriever);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(subRetriever, rulesetIds, matchCriteria);
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
