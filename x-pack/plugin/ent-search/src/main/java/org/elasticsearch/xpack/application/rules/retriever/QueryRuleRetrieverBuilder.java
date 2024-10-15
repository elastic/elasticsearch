/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.rules.RuleQueryBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query rule retriever applies query rules defined in one or more rulesets to the underlying retriever.
 */
public final class QueryRuleRetrieverBuilder extends CompoundRetrieverBuilder<QueryRuleRetrieverBuilder> {

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

    public QueryRuleRetrieverBuilder(
        List<String> rulesetIds,
        Map<String, Object> matchCriteria,
        RetrieverBuilder retrieverBuilder,
        int rankWindowSize
    ) {
        super(List.of(new RetrieverSource(retrieverBuilder, null)), rankWindowSize);
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
    }

    public QueryRuleRetrieverBuilder(
        List<String> rulesetIds,
        Map<String, Object> matchCriteria,
        List<RetrieverSource> retrieverSource,
        int rankWindowSize,
        String retrieverName
    ) {
        super(retrieverSource, rankWindowSize);
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
        this.retrieverName = retrieverName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected SearchSourceBuilder createSearchSourceBuilder(PointInTimeBuilder pit, RetrieverBuilder retrieverBuilder) {
        var sourceBuilder = new SearchSourceBuilder().pointInTimeBuilder(pit)
            .trackTotalHits(false)
            .storedFields(new StoredFieldsContext(false))
            .size(rankWindowSize);
        retrieverBuilder.extractToSearchSourceBuilder(sourceBuilder, true);

        QueryBuilder query = sourceBuilder.query();
        if (query != null && query instanceof RuleQueryBuilder == false) {
            QueryBuilder ruleQuery = new RuleQueryBuilder(query, matchCriteria, rulesetIds);
            sourceBuilder.query(ruleQuery);
        }

        addSort(sourceBuilder);

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
    protected QueryRuleRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers) {
        return new QueryRuleRetrieverBuilder(rulesetIds, matchCriteria, newChildRetrievers, rankWindowSize, retrieverName);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.getFirst();
        RankDoc[] rankDocs = new RankDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            rankDocs[i] = new RankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
        }
        return rankDocs;
    }

    @Override
    public QueryBuilder explainQuery() {
        // the original matching set of the QueryRuleRetriever retriever is specified by its nested retriever
        return new RankDocsQueryBuilder(rankDocs, new QueryBuilder[] { innerRetrievers.getFirst().retriever().explainQuery() }, true);
    }

    @Override
    public boolean doEquals(Object o) {
        QueryRuleRetrieverBuilder that = (QueryRuleRetrieverBuilder) o;
        return super.doEquals(o) && Objects.equals(rulesetIds, that.rulesetIds) && Objects.equals(matchCriteria, that.matchCriteria);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), rulesetIds, matchCriteria);
    }
}
