/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RankDocsQueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilderWrapper;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.sort.ScoreSortBuilder;
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
public final class QueryRuleRetrieverBuilder extends CompoundRetrieverBuilder<QueryRuleRetrieverBuilder> {

    public static final String NAME = "rule";

    public static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    public static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<QueryRuleRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
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
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder innerRetriever = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(innerRetriever.getName());
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public static QueryRuleRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
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
        super(new ArrayList<>(), rankWindowSize);
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
        addChild(new QueryRuleRetrieverBuilderWrapper(retrieverBuilder));
    }

    public QueryRuleRetrieverBuilder(
        List<String> rulesetIds,
        Map<String, Object> matchCriteria,
        List<RetrieverSource> retrieverSource,
        int rankWindowSize,
        String retrieverName,
        List<QueryBuilder> preFilterQueryBuilders
    ) {
        super(retrieverSource, rankWindowSize);
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
        this.retrieverName = retrieverName;
        this.preFilterQueryBuilders = preFilterQueryBuilders;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public int rankWindowSize() {
        return rankWindowSize;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder source) {
        checkValidSort(source.sorts());
        source.query(new RuleQueryBuilder(source.query(), matchCriteria, rulesetIds));
        return source;
    }

    private static void checkValidSort(List<SortBuilder<?>> sortBuilders) {
        if (sortBuilders.isEmpty()) {
            return;
        }

        if (sortBuilders.getFirst() instanceof ScoreSortBuilder == false) {
            throw new IllegalArgumentException("[" + NAME + "] retriever only supports sort by score, got: " + sortBuilders);
        }
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array(RULESET_IDS_FIELD.getPreferredName(), rulesetIds.toArray());
        builder.startObject(MATCH_CRITERIA_FIELD.getPreferredName());
        builder.mapContents(matchCriteria);
        builder.endObject();
        builder.field(RETRIEVER_FIELD.getPreferredName(), innerRetrievers.getFirst().retriever());
        // We need to explicitly include this here as it's not propagated by the wrapper
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    protected QueryRuleRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return new QueryRuleRetrieverBuilder(
            rulesetIds,
            matchCriteria,
            newChildRetrievers,
            rankWindowSize,
            retrieverName,
            newPreFilterQueryBuilders
        );
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
        assert rankResults.size() == 1;
        ScoreDoc[] scoreDocs = rankResults.getFirst();
        RankDoc[] rankDocs = new RuleQueryRankDoc[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            if (explain) {
                rankDocs[i] = new RuleQueryRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex, rulesetIds, matchCriteria);
            } else {
                rankDocs[i] = new RuleQueryRankDoc(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
            }
            rankDocs[i].rank = i + 1;
        }
        return rankDocs;
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

    /**
     * We need to wrap the QueryRulesRetrieverBuilder in order to ensure that the top docs query that is generated
     * by this retriever correctly generates and executes a Rule query.
     */
    class QueryRuleRetrieverBuilderWrapper extends RetrieverBuilderWrapper<QueryRuleRetrieverBuilderWrapper> {
        protected QueryRuleRetrieverBuilderWrapper(RetrieverBuilder in) {
            super(in);
        }

        @Override
        protected QueryRuleRetrieverBuilderWrapper clone(RetrieverBuilder in) {
            return new QueryRuleRetrieverBuilderWrapper(in);
        }

        @Override
        public QueryBuilder topDocsQuery() {
            return new RuleQueryBuilder(in.topDocsQuery(), matchCriteria, rulesetIds);
        }

        @Override
        public QueryBuilder explainQuery() {
            return new RankDocsQueryBuilder(
                in.getRankDocs(),
                new QueryBuilder[] { new RuleQueryBuilder(in.explainQuery(), matchCriteria, rulesetIds) },
                true
            );
        }
    }
}
