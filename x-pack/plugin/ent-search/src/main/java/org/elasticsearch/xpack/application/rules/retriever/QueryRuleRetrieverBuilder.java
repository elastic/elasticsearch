/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query rule retriever applies query rules defined in one or more rulesets to the underlying retriever.
 */
public final class QueryRuleRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "rule";
    public static final NodeFeature QUERY_RULE_RETRIEVERS_SUPPORTED = new NodeFeature("query_rule_retriever_supported");

    public static final ParseField RULESET_IDS_FIELD = new ParseField("ruleset_ids");
    public static final ParseField MATCH_CRITERIA_FIELD = new ParseField("match_criteria");
    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<QueryRuleRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "rule",
        args -> {
            List<String> rulesetIds = (List<String>) args[0];
            Map<String, Object> matchCriteria = (Map<String, Object>) args[1];
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[2];
            return new QueryRuleRetrieverBuilder(rulesetIds, matchCriteria, retrieverBuilder);
        }
    );

    static {
        PARSER.declareStringArray(constructorArg(), RULESET_IDS_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), MATCH_CRITERIA_FIELD);
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> p.namedObject(RetrieverBuilder.class, n, c), RETRIEVER_FIELD);
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
    private final RetrieverBuilder retrieverBuilder;

    public QueryRuleRetrieverBuilder(List<String> rulesetIds, Map<String, Object> matchCriteria, RetrieverBuilder retrieverBuilder) {
        this.rulesetIds = rulesetIds;
        this.matchCriteria = matchCriteria;
        this.retrieverBuilder = retrieverBuilder;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        assert rankDocs != null : "{rankDocs} should have been materialized at this point";

        // TODO is this correct?
        return retrieverBuilder.topDocsQuery();
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {

        // TODO throw if compoundUsed is true?

        QueryBuilder organicQuery = retrieverBuilder.topDocsQuery();
        QueryBuilder queryBuilder = new RuleQueryBuilder(organicQuery, matchCriteria, rulesetIds);

        searchSourceBuilder.query(queryBuilder);
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array(RULESET_IDS_FIELD.getPreferredName(), rulesetIds.toArray());
        builder.startObject(MATCH_CRITERIA_FIELD.getPreferredName());
        builder.mapContents(matchCriteria);
        builder.endObject();
        builder.startObject("retriever");
        builder.startObject();
        builder.field(retrieverBuilder.getName());
        retrieverBuilder.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
    }

    @Override
    public boolean doEquals(Object o) {
        QueryRuleRetrieverBuilder that = (QueryRuleRetrieverBuilder) o;
        return Objects.equals(rulesetIds, that.rulesetIds)
            && Objects.equals(matchCriteria, that.matchCriteria)
            && Objects.equals(retrieverBuilder, that.retrieverBuilder);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(rulesetIds, matchCriteria, retrieverBuilder);
    }
}
