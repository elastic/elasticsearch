/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettingsService;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class OrganicQueryRewriter extends AbstractQueryRewriter<RelevanceSettings> {

    private final QueryFieldsResolver queryFieldsResolver;

    OrganicQueryRewriter(ClusterService clusterService, RelevanceSettingsService settingsService, QueryFieldsResolver queryFieldsResolver) {
        super(clusterService, settingsService);
        this.queryFieldsResolver = queryFieldsResolver;
    }

    @Override
    public QueryBuilder rewriteQuery(
        QueryBuilder baseQuery,
        RelevanceMatchQueryBuilder relevanceMatchQuery,
        SearchExecutionContext context
    ) {
        return rewriteQuery(relevanceMatchQuery, context);
    }

    public QueryBuilder rewriteQuery(RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context) {
        Map<String, Float> fieldsAndBoosts = retrieveFieldsAndBoosts(relevanceMatchQuery, context);
        QueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(relevanceMatchQuery.getQuery(), fieldsAndBoosts);
        queryBuilder = applyScriptScoring(relevanceMatchQuery, context, queryBuilder);
        return queryBuilder;
    }

    @Override
    protected String getSettingsId(SearchEngine searchEngine) {
        return searchEngine.getRelevanceSettingsId();
    }

    @Override
    protected String getSettingsId(RelevanceMatchQueryBuilder relevanceMatchQuery) {
        return relevanceMatchQuery.getRelevanceSettingsId();
    }

    private QueryBuilder applyScriptScoring(
        RelevanceMatchQueryBuilder relevanceMatchQuery,
        SearchExecutionContext context,
        QueryBuilder queryBuilder
    ) {
        RelevanceSettings relevanceSettings = this.getSettings(relevanceMatchQuery, context);
        if (relevanceSettings != null) {
            String scriptSource = relevanceSettings.getQueryConfiguration().getScriptSource();
            if (scriptSource != null) {
                return QueryBuilders.scriptScoreQuery(queryBuilder, new Script(scriptSource));
            }
        }
        return queryBuilder;
    }

    private Map<String, Float> retrieveFieldsAndBoosts(RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context) {
        RelevanceSettings relevanceSettings = this.getSettings(relevanceMatchQuery, context);
        if (relevanceSettings != null) {
            return relevanceSettings.getQueryConfiguration().getFieldsAndBoosts();
        }

        Collection<String> fields = queryFieldsResolver.getQueryFields(context);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("[relevance_match] query cannot find text fields in the index");
        }
        return fields.stream().collect(Collectors.toMap(Function.identity(), (field) -> AbstractQueryBuilder.DEFAULT_BOOST));
    }
}
