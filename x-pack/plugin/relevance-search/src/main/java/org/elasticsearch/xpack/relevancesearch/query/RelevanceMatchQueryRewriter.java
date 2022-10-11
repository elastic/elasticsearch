/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.relevancesearch.relevance.QueryConfiguration;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Rewrites the relevance_match query using information stored in the {@link RelevanceMatchQueryBuilder} class.
 *
 * It holds other services used for this task to get relevance and curations settings
 */
public class RelevanceMatchQueryRewriter {

    private final RelevanceSettingsService relevanceSettingsService;
    private final CurationsService curationsService;

    private final QueryFieldsResolver queryFieldsResolver;

    private QueryConfiguration queryConfiguration;

    public RelevanceMatchQueryRewriter(
        RelevanceSettingsService relevanceSettingsService,
        CurationsService curationsService,
        QueryFieldsResolver queryFieldsResolver
    ) {
        this.relevanceSettingsService = relevanceSettingsService;
        this.curationsService = curationsService;
        this.queryFieldsResolver = queryFieldsResolver;
        this.queryConfiguration = null;
    }

    public Query rewriteQuery(RelevanceMatchQueryBuilder relevanceMatchQueryBuilder, SearchExecutionContext context) throws IOException {
        this.setQueryConfiguration(relevanceMatchQueryBuilder.getRelevanceSettingsId());

        Map<String, Float> fieldsAndBoosts = retrieveFieldsAndBoosts(context);
        String scriptSource = retrieveScriptSource();

        QueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(relevanceMatchQueryBuilder.getQuery(), fieldsAndBoosts);
        if (scriptSource != null) {
            queryBuilder = QueryBuilders.scriptScoreQuery(queryBuilder, new Script(scriptSource));
        }
        return applyCurations(queryBuilder, relevanceMatchQueryBuilder).toQuery(context);
    }

    private static QueryBuilder applyExcludedDocs(QueryBuilder queryBuilder, CurationSettings curationSettings) {
        if (curationSettings.excludedDocs().isEmpty() == false) {
            BoolQueryBuilder booleanQueryBuilder = new BoolQueryBuilder();
            booleanQueryBuilder.should(queryBuilder);

            BoolQueryBuilder excludedDocsBuilder = new BoolQueryBuilder();
            for (CurationSettings.DocumentReference excludedDoc : curationSettings.excludedDocs()) {
                BoolQueryBuilder mustQueryBuilder = new BoolQueryBuilder();
                mustQueryBuilder.must(new TermsQueryBuilder("_index", excludedDoc.index()));
                mustQueryBuilder.must(new TermsQueryBuilder("_id", excludedDoc.id()));

                excludedDocsBuilder.mustNot(mustQueryBuilder);
            }
            booleanQueryBuilder.filter(excludedDocsBuilder);

            queryBuilder = booleanQueryBuilder;
        }
        return queryBuilder;
    }

    private static QueryBuilder applyPinnedDocs(QueryBuilder queryBuilder, CurationSettings curationSettings) {
        if (curationSettings.pinnedDocs().isEmpty() == false) {
            final PinnedQueryBuilder.Item[] items = curationSettings.pinnedDocs()
                .stream()
                .map(docRef -> new PinnedQueryBuilder.Item(docRef.index(), docRef.id()))
                .toArray(PinnedQueryBuilder.Item[]::new);
            queryBuilder = new PinnedQueryBuilder(queryBuilder, items);
        }
        return queryBuilder;
    }

    private void setQueryConfiguration(String relevanceSettingsId) {
        if (relevanceSettingsId == null) {
            // we'll work with defaults
            queryConfiguration = null;
        } else {
            try {
                RelevanceSettings relevanceSettings = relevanceSettingsService.getRelevanceSettings(relevanceSettingsId);
                queryConfiguration = relevanceSettings.getQueryConfiguration();
            } catch (RelevanceSettingsService.RelevanceSettingsNotFoundException e) {
                throw new IllegalArgumentException("[relevance_match] query can't find search settings: " + relevanceSettingsId);
            } catch (RelevanceSettingsService.RelevanceSettingsInvalidException e) {
                throw new IllegalArgumentException("[relevance_match] invalid relevance search settings for: " + relevanceSettingsId);
            }
        }
    }

    private String retrieveScriptSource() {
        if (queryConfiguration != null) {
            return queryConfiguration.getScriptSource();
        }
        return null;
    }

    private Map<String, Float> retrieveFieldsAndBoosts(SearchExecutionContext context) {
        Map<String, Float> fieldsAndBoosts;
        if (queryConfiguration != null) {
            fieldsAndBoosts = queryConfiguration.getFieldsAndBoosts();
        } else {
            Collection<String> fields = queryFieldsResolver.getQueryFields(context);
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("[relevance_match] query cannot find text fields in the index");
            }
            fieldsAndBoosts = fields.stream().collect(Collectors.toMap(Function.identity(), (field) -> AbstractQueryBuilder.DEFAULT_BOOST));
        }
        return fieldsAndBoosts;
    }

    private QueryBuilder applyCurations(QueryBuilder queryBuilder, RelevanceMatchQueryBuilder relevanceMatchQueryBuilder) {
        String curationsSettingsId = relevanceMatchQueryBuilder.getCurationsSettingsId();
        if (curationsSettingsId != null) {
            try {
                CurationSettings curationSettings = curationsService.getCurationsSettings(curationsSettingsId);
                final boolean conditionMatch = curationSettings.conditions().stream().anyMatch(c -> c.match(relevanceMatchQueryBuilder));
                if (conditionMatch) {
                    queryBuilder = applyPinnedDocs(queryBuilder, curationSettings);
                    queryBuilder = applyExcludedDocs(queryBuilder, curationSettings);
                }

            } catch (CurationsService.CurationsSettingsNotFoundException e) {
                throw new IllegalArgumentException("[relevance_match] query cannot find curation settings: " + curationsSettingsId);
            }
        }

        return queryBuilder;
    }

}
