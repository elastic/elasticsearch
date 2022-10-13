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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.settings.Settings;
import org.elasticsearch.xpack.relevancesearch.settings.SettingsService;
import org.elasticsearch.xpack.relevancesearch.settings.SettingsService.SettingsServiceException;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettingsService;
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

    private final QueryRewriter organicQueryRewriter;
    private final QueryRewriter curationQueryRewriter;

    public RelevanceMatchQueryRewriter(
        ClusterService clusterService,
        RelevanceSettingsService relevanceSettingsService,
        CurationsService curationsService,
        QueryFieldsResolver queryFieldsResolver
    ) {
        this.organicQueryRewriter = new OrganicQueryRewriter(clusterService, relevanceSettingsService, queryFieldsResolver);
        this.curationQueryRewriter = new CurationQueryRewriter(clusterService, curationsService);
    }

    public QueryBuilder rewriteQuery(RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context) throws IOException {
        final QueryBuilder organicQuery = organicQueryRewriter.rewriteQuery(null, relevanceMatchQuery, context);
        return curationQueryRewriter.rewriteQuery(organicQuery, relevanceMatchQuery, context);
    }

    private interface QueryRewriter {
        QueryBuilder rewriteQuery(QueryBuilder baseQuery, RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context);
    }

    private abstract static class AbstractQueryRewriter<S extends Settings> implements QueryRewriter {
        private final ClusterService clusterService;
        private final SettingsService<S> settingsService;

        AbstractQueryRewriter(ClusterService clusterService, SettingsService<S> settingsService) {
            this.clusterService = clusterService;
            this.settingsService = settingsService;
        }

        protected S getSettings(RelevanceMatchQueryBuilder relevanceMatchQuery, SearchExecutionContext context) {
            String settingsId = getSettingsId(relevanceMatchQuery);
            SearchEngine searchEngine = getSearchEngine(context);

            if (settingsId == null && searchEngine != null) {
                settingsId = getSettingsId(searchEngine);
            }

            if (settingsId != null) {
                try {
                    return settingsService.getSettings(settingsId);
                } catch (SettingsServiceException e) {
                    throw new IllegalArgumentException("[relevance_match] " + e.getMessage());
                }
            }

            return null;
        }

        protected abstract String getSettingsId(SearchEngine searchEngine);

        protected abstract String getSettingsId(RelevanceMatchQueryBuilder relevanceMatchQuery);

        private SearchEngine getSearchEngine(SearchExecutionContext context) {
            if (context.getSearchEngineName() == null) {
                return null;
            }

            assert clusterService.state().metadata().searchEngines().containsKey(context.getSearchEngineName());

            return clusterService.state().metadata().searchEngines().get(context.getSearchEngineName());
        }
    }

    private static class OrganicQueryRewriter extends AbstractQueryRewriter<RelevanceSettings> {

        private final QueryFieldsResolver queryFieldsResolver;

        OrganicQueryRewriter(
            ClusterService clusterService,
            RelevanceSettingsService settingsService,
            QueryFieldsResolver queryFieldsResolver
        ) {
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
            return new CombinedFieldsQueryBuilder(relevanceMatchQuery.getQuery(), fieldsAndBoosts);
        }

        @Override
        protected String getSettingsId(SearchEngine searchEngine) {
            return searchEngine.getRelevanceSettingsId();
        }

        @Override
        protected String getSettingsId(RelevanceMatchQueryBuilder relevanceMatchQuery) {
            return relevanceMatchQuery.getRelevanceSettingsId();
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

    private static class CurationQueryRewriter extends AbstractQueryRewriter<CurationSettings> {
        CurationQueryRewriter(ClusterService clusterService, CurationsService settingsService) {
            super(clusterService, settingsService);
        }

        @Override
        protected String getSettingsId(SearchEngine searchEngine) {
            // TODO: add curation to engine.
            return null;
        }

        @Override
        protected String getSettingsId(RelevanceMatchQueryBuilder relevanceMatchQuery) {
            return relevanceMatchQuery.getCurationsSettingsId();
        }

        @Override
        public QueryBuilder rewriteQuery(
            QueryBuilder baseQuery,
            RelevanceMatchQueryBuilder relevanceMatchQuery,
            SearchExecutionContext context
        ) {
            CurationSettings curationSettings = getSettings(relevanceMatchQuery, context);

            if (curationSettings == null || curationSettings.conditions().stream().noneMatch(c -> c.match(relevanceMatchQuery))) {
                return baseQuery;
            }

            return applyExcludedDocs(applyPinnedDocs(baseQuery, curationSettings), curationSettings);
        }

        private QueryBuilder applyExcludedDocs(QueryBuilder queryBuilder, CurationSettings curationSettings) {
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

        private QueryBuilder applyPinnedDocs(QueryBuilder queryBuilder, CurationSettings curationSettings) {
            if (curationSettings.pinnedDocs().isEmpty() == false) {
                final PinnedQueryBuilder.Item[] items = curationSettings.pinnedDocs()
                    .stream()
                    .map(docRef -> new PinnedQueryBuilder.Item(docRef.index(), docRef.id()))
                    .toArray(PinnedQueryBuilder.Item[]::new);
                queryBuilder = new PinnedQueryBuilder(queryBuilder, items);
            }
            return queryBuilder;
        }
    }
}
