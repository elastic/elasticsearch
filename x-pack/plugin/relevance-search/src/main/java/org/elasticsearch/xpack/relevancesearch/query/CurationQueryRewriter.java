/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationsService;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;

class CurationQueryRewriter extends AbstractQueryRewriter<CurationSettings> {
    public CurationQueryRewriter(ClusterService clusterService, CurationsService settingsService) {
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
