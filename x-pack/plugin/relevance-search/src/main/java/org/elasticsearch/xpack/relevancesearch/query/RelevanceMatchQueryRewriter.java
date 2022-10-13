/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.relevancesearch.settings.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettingsService;

import java.io.IOException;

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

}
