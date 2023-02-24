/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class AnalyticsCollectionService {

    private final Client clientWithOrigin;

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public AnalyticsCollectionService(
        Client client,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /**
     * Retrieve an analytics collection by name {@AnalyticsCollection}
     *
     * @param collectionName {@AnalyticsCollection} name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void getAnalyticsCollection(String collectionName, ActionListener<AnalyticsCollection> listener) {

    }

    /**
     * Create a new {@AnalyticsCollection}
     *
     * @param analyticsCollection {@AnalyticsCollection} to be created.
     * @param listener The action listener to invoke on response/failure.
     */
    public void createAnalyticsCollection(AnalyticsCollection analyticsCollection, ActionListener<AnalyticsCollection> listener) {

    }

    /**
     * Delete an analytics collection by name {@AnalyticsCollection}
     *
     * @param collectionName {@AnalyticsCollection} name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void deleteAnalyticsCollection(String collectionName, ActionListener<AnalyticsCollection> listener) {

    }
}
