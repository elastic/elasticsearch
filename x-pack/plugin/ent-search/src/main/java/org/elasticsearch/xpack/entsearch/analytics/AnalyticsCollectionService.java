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
import org.elasticsearch.cluster.service.ClusterService;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class AnalyticsCollectionService {

    private final Client clientWithOrigin;

    private final ClusterService clusterService;

    public AnalyticsCollectionService(Client client, ClusterService clusterService) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.clusterService = clusterService;
    }

    public void getAnalyticsCollection(String collectionName, ActionListener<AnalyticsCollection> listener) {

    }

    public void putAnalyticsCollection(AnalyticsCollection analyticsCollection, ActionListener<AnalyticsCollection> listener) {

    }

    public void deleteAnalyticsCollection(ActionListener<AnalyticsCollection> listener) {

    }

    public void listAnalyticsCollection(ActionListener<AnalyticsCollection> listener) {

    }
}
