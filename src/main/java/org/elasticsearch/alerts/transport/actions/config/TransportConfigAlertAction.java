/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.ConfigurationManager;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the config operation.
 */
public class TransportConfigAlertAction extends TransportMasterNodeOperationAction<ConfigAlertRequest, ConfigAlertResponse> {

    private final ConfigurationManager configManager;

    @Inject
    public TransportConfigAlertAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters, ConfigurationManager configManager) {
        super(settings, ConfigAlertAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.configManager = configManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ConfigAlertRequest newRequest() {
        return new ConfigAlertRequest();
    }

    @Override
    protected ConfigAlertResponse newResponse() {
        return new ConfigAlertResponse();
    }

    @Override
    protected void masterOperation(ConfigAlertRequest request, ClusterState state, ActionListener<ConfigAlertResponse> listener) throws ElasticsearchException {
        try {
            IndexResponse indexResponse = configManager.updateConfig(request.getConfigSource());
            listener.onResponse(new ConfigAlertResponse(indexResponse));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ConfigAlertRequest request, ClusterState state) {
        request.beforeLocalFork(); // This is the best place to make the config source safe
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, AlertsStore.ALERT_INDEX);
    }


}
