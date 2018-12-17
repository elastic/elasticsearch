/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataFrameInitializationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DataFrameInitializationService.class);

    private final Client client;
    private final ThreadPool threadPool;

    private final AtomicBoolean indexTemplateCreationInProgress = new AtomicBoolean(false);

    public DataFrameInitializationService(ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.client = client;
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (event.localNodeMaster()) {
            addTemplatesIfMissing(state);
        }
    }

    private void addTemplatesIfMissing(ClusterState state) {
        if (state.metaData().getTemplates().containsKey(DataFrameInternalIndex.INDEX_TEMPLATE_NAME) == false) {
            if (indexTemplateCreationInProgress.compareAndSet(false, true)) {
                logger.info("add index template for data frame internal index");
                final Executor executor = threadPool.generic();
                executor.execute(() -> {
                    DataFrameInternalIndex.createIndexTemplate(client, ActionListener.wrap(r -> {
                        if (r) {
                            // template creation is logged by MetaDataIndexTemplateService already
                            logger.debug("Created data frame internal index");
                        }
                        indexTemplateCreationInProgress.set(false);
                    }, e -> {
                        logger.error("Error creating data frame internal index", e);
                        indexTemplateCreationInProgress.set(false);
                    }));
                });
            } else {
                logger.trace("index template for data frame internal index in progress");
            }
        } else {
            logger.debug("found existing data frame index template, no action required");
        }
    }
}
