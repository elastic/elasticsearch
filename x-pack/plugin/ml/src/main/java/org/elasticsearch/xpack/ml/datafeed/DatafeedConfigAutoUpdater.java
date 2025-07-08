/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file has been contributed to be a Generative AI
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.ml.MlAutoUpdateService;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.core.Strings.format;

public class DatafeedConfigAutoUpdater implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(DatafeedConfigAutoUpdater.class);
    private final DatafeedConfigProvider provider;
    private final IndexNameExpressionResolver expressionResolver;

    public DatafeedConfigAutoUpdater(DatafeedConfigProvider provider, IndexNameExpressionResolver expressionResolver) {
        this.provider = provider;
        this.expressionResolver = expressionResolver;
    }

    @Override
    public boolean isMinTransportVersionSupported(TransportVersion minNodeVersion) {
        return minNodeVersion.onOrAfter(TransportVersions.V_8_0_0);
    }

    @Override
    public boolean isAbleToRun(ClusterState latestState) {
        String[] indices = expressionResolver.concreteIndexNames(
            latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            MlConfigIndex.indexName()
        );
        for (String index : indices) {
            if (latestState.metadata().getProject().hasIndex(index) == false) {
                continue;
            }
            IndexRoutingTable routingTable = latestState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false || routingTable.readyForSearch() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getName() {
        return "datafeed_aggs_updater";
    }

    @Override
    public void runUpdate(ClusterState latestState) {
        PlainActionFuture<List<DatafeedConfig.Builder>> getdatafeeds = new PlainActionFuture<>();
        provider.expandDatafeedConfigs("_all", true, null, getdatafeeds);
        List<DatafeedConfig.Builder> datafeedConfigBuilders = getdatafeeds.actionGet();
        List<DatafeedUpdate> updates = datafeedConfigBuilders.stream()
            .map(DatafeedConfig.Builder::build)
            .filter(DatafeedConfig::aggsRewritten)
            .map(
                datafeedConfig -> new DatafeedUpdate.Builder().setAggregations(datafeedConfig.getAggProvider())
                    .setId(datafeedConfig.getId())
                    .build()
            )
            .toList();
        if (updates.isEmpty()) {
            return;
        }

        logger.debug(
            () -> format("%s datafeeds are currently being updated", updates.stream().map(DatafeedUpdate::getId).collect(toList()))
        );

        List<Exception> failures = new ArrayList<>();
        for (DatafeedUpdate update : updates) {
            PlainActionFuture<DatafeedConfig> updateDatafeeds = new PlainActionFuture<>();
            provider.updateDatefeedConfig(
                update.getId(),
                update,
                Collections.emptyMap(),
                (updatedConfig, listener) -> listener.onResponse(Boolean.TRUE),
                updateDatafeeds
            );
            try {
                updateDatafeeds.actionGet();
                logger.debug(() -> "[" + update.getId() + "] datafeed successfully updated");
            } catch (Exception ex) {
                logger.warn(() -> "[" + update.getId() + "] failed being updated", ex);
                if (ex instanceof ElasticsearchException elasticsearchException) {
                    failures.add(
                        new ElasticsearchStatusException(
                            "Failed to update datafeed {}",
                            elasticsearchException.status(),
                            elasticsearchException,
                            update.getId()
                        )
                    );
                } else {
                    failures.add(
                        new ElasticsearchStatusException("Failed to update datafeed {}", RestStatus.REQUEST_TIMEOUT, ex, update.getId())
                    );
                }
            }
        }
        if (failures.isEmpty()) {
            logger.debug(
                () -> format("%s datafeeds are finished being updated", updates.stream().map(DatafeedUpdate::getId).collect(toList()))
            );
            return;
        }

        ElasticsearchException exception = new ElasticsearchException("some datafeeds failed being upgraded.");
        failures.forEach(exception::addSuppressed);
        throw exception;
    }
}
