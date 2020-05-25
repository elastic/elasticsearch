/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlAutoUpdateService;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DatafeedConfigAutoUpdater implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(DatafeedConfigAutoUpdater.class);
    private final DatafeedConfigProvider provider;
    private final IndexNameExpressionResolver expressionResolver;

    public DatafeedConfigAutoUpdater(DatafeedConfigProvider provider, IndexNameExpressionResolver expressionResolver) {
        this.provider = provider;
        this.expressionResolver = expressionResolver;
    }

    @Override
    public boolean isMinNodeVersionSupported(Version minNodeVersion) {
        return minNodeVersion.onOrAfter(Version.V_8_0_0);
    }

    @Override
    public boolean isAbleToRun(ClusterState latestState) {
        String[] indices = expressionResolver.concreteIndexNames(latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.configIndexName());
        for (String index : indices) {
            if (latestState.metadata().hasIndex(index) == false) {
                continue;
            }
            IndexRoutingTable routingTable = latestState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
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
    public void runUpdate() {
        PlainActionFuture<List<DatafeedConfig.Builder>> getdatafeeds = PlainActionFuture.newFuture();
        provider.expandDatafeedConfigs("_all", true, getdatafeeds);
        List<DatafeedConfig.Builder> datafeedConfigBuilders = getdatafeeds.actionGet();
        List<DatafeedUpdate> updates = datafeedConfigBuilders.stream()
            .map(DatafeedConfig.Builder::build)
            .filter(DatafeedConfig::aggsRewritten)
            .map(datafeedConfig -> new DatafeedUpdate.Builder()
                .setAggregations(datafeedConfig.getAggProvider())
                .setId(datafeedConfig.getId())
                .build())
            .collect(Collectors.toList());
        if (updates.isEmpty()) {
            return;
        }

        logger.debug(() -> new ParameterizedMessage("{} datafeeds are currently being updated",
            updates.stream().map(DatafeedUpdate::getId).collect(Collectors.toList())));

        List<Exception> failures = new ArrayList<>();
        for (DatafeedUpdate update : updates) {
            PlainActionFuture<DatafeedConfig> updateDatafeeds = PlainActionFuture.newFuture();
            provider.updateDatefeedConfig(update.getId(),
                update,
                Collections.emptyMap(),
                (updatedConfig, listener) -> listener.onResponse(Boolean.TRUE),
                updateDatafeeds);
            try {
                updateDatafeeds.actionGet();
                logger.debug(() -> new ParameterizedMessage("[{}] datafeed successfully updated", update.getId()));
            } catch (Exception ex) {
                logger.warn(new ParameterizedMessage("[{}] failed being updated", update.getId()), ex);
                failures.add(new ElasticsearchException("Failed to update datafeed {}", ex, update.getId()));
            }
        }
        if (failures.isEmpty()) {
            logger.debug(() -> new ParameterizedMessage("{} datafeeds are finished being updated",
                updates.stream().map(DatafeedUpdate::getId).collect(Collectors.toList())));
            return;
        }

        ElasticsearchException exception = new ElasticsearchException("some datafeeds failed being upgraded.");
        failures.forEach(exception::addSuppressed);
        throw exception;
    }
}
