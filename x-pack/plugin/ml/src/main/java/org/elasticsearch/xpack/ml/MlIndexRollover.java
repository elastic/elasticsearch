/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * If any of the indices listed in {@code indicesToRollover} are legacy indices
 * then call rollover to produce a new index with the current version. If the
 * index does not have an alias the alias is created first.
 * If none of the {@code indicesToRollover} exist or they are all non-legacy
 * indices then nothing will be updated.
 */
public class MlIndexRollover implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(MlIndexRollover.class);

    public record IndexPatternAndAlias(String indexPattern, String alias) {}

    private final IndexNameExpressionResolver expressionResolver;
    private final OriginSettingClient client;
    private final List<IndexPatternAndAlias> indicesToRollover;

    public MlIndexRollover(List<IndexPatternAndAlias> indicesToRollover, IndexNameExpressionResolver expressionResolver, Client client) {
        this.expressionResolver = expressionResolver;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.indicesToRollover = indicesToRollover;
    }

    @Override
    public boolean isMinTransportVersionSupported(TransportVersion minTransportVersion) {
        // Wait for all nodes to be upgraded to ensure that the
        // newly created index will be of the latest version.
        return minTransportVersion.onOrAfter(TransportVersions.ML_ROLLOVER_LEGACY_INDICES);
    }

    @Override
    public boolean isAbleToRun(ClusterState latestState) {
        for (var indexPatternAndAlias : indicesToRollover) {
            String[] indices = expressionResolver.concreteIndexNames(
                latestState,
                IndicesOptions.lenientExpandOpenHidden(),
                indexPatternAndAlias.indexPattern
            );
            if (indices.length == 0) {
                // The index does not exist but the MlAutoUpdateService will
                // need to run this action and mark it as done.
                // Ignore the missing index and continue the loop
                continue;
            }

            String latestIndex = MlIndexAndAlias.latestIndex(indices);
            IndexRoutingTable routingTable = latestState.getRoutingTable().index(latestIndex);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String getName() {
        return "ml_legacy_index_rollover";
    }

    @Override
    public void runUpdate(ClusterState latestState) {
        List<Exception> failures = new ArrayList<>();

        for (var indexPatternAndAlias : indicesToRollover) {
            PlainActionFuture<Boolean> rolloverIndices = new PlainActionFuture<>();
            rolloverLegacyIndices(latestState, indexPatternAndAlias.indexPattern(), indexPatternAndAlias.alias(), rolloverIndices);
            try {
                rolloverIndices.actionGet();
            } catch (Exception ex) {
                logger.warn(() -> "failed rolling over legacy index [" + indexPatternAndAlias.indexPattern() + "]", ex);
                if (ex instanceof ElasticsearchException elasticsearchException) {
                    failures.add(
                        new ElasticsearchStatusException("Failed rollover", elasticsearchException.status(), elasticsearchException)
                    );
                } else {
                    failures.add(new ElasticsearchStatusException("Failed rollover", RestStatus.REQUEST_TIMEOUT, ex));
                }

                break;
            }
        }

        if (failures.isEmpty()) {
            logger.info("ML legacy indices rolled over");
            return;
        }

        ElasticsearchException exception = new ElasticsearchException("some error");
        failures.forEach(exception::addSuppressed);
        throw exception;
    }

    private void rolloverLegacyIndices(ClusterState clusterState, String indexPattern, String alias, ActionListener<Boolean> listener) {
        var concreteIndices = expressionResolver.concreteIndexNames(clusterState, IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED, indexPattern);

        if (concreteIndices.length == 0) {
            // no matching indices
            listener.onResponse(Boolean.FALSE);
            return;
        }

        String latestIndex = MlIndexAndAlias.latestIndex(concreteIndices);
        // Indices created before 8.0 are read only in 9
        boolean isCompatibleIndexVersion = MlIndexAndAlias.indexIsReadWriteCompatibleInV9(
            clusterState.metadata().getProject().index(latestIndex).getCreationVersion()
        );
        boolean hasAlias = clusterState.getMetadata().getProject().hasAlias(alias);

        if (isCompatibleIndexVersion && hasAlias) {
            // v8 index with alias, no action required
            listener.onResponse(Boolean.FALSE);
            return;
        }

        SubscribableListener.<Boolean>newForked(l -> {
            if (hasAlias == false) {
                MlIndexAndAlias.updateWriteAlias(
                    client,
                    alias,
                    null,
                    latestIndex,
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    l
                );
            } else {
                l.onResponse(Boolean.TRUE);
            }
        }).<Boolean>andThen((l, success) -> {
            if (isCompatibleIndexVersion == false) {
                logger.info("rolling over legacy index [{}] with alias [{}]", latestIndex, alias);
                rollover(alias, l);
            } else {
                l.onResponse(Boolean.TRUE);
            }
        }).addListener(listener);
    }

    private void rollover(String alias, ActionListener<Boolean> listener) {
        client.admin().indices().rolloverIndex(new RolloverRequest(alias, null), listener.delegateFailure((l, response) -> {
            l.onResponse(Boolean.TRUE);
        }));
    }

    /**
     * True if the version is read *and* write compatible not just read only compatible
     */
    static boolean isCompatibleIndexVersion(IndexVersion version) {
        return version.onOrAfter(IndexVersions.MINIMUM_COMPATIBLE);
    }
}
