/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.TransportVersions.ML_ROLLOVER_LEGACY_INDICES;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Rollover the various .ml-anomalies result indices
 * updating the read and write aliases
 */
public class MlAnomaliesIndexUpdate implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(MlAnomaliesIndexUpdate.class);

    private final IndexNameExpressionResolver expressionResolver;
    private final OriginSettingClient client;

    public MlAnomaliesIndexUpdate(IndexNameExpressionResolver expressionResolver, Client client) {
        this.expressionResolver = expressionResolver;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    public boolean isMinTransportVersionSupported(TransportVersion minTransportVersion) {
        // Automatic rollover does not require any new features
        // but wait for all nodes to be upgraded anyway
        return minTransportVersion.onOrAfter(ML_ROLLOVER_LEGACY_INDICES);
    }

    @Override
    public boolean isAbleToRun(ClusterState latestState) {
        // Find the .ml-anomalies-shared and all custom results indices
        String[] indices = expressionResolver.concreteIndexNames(
            latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.jobResultsIndexPattern()
        );

        for (String index : indices) {
            IndexRoutingTable routingTable = latestState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getName() {
        return "ml_anomalies_index_update";
    }

    @Override
    public void runUpdate(ClusterState latestState) {
        List<Exception> failures = new ArrayList<>();

        // list all indices starting .ml-anomalies-
        // this includes the shared index and all custom results indices
        String[] indices = expressionResolver.concreteIndexNames(
            latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.jobResultsIndexPattern()
        );

        for (String index : indices) {
            boolean isCompatibleIndexVersion = MlIndexAndAlias.indexIsReadWriteCompatibleInV9(
                latestState.metadata().getProject().index(index).getCreationVersion()
            );

            // Ensure the index name is of a format amenable to simplifying maintenance
            boolean isCompatibleIndexFormat = MlIndexAndAlias.has6DigitSuffix(index);

            if (isCompatibleIndexVersion && isCompatibleIndexFormat) {
                continue;
            }

            // Check if this index has already been rolled over
            String latestIndex = MlIndexAndAlias.latestIndexMatchingBaseName(index, expressionResolver, latestState);

            if (index.equals(latestIndex) == false) {
                logger.debug("index [{}] will not be rolled over as there is a later index [{}]", index, latestIndex);
                continue;
            }

            PlainActionFuture<Boolean> updated = new PlainActionFuture<>();
            rollAndUpdateAliases(latestState, index, updated);
            try {
                updated.actionGet();
            } catch (Exception ex) {
                var message = "failed rolling over legacy ml anomalies index [" + index + "]";
                logger.warn(message, ex);
                if (ex instanceof ElasticsearchException elasticsearchException) {
                    failures.add(new ElasticsearchStatusException(message, elasticsearchException.status(), elasticsearchException));
                } else {
                    failures.add(new ElasticsearchStatusException(message, RestStatus.REQUEST_TIMEOUT, ex));
                }

                break;
            }
        }

        if (failures.isEmpty()) {
            logger.info("legacy ml anomalies indices rolled over and aliases updated");
            return;
        }

        var exception = new ElasticsearchStatusException("failed to roll over legacy ml anomalies", RestStatus.CONFLICT);
        failures.forEach(exception::addSuppressed);
        throw exception;
    }

    private void rollAndUpdateAliases(ClusterState clusterState, String index, ActionListener<Boolean> listener) {
        // Create an alias specifically for rolling over.
        // The ml-anomalies index has aliases for each job, any
        // of which could be used but that means one alias is
        // treated differently.
        // Using a `.` in the alias name avoids any conflicts
        // as AD job Ids cannot start with `.`
        String rolloverAlias = index + ".rollover_alias";

        // If the index does not end in a digit then rollover does not know
        // what to name the new index so it must be specified in the request.
        // Otherwise leave null and rollover will calculate the new name
        String newIndexName = MlIndexAndAlias.has6DigitSuffix(index) ? null : index + MlIndexAndAlias.FIRST_INDEX_SIX_DIGIT_SUFFIX;
        IndicesAliasesRequestBuilder aliasRequestBuilder = client.admin()
            .indices()
            .prepareAliases(
                MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT
            );

        SubscribableListener.<Boolean>newForked(
            l -> { createAliasForRollover(index, rolloverAlias, l.map(AcknowledgedResponse::isAcknowledged)); }
        ).<String>andThen((l, success) -> {
            rollover(rolloverAlias, newIndexName, l);
        }).<Boolean>andThen((l, newIndexNameResponse) -> {
            MlIndexAndAlias.addIndexAliasesRequests(aliasRequestBuilder, index, newIndexNameResponse, clusterState);
            // Delete the new alias created for the rollover action
            aliasRequestBuilder.removeAlias(newIndexNameResponse, rolloverAlias);
            MlIndexAndAlias.updateAliases(aliasRequestBuilder, l);
        }).addListener(listener);
    }

    private void rollover(String alias, @Nullable String newIndexName, ActionListener<String> listener) {
        MlIndexAndAlias.rollover(client, new RolloverRequest(alias, newIndexName), listener);
    }

    private void createAliasForRollover(String indexName, String aliasName, ActionListener<IndicesAliasesResponse> listener) {
        MlIndexAndAlias.createAliasForRollover(logger, client, indexName, aliasName, listener);
    }
}
