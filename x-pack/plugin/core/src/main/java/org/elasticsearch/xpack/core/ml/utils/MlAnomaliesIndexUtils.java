/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;

public class MlAnomaliesIndexUtils {
    public static void rollover(Client client, RolloverRequest rolloverRequest, ActionListener<String> listener) {
        client.admin()
                .indices()
                .rolloverIndex(
                    rolloverRequest,
                        ActionListener.wrap(response -> listener.onResponse(response.getNewIndex()), e -> {
                            if (e instanceof ResourceAlreadyExistsException alreadyExistsException) {
                                // The destination index already exists possibly because it has been rolled over already.
                                listener.onResponse(alreadyExistsException.getIndex().getName());
                            } else {
                                listener.onFailure(e);
                            }
                        })
                );
    }

    public static void createAliasForRollover(
        Logger logger,
        Client client,
        String indexName,
        String aliasName,
        ActionListener<IndicesAliasesResponse> listener
    ) {
        logger.warn("creating rollover [{}] alias for [{}]", aliasName, indexName);
        client.admin()
            .indices()
            .prepareAliases(
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            )
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(aliasName).isHidden(true))
            .execute(listener);
    }

    public static void updateAliases(IndicesAliasesRequestBuilder request, ActionListener<Boolean> listener) {
        request.execute(listener.delegateFailure((l, response) -> l.onResponse(Boolean.TRUE)));
    }

    public static IndicesAliasesRequestBuilder addIndexAliasesRequests(
        IndicesAliasesRequestBuilder aliasRequestBuilder,
        String oldIndex,
        String newIndex,
        ClusterState clusterState
    ) {
        // Multiple jobs can share the same index each job
        // has a read and write alias that needs updating
        // after the rollover
        var meta = clusterState.metadata().getProject().index(oldIndex);
        assert meta != null;
        if (meta == null) {
            return aliasRequestBuilder;
        }

        for (var alias : meta.getAliases().values()) {
            if (isAnomaliesWriteAlias(alias.alias())) {
                aliasRequestBuilder.addAliasAction(
                    IndicesAliasesRequest.AliasActions.add().index(newIndex).alias(alias.alias()).isHidden(true).writeIndex(true)
                );
                aliasRequestBuilder.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(oldIndex).alias(alias.alias()));
            } else if (isAnomaliesReadAlias(alias.alias())) {
                String jobId = AnomalyDetectorsIndex.jobIdFromAlias(alias.alias());
                aliasRequestBuilder.addAliasAction(
                    IndicesAliasesRequest.AliasActions.add()
                        .index(newIndex)
                        .alias(alias.alias())
                        .isHidden(true)
                        .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                );
            }
        }

        return aliasRequestBuilder;
    }

    public static boolean isAnomaliesWriteAlias(String aliasName) {
        return aliasName.startsWith(AnomalyDetectorsIndexFields.RESULTS_INDEX_WRITE_PREFIX);
    }

    static boolean isAnomaliesReadAlias(String aliasName) {
        if (aliasName.startsWith(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX) == false) {
            return false;
        }

        // See {@link AnomalyDetectorsIndex#jobResultsAliasedName}
        String jobIdPart = aliasName.substring(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length());
        // If this is a write alias it will start with a `.` character
        // which is not a valid job id.
        return MlStrings.isValidId(jobIdPart);
    }


}
