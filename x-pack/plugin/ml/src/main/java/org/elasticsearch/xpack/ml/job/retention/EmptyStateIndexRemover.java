/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static java.util.stream.Collectors.toSet;

/**
 * This class deletes empty indices matching .ml-state* pattern that are not pointed at by the .ml-state-write alias.
 */
public class EmptyStateIndexRemover implements MlDataRemover {

    private final OriginSettingClient client;
    private final TaskId parentTaskId;

    public EmptyStateIndexRemover(OriginSettingClient client, TaskId parentTaskId) {
        this.client = Objects.requireNonNull(client);
        this.parentTaskId = parentTaskId;
    }

    @Override
    public void remove(float requestsPerSec, ActionListener<Boolean> listener, BooleanSupplier isTimedOutSupplier) {
        try {
            if (isTimedOutSupplier.getAsBoolean()) {
                listener.onResponse(false);
                return;
            }
            getEmptyStateIndices(listener.delegateFailureAndWrap((delegate, emptyStateIndices) -> {
                if (emptyStateIndices.isEmpty()) {
                    delegate.onResponse(true);
                    return;
                }
                getCurrentStateIndices(delegate.delegateFailureAndWrap((l, currentStateIndices) -> {
                    Set<String> stateIndicesToRemove = Sets.difference(emptyStateIndices, currentStateIndices);
                    if (stateIndicesToRemove.isEmpty()) {
                        l.onResponse(true);
                        return;
                    }
                    executeDeleteEmptyStateIndices(stateIndicesToRemove, l);
                }));
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void getEmptyStateIndices(ActionListener<Set<String>> listener) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(AnomalyDetectorsIndex.jobStateIndexPattern());
        indicesStatsRequest.setParentTask(parentTaskId);
        client.admin()
            .indices()
            .stats(
                indicesStatsRequest,
                listener.delegateFailureAndWrap(
                    (l, indicesStatsResponse) -> l.onResponse(
                        indicesStatsResponse.getIndices()
                            .values()
                            .stream()
                            .filter(stats -> stats.getTotal().getDocs() == null || stats.getTotal().getDocs().getCount() == 0)
                            .map(IndexStats::getIndex)
                            .collect(toSet())
                    )
                )
            );
    }

    private void getCurrentStateIndices(ActionListener<Set<String>> listener) {
        GetIndexRequest getIndexRequest = new GetIndexRequest(MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT).indices(
            AnomalyDetectorsIndex.jobStateIndexWriteAlias()
        );
        getIndexRequest.setParentTask(parentTaskId);
        client.admin()
            .indices()
            .getIndex(
                getIndexRequest,
                listener.delegateFailureAndWrap((l, getIndexResponse) -> l.onResponse(Set.of(getIndexResponse.getIndices())))
            );
    }

    private void executeDeleteEmptyStateIndices(Set<String> emptyStateIndices, ActionListener<Boolean> listener) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(emptyStateIndices.toArray(new String[0]));
        deleteIndexRequest.setParentTask(parentTaskId);
        client.admin()
            .indices()
            .delete(
                deleteIndexRequest,
                listener.delegateFailureAndWrap((l, deleteResponse) -> l.onResponse(deleteResponse.isAcknowledged()))
            );
    }
}
