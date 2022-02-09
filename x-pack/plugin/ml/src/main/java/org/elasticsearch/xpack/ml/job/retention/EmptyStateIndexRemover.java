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
            getEmptyStateIndices(ActionListener.wrap(emptyStateIndices -> {
                if (emptyStateIndices.isEmpty()) {
                    listener.onResponse(true);
                    return;
                }
                getCurrentStateIndices(ActionListener.wrap(currentStateIndices -> {
                    Set<String> stateIndicesToRemove = Sets.difference(emptyStateIndices, currentStateIndices);
                    if (stateIndicesToRemove.isEmpty()) {
                        listener.onResponse(true);
                        return;
                    }
                    executeDeleteEmptyStateIndices(stateIndicesToRemove, listener);
                }, listener::onFailure));
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void getEmptyStateIndices(ActionListener<Set<String>> listener) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(AnomalyDetectorsIndex.jobStateIndexPattern());
        indicesStatsRequest.setParentTask(parentTaskId);
        client.admin().indices().stats(indicesStatsRequest, ActionListener.wrap(indicesStatsResponse -> {
            Set<String> emptyStateIndices = indicesStatsResponse.getIndices()
                .values()
                .stream()
                .filter(stats -> stats.getTotal().getDocs().getCount() == 0)
                .map(IndexStats::getIndex)
                .collect(toSet());
            listener.onResponse(emptyStateIndices);
        }, listener::onFailure));
    }

    private void getCurrentStateIndices(ActionListener<Set<String>> listener) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(AnomalyDetectorsIndex.jobStateIndexWriteAlias());
        getIndexRequest.setParentTask(parentTaskId);
        client.admin()
            .indices()
            .getIndex(
                getIndexRequest,
                ActionListener.wrap(getIndexResponse -> listener.onResponse(Set.of(getIndexResponse.getIndices())), listener::onFailure)
            );
    }

    private void executeDeleteEmptyStateIndices(Set<String> emptyStateIndices, ActionListener<Boolean> listener) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(emptyStateIndices.toArray(new String[0]));
        deleteIndexRequest.setParentTask(parentTaskId);
        client.admin()
            .indices()
            .delete(
                deleteIndexRequest,
                ActionListener.wrap(deleteResponse -> listener.onResponse(deleteResponse.isAcknowledged()), listener::onFailure)
            );
    }
}
