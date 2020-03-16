/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

/**
 * This class deletes empty indices matching .ml-state* pattern that are not pointed at by the .ml-state-write alias.
 */
public class EmptyStateIndexRemover implements MlDataRemover {
    
    private final OriginSettingClient client;

    public EmptyStateIndexRemover(OriginSettingClient client) {
        this.client = Objects.requireNonNull(client);
    }

    @Override
    public void remove(ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        try {
            if (isTimedOutSupplier.get()) {
                listener.onResponse(false);
                return;
            }
            getEmptyStateIndices(
                ActionListener.wrap(
                    emptyStateIndices -> {
                        if (emptyStateIndices.isEmpty()) {
                            listener.onResponse(true);
                            return;
                        }
                        getCurrentStateIndices(
                            ActionListener.wrap(
                                currentStateIndices -> {
                                    Set<String> stateIndicesToRemove = Sets.difference(emptyStateIndices, currentStateIndices);
                                    executeDeleteEmptyStateIndices(stateIndicesToRemove, listener);
                                },
                                listener::onFailure
                            )
                        );
                    },
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void getEmptyStateIndices(ActionListener<Set<String>> listener) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest().indices(AnomalyDetectorsIndex.jobStateIndexPattern());
        client.admin().indices().stats(
            indicesStatsRequest,
            ActionListener.wrap(
                indicesStatsResponse -> {
                    Set<String> emptyStateIndices =
                        indicesStatsResponse.getIndices().values().stream()
                            .filter(stats -> stats.getTotal().getDocs().getCount() == 0)
                            .map(IndexStats::getIndex)
                            .collect(toSet());
                    listener.onResponse(emptyStateIndices);
                },
                listener::onFailure
            )
        );
    }

    private void getCurrentStateIndices(ActionListener<Set<String>> listener) {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(AnomalyDetectorsIndex.jobStateIndexPattern());
        client.admin().indices().getIndex(
            getIndexRequest,
            ActionListener.wrap(
                getIndexResponse -> listener.onResponse(getCurrentStateIndicesFromResponse(getIndexResponse)),
                listener::onFailure
            )
        );
    }

    private static Set<String> getCurrentStateIndicesFromResponse(GetIndexResponse getIndexResponse) {
        Set<String> currentStateIndices = new HashSet<>();
        for (var aliasEntry : getIndexResponse.aliases()) {
            for (var alias : aliasEntry.value) {
                if (AnomalyDetectorsIndex.jobStateIndexWriteAlias().equals(alias.alias())) {
                    currentStateIndices.add(aliasEntry.key);
                }
            }
        }
        return currentStateIndices;
    }

    private void executeDeleteEmptyStateIndices(Set<String> emptyStateIndices, ActionListener<Boolean> listener) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(emptyStateIndices.toArray(new String[0]));
        client.admin().indices().delete(
            deleteIndexRequest,
            ActionListener.wrap(
                deleteResponse -> listener.onResponse(deleteResponse.isAcknowledged()),
                listener::onFailure
            )
        );
    }
}
