/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

/**
 * Adapter interface so that the actions needed for {@link DefaultCheckpointProvider} can execute in the same way when using either an
 * {@link ElasticsearchClient} (for local-cluster requests) and a {@link RemoteClusterClient} (for remote-cluster requests).
 */
interface CheckpointClient {

    /**
     * Execute {@link GetIndexAction}.
     */
    void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener);

    /**
     * Execute {@link IndicesStatsAction}.
     */
    void getIndicesStats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener);

    /**
     * Execute {@link GetCheckpointAction}.
     */
    void getCheckpoint(GetCheckpointAction.Request request, ActionListener<GetCheckpointAction.Response> listener);

    /**
     * Construct a {@link CheckpointClient} which executes its requests on the local cluster.
     */
    static CheckpointClient local(ElasticsearchClient client) {
        return new CheckpointClient() {
            @Override
            public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
                client.execute(GetIndexAction.INSTANCE, request, listener);
            }

            @Override
            public void getIndicesStats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener) {
                client.execute(IndicesStatsAction.INSTANCE, request, listener);
            }

            @Override
            public void getCheckpoint(GetCheckpointAction.Request request, ActionListener<GetCheckpointAction.Response> listener) {
                client.execute(GetCheckpointAction.INSTANCE, request, listener);
            }
        };
    }

    /**
     * Construct a {@link CheckpointClient} which executes its requests on a remote cluster.
     */
    static CheckpointClient remote(RemoteClusterClient client) {
        return new CheckpointClient() {
            @Override
            public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
                client.execute(GetIndexAction.REMOTE_TYPE, request, listener);
            }

            @Override
            public void getIndicesStats(IndicesStatsRequest request, ActionListener<IndicesStatsResponse> listener) {
                client.execute(IndicesStatsAction.REMOTE_TYPE, request, listener);
            }

            @Override
            public void getCheckpoint(GetCheckpointAction.Request request, ActionListener<GetCheckpointAction.Response> listener) {
                client.execute(GetCheckpointAction.REMOTE_TYPE, request, listener);
            }
        };
    }
}
