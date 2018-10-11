/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

public class TransportPauseFollowAction extends HandledTransportAction<PauseFollowAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportPauseFollowAction(
            final Settings settings,
            final TransportService transportService,
            final ActionFilters actionFilters,
            final Client client,
            final PersistentTasksService persistentTasksService) {
        super(settings, PauseFollowAction.NAME, transportService, actionFilters, PauseFollowAction.Request::new);
        this.client = client;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(
            final Task task,
            final PauseFollowAction.Request request,
            final ActionListener<AcknowledgedResponse> listener) {

        client.admin().cluster().state(new ClusterStateRequest(), ActionListener.wrap(r -> {
            PersistentTasksCustomMetaData persistentTasksMetaData = r.getState().metaData().custom(PersistentTasksCustomMetaData.TYPE);
            if (persistentTasksMetaData == null) {
                listener.onFailure(new IllegalArgumentException("no shard follow tasks for [" + request.getFollowIndex() + "]"));
                return;
            }

            List<String> shardFollowTaskIds = persistentTasksMetaData.tasks().stream()
                .filter(persistentTask -> ShardFollowTask.NAME.equals(persistentTask.getTaskName()))
                .filter(persistentTask -> {
                    ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                    return shardFollowTask.getFollowShardId().getIndexName().equals(request.getFollowIndex());
                })
                .map(PersistentTasksCustomMetaData.PersistentTask::getId)
                .collect(Collectors.toList());

            if (shardFollowTaskIds.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("no shard follow tasks for [" + request.getFollowIndex() + "]"));
                return;
            }

            final AtomicInteger counter = new AtomicInteger(shardFollowTaskIds.size());
            final AtomicReferenceArray<Object> responses = new AtomicReferenceArray<>(shardFollowTaskIds.size());
            int i = 0;

            for (String taskId : shardFollowTaskIds) {
                final int shardId = i++;
                persistentTasksService.sendRemoveRequest(taskId,
                        new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                                responses.set(shardId, task);
                                finalizeResponse();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                responses.set(shardId, e);
                                finalizeResponse();
                            }

                            void finalizeResponse() {
                                Exception error = null;
                                if (counter.decrementAndGet() == 0) {
                                    for (int j = 0; j < responses.length(); j++) {
                                        Object response = responses.get(j);
                                        if (response instanceof Exception) {
                                            if (error == null) {
                                                error = (Exception) response;
                                            } else {
                                                error.addSuppressed((Throwable) response);
                                            }
                                        }
                                    }

                                    if (error == null) {
                                        // include task ids?
                                        listener.onResponse(new AcknowledgedResponse(true));
                                    } else {
                                        // TODO: cancel all started tasks
                                        listener.onFailure(error);
                                    }
                                }
                            }
                        });
            }
        }, listener::onFailure));
    }

}
