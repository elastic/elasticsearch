/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

public class TransportStartReindexJobAction
    extends TransportMasterNodeAction<StartReindexJobAction.Request, StartReindexJobAction.Response> {

    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportStartReindexJobAction(TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
        super(StartReindexJobAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            StartReindexJobAction.Request::new);
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected StartReindexJobAction.Response newResponse() {
        return new StartReindexJobAction.Response();
    }

    @Override
    protected void masterOperation(StartReindexJobAction.Request request, ClusterState clusterState,
                                   ActionListener<StartReindexJobAction.Response> listener) {
        startPersistentTask(request.getReindexRequest(), listener);
    }

    private void startPersistentTask(ReindexRequest request, ActionListener<StartReindexJobAction.Response> listener) {
        String taskId = UUIDs.randomBase64UUID();

        // TODO: Task name
        persistentTasksService.sendStartRequest(taskId, ReindexTask.NAME, new ReindexJob(request),
                ActionListener.wrap(
                        reindexPersistentTask -> waitForReindexTaskStarted(taskId, request, listener),
                        e -> {
                            // TODO: This probably should not happen as we are generating the UUID ourselves?
                            if (e instanceof ResourceAlreadyExistsException) {
                                e = new ElasticsearchStatusException("Cannot create job [" + taskId +
                                        "] because it has already been created (task exists)", RestStatus.CONFLICT, e);
                            }
                            listener.onFailure(e);
                        }));
    }


    private void waitForReindexTaskStarted(String taskId, ReindexRequest request, ActionListener<StartReindexJobAction.Response> listener) {
        persistentTasksService.waitForPersistentTaskCondition(taskId, Objects::nonNull, request.getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskListener<ReindexJob>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> task) {
                        boolean waitForReindexToComplete = true;

                        if (waitForReindexToComplete) {
                            startReindex(taskId, request, listener);
                        } else {
                            listener.onResponse(new StartReindexJobAction.Response(true, taskId));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchException("Creation of task for Reindex Job ID ["
                                + taskId + "] timed out after [" + timeout + "]"));
                    }
                });
    }

    private void startReindex(String taskId, ReindexRequest request, ActionListener<StartReindexJobAction.Response> listener) {
        client.execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse response) {
                listener.onResponse(new StartReindexJobAction.Response(true, taskId, response));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(StartReindexJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
