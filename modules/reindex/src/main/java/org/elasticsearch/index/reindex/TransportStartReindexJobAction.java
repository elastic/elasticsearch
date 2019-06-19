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

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportStartReindexJobAction
    extends TransportMasterNodeAction<StartReindexJobAction.Request, StartReindexJobAction.Response> {

    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportStartReindexJobAction(TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          ClusterService clusterService, PersistentTasksService persistentTasksService) {
        super(StartReindexJobAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            StartReindexJobAction.Request::new);
        this.persistentTasksService = persistentTasksService;
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
        String generatedId = UUIDs.randomBase64UUID();

        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
            .filter(e -> ReindexPlugin.HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        ReindexJob job = new ReindexJob(request.getReindexRequest(), filteredHeaders);

        // TODO: Task name
        persistentTasksService.sendStartRequest(generatedId, ReindexTask.NAME, job, new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> persistentTask) {
                    if (request.getWaitForCompletion()) {
                        waitForReindexDone(persistentTask.getId(), listener);
                    } else {
                        waitForReindexTask(persistentTask.getId(), listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: This probably should not happen as we are generating the UUID ourselves?
                    if (e instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot create job [" + generatedId +
                            "] because it has already been created (task exists)", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            });
    }

    private void waitForReindexTask(String taskId, ActionListener<StartReindexJobAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, Objects::nonNull, TimeValue.timeValueSeconds(15),
                new PersistentTasksService.WaitForPersistentTaskListener<ReindexJob>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> task) {
                        listener.onResponse(new StartReindexJobAction.Response(true, taskId));
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

    private void waitForReindexDone(String taskId, ActionListener<StartReindexJobAction.Response> listener) {
        // TODO: Configurable timeout? This currently has a low timeout to prevent tests from hanging.
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexDonePredicate(), TimeValue.timeValueSeconds(15),
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexJob>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> task) {
                    ReindexJobState state = (ReindexJobState) task.getState();
                    if (state.getJobException() == null) {
                        listener.onResponse(new StartReindexJobAction.Response(true, taskId, state.getReindexResponse()));
                    } else {
                        listener.onFailure(state.getJobException());
                    }
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

    private class ReindexDonePredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();
            // TODO: Dataframes has a lot of handling around an existing, not non-assigned task. Do we need that?
            return assignment != null && assignment.isAssigned() && isDone(persistentTask);
        }

        private boolean isDone(PersistentTasksCustomMetaData.PersistentTask<?> task) {
            ReindexJobState state = (ReindexJobState) task.getState();
            return state != null && (state.getReindexResponse() != null || state.getJobException() != null);
        }
    }
}
