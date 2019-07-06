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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

public class TransportStartReindexJobAction
    extends TransportMasterNodeAction<StartReindexJobAction.Request, StartReindexJobAction.Response> {

    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportStartReindexJobAction(TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
        super(StartReindexJobAction.NAME, transportService, clusterService, threadPool, actionFilters, StartReindexJobAction.Request::new,
            indexNameExpressionResolver);
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected StartReindexJobAction.Response newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected StartReindexJobAction.Response read(StreamInput in) throws IOException {
        return new StartReindexJobAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, StartReindexJobAction.Request request, ClusterState state,
                                   ActionListener<StartReindexJobAction.Response> listener) {
        String generatedId = UUIDs.randomBase64UUID();

        ReindexJob job = new ReindexJob(request.getReindexRequest(), threadPool.getThreadContext().getHeaders());

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
                assert e instanceof ResourceAlreadyExistsException == false : "UUID generation should not produce conflicts";
                listener.onFailure(e);
            }
        });

    }

    private void waitForReindexDone(String taskId, ActionListener<StartReindexJobAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(true), null,
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

    private void waitForReindexTask(String taskId, ActionListener<StartReindexJobAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(false), null,
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexJob>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> task) {
                    String executorNode = task.getExecutorNode();
                    GetReindexJobTaskAction.Request request = new GetReindexJobTaskAction.Request(executorNode, taskId);
                    ThreadContext threadContext = threadPool.getThreadContext();
                    try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                        threadContext.markAsSystemContext();
                        client.execute(GetReindexJobTaskAction.INSTANCE, request, new ActionListener<>() {
                            @Override
                            public void onResponse(GetReindexJobTaskAction.Responses responses) {
                                List<GetReindexJobTaskAction.Response> tasks = responses.getTasks();
                                assert tasks.size() == 1 : "Expected 1 response, found " + tasks.size();
                                GetReindexJobTaskAction.Response response = tasks.get(0);
                                listener.onResponse(new StartReindexJobAction.Response(true, response.getTaskId().toString()));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
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

    private class ReindexPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private boolean waitForDone;

        private ReindexPredicate(boolean waitForDone) {
            this.waitForDone = waitForDone;
        }

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();
            return assignment != null && assignment.isAssigned() && (waitForDone == false || isDone(persistentTask));
        }

        private boolean isDone(PersistentTasksCustomMetaData.PersistentTask<?> task) {
            ReindexJobState state = (ReindexJobState) task.getState();
            return state != null && (state.getReindexResponse() != null || state.getJobException() != null);
        }
    }
}
