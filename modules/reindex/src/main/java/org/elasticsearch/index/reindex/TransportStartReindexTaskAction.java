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
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportStartReindexTaskAction
    extends HandledTransportAction<StartReindexTaskAction.Request, StartReindexTaskAction.Response> {

    public static final Setting<Integer> MAX_CONCURRENT_REINDEX_TASKS =
        Setting.intSetting("reindex.tasks.max_concurrent", 10000, 1, Setting.Property.Dynamic, Setting.Property.NodeScope);
    private volatile int maxConcurrentTasks;

    private final List<String> headersToInclude;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final ReindexValidator reindexValidator;
    private final ReindexIndexClient reindexIndexClient;

    @Inject
    public TransportStartReindexTaskAction(Settings settings, Client client, TransportService transportService, ThreadPool threadPool,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                           ClusterService clusterService, PersistentTasksService persistentTasksService,
                                           AutoCreateIndex autoCreateIndex, NamedXContentRegistry xContentRegistry) {
        super(StartReindexTaskAction.NAME, transportService, actionFilters, StartReindexTaskAction.Request::new);
        this.maxConcurrentTasks = MAX_CONCURRENT_REINDEX_TASKS.get(settings);
        this.headersToInclude = ReindexHeaders.REINDEX_INCLUDED_HEADERS.get(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.persistentTasksService = persistentTasksService;
        this.reindexIndexClient = new ReindexIndexClient(client, clusterService, xContentRegistry);
        this.clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CONCURRENT_REINDEX_TASKS, (max) -> maxConcurrentTasks = max);
    }

    @Override
    protected void doExecute(Task task, StartReindexTaskAction.Request request, ActionListener<StartReindexTaskAction.Response> listener) {
        try {
            reindexValidator.initialValidation(request.getReindexRequest());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (tasks != null && tasks.count() >= maxConcurrentTasks) {
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> reindexTasks = tasks.findTasks(ReindexTask.NAME, (t) -> true);
            if (reindexTasks != null && reindexTasks.size() >= maxConcurrentTasks) {
                listener.onFailure(new IllegalStateException("Maximum concurrent reindex operations exceeded [max=" +
                    maxConcurrentTasks + "]"));
                return;
            }
        }

        String generatedId = UUIDs.randomBase64UUID();

        ThreadContext threadContext = threadPool.getThreadContext();
        Map<String, String> included = headersToInclude.stream()
            .map(header -> new Tuple<>(header, threadContext.getHeader(header)))
            .filter(t -> t.v2() != null)
            .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        // In the current implementation, we only need to store task results if we do not wait for completion
        boolean storeTaskResult = request.getWaitForCompletion() == false;
        ReindexTaskParams job = new ReindexTaskParams(storeTaskResult, included);

        ReindexTaskStateDoc reindexState = new ReindexTaskStateDoc(request.getReindexRequest());
        reindexIndexClient.createReindexTaskDoc(generatedId, reindexState, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                persistentTasksService.sendStartRequest(generatedId, ReindexTask.NAME, job, new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> persistentTask) {
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

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void waitForReindexDone(String taskId, ActionListener<StartReindexTaskAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(true), null,
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> task) {
                    ReindexPersistentTaskState state = (ReindexPersistentTaskState) task.getState();
                    if (state.getStatus() == ReindexPersistentTaskState.Status.ASSIGNMENT_FAILED) {
                        listener.onFailure(new ElasticsearchException("Reindexing failed. Task node could not assign itself as the "
                            + "coordinating node in the " + ReindexIndexClient.REINDEX_ALIAS + " index"));
                    } else if (state.getStatus() == ReindexPersistentTaskState.Status.DONE) {
                        reindexIndexClient.getReindexTaskDoc(taskId, new ActionListener<>() {
                            @Override
                            public void onResponse(ReindexTaskState taskState) {
                                ReindexTaskStateDoc reindexState = taskState.getStateDoc();
                                if (reindexState.getException() == null) {
                                    listener.onResponse(new StartReindexTaskAction.Response(task.getId(), taskId,
                                        reindexState.getReindexResponse()));
                                } else {
                                    Exception exception = reindexState.getException();
                                    RestStatus statusCode = reindexState.getFailureStatusCode();
                                    listener.onFailure(new ElasticsearchStatusException(exception.getMessage(), statusCode, exception));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                    } else {
                        throw new AssertionError("Unexpected reindex job status: " + state.getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private void waitForReindexTask(String taskId, ActionListener<StartReindexTaskAction.Response> listener) {
        // TODO: Configurable timeout?
        persistentTasksService.waitForPersistentTaskCondition(taskId, new ReindexPredicate(false), null,
            new PersistentTasksService.WaitForPersistentTaskListener<ReindexTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexTaskParams> task) {
                    ReindexPersistentTaskState state = (ReindexPersistentTaskState) task.getState();
                    listener.onResponse(new StartReindexTaskAction.Response(task.getId(), state.getEphemeralTaskId().toString()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
    }

    private static class ReindexPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

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
            if (assignment == null || assignment.isAssigned() == false) {
                return false;
            }

            ReindexPersistentTaskState state = (ReindexPersistentTaskState) persistentTask.getState();


            if (waitForDone == false) {
                return isStarted(state);
            } else {
                return isDone(state);
            }
        }

        private boolean isStarted(ReindexPersistentTaskState state) {
            return state != null;
        }

        private boolean isDone(ReindexPersistentTaskState state) {
            return state != null && state.isDone();
        }
    }
}
