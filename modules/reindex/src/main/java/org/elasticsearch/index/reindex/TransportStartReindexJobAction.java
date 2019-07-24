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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.index.reindex.ReindexTask.REINDEX_ORIGIN;

public class TransportStartReindexJobAction
    extends TransportMasterNodeAction<StartReindexJobAction.Request, StartReindexJobAction.Response> {

    private final PersistentTasksService persistentTasksService;
    private final Client taskClient;

    @Inject
    public TransportStartReindexJobAction(TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
        super(StartReindexJobAction.NAME, transportService, clusterService, threadPool, actionFilters, StartReindexJobAction.Request::new,
            indexNameExpressionResolver);
        this.persistentTasksService = persistentTasksService;
        this.taskClient = new OriginSettingClient(client, REINDEX_ORIGIN);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected StartReindexJobAction.Response read(StreamInput in) throws IOException {
        return new StartReindexJobAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, StartReindexJobAction.Request request, ClusterState state,
                                   ActionListener<StartReindexJobAction.Response> listener) {
        // TODO: If the connection is lost to the master, this action might be retried creating two tasks.
        //  Eventually prevent this (perhaps by pre-generating UUID).
        String generatedId = UUIDs.randomBase64UUID();

        // In the current implementation, we only need to store task results if we do not wait for completion
        boolean storeTaskResult = request.getWaitForCompletion() == false;
        ReindexJob job = new ReindexJob(storeTaskResult, threadPool.getThreadContext().getHeaders());

        boolean reindexIndexExists = state.routingTable().hasIndex(ReindexTask.REINDEX_INDEX);

        createReindexTaskDoc(generatedId, request.getReindexRequest(), reindexIndexExists, new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
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

            @Override
            public void onFailure(Exception e) {
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
                        listener.onResponse(new StartReindexJobAction.Response(taskId, state.getReindexResponse()));
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
                    ReindexJobState state = (ReindexJobState) task.getState();
                    listener.onResponse(new StartReindexJobAction.Response(state.getEphemeralTaskId().toString()));
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

    private void createReindexTaskDoc(String taskId, ReindexRequest reindexRequest, boolean indexExists, ActionListener<Void> listener) {
        if (indexExists) {
            IndexRequest indexRequest = new IndexRequest(ReindexTask.REINDEX_INDEX).id(taskId).opType(DocWriteRequest.OpType.CREATE);
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
                ReindexTaskIndexState reindexState = new ReindexTaskIndexState(reindexRequest);
                reindexState.toXContent(builder, ToXContent.EMPTY_PARAMS);
                indexRequest.source(builder);
            } catch (IOException e) {
                listener.onFailure(new ElasticsearchException("Couldn't serialize reindex request into XContent", e));
            }
            taskClient.index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.settings(reindexIndexSettings());
            createIndexRequest.index(ReindexTask.REINDEX_INDEX);
            createIndexRequest.cause("auto(reindex api)");
            createIndexRequest.mapping("_doc", "{\"dynamic\": false}", XContentType.JSON);

            taskClient.admin().indices().create(createIndexRequest, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    createReindexTaskDoc(taskId, reindexRequest, true, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        try {
                            createReindexTaskDoc(taskId, reindexRequest, true, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        }
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
            if (assignment == null || assignment.isAssigned() == false) {
                return false;
            }

            ReindexJobState state = (ReindexJobState) persistentTask.getState();


            if (waitForDone == false) {
                return isStarted(state);
            } else {
                return isDone(state);
            }
        }

        private boolean isStarted(ReindexJobState state) {
            return state != null;
        }

        private boolean isDone(ReindexJobState state) {
            return state != null && (state.getReindexResponse() != null || state.getJobException() != null);
        }
    }

    private Settings reindexIndexSettings() {
        // TODO: Copied from task index
        return Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetaData.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();
    }
}
