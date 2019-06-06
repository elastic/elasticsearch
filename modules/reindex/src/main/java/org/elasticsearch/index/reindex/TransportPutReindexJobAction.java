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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;

public class TransportPutReindexJobAction extends TransportMasterNodeAction<PutReindexJobAction.Request, AcknowledgedResponse> {

    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportPutReindexJobAction(TransportService transportService, ThreadPool threadPool,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
        super(PutReindexJobAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            PutReindexJobAction.Request::new);
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(PutReindexJobAction.Request request, ClusterState clusterState,
                                   ActionListener<AcknowledgedResponse> listener) {
        startPersistentTask(request.getReindexJob(), listener);
//        createIndex(job, listener, persistentTasksService, client, logger);
    }

//
//    static void createIndex(RollupJob job, ActionListener<AcknowledgedResponse> listener,
//                            PersistentTasksService persistentTasksService, Client client, Logger logger) {
//
//        String jobMetadata = "\"" + job.getConfig().getId() + "\":" + job.getConfig().toJSONString();
//
//        String mapping = Rollup.DYNAMIC_MAPPING_TEMPLATE
//                .replace(Rollup.MAPPING_METADATA_PLACEHOLDER, jobMetadata);
//
//        CreateIndexRequest request = new CreateIndexRequest(job.getConfig().getRollupIndex());
//        request.mapping(RollupField.TYPE_NAME, mapping, XContentType.JSON);
//
//        client.execute(CreateIndexAction.INSTANCE, request,
//                ActionListener.wrap(createIndexResponse -> startPersistentTask(job, listener, persistentTasksService), e -> {
//                    if (e instanceof ResourceAlreadyExistsException) {
//                        logger.debug("Rolled index already exists for rollup job [" + job.getConfig().getId() + "], updating metadata.");
//                        updateMapping(job, listener, persistentTasksService, client, logger);
//                    } else {
//                        String msg = "Could not create index for rollup job [" + job.getConfig().getId() + "]";
//                        logger.error(msg);
//                        listener.onFailure(new RuntimeException(msg, e));
//                    }
//                }));
//    }
//
//    @SuppressWarnings("unchecked")
//    static void updateMapping(RollupJob job, ActionListener<AcknowledgedResponse> listener,
//                              PersistentTasksService persistentTasksService, Client client, Logger logger) {
//
//        final String indexName = job.getConfig().getRollupIndex();
//
//        CheckedConsumer<GetMappingsResponse, Exception> getMappingResponseHandler = getMappingResponse -> {
//            MappingMetaData mappings = getMappingResponse.getMappings().get(indexName).get(RollupField.TYPE_NAME);
//            Object m = mappings.getSourceAsMap().get("_meta");
//            if (m == null) {
//                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected " +
//                    "to find _meta key in mapping of rollup index [" + indexName + "] but not found).";
//                logger.error(msg);
//                listener.onFailure(new RuntimeException(msg));
//                return;
//            }
//
//            Map<String, Object> metadata = (Map<String, Object>) m;
//            if (metadata.get(RollupField.ROLLUP_META) == null) {
//                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected " +
//                    "to find rollup meta key [" + RollupField.ROLLUP_META + "] in mapping of rollup index ["
//                    + indexName + "] but not found).";
//                logger.error(msg);
//                listener.onFailure(new RuntimeException(msg));
//                return;
//            }
//
//            Map<String, Object> rollupMeta = (Map<String, Object>)((Map<String, Object>) m).get(RollupField.ROLLUP_META);
//
//            String stringVersion = (String)((Map<String, Object>) m).get(Rollup.ROLLUP_TEMPLATE_VERSION_FIELD);
//            if (stringVersion == null) {
//                listener.onFailure(new IllegalStateException("Could not determine version of existing rollup metadata for index ["
//                    + indexName + "]"));
//                return;
//            }
//
//            if (rollupMeta.get(job.getConfig().getId()) != null) {
//                String msg = "Cannot create rollup job [" + job.getConfig().getId()
//                        + "] because job was previously created (existing metadata).";
//                logger.error(msg);
//                listener.onFailure(new ElasticsearchStatusException(msg, RestStatus.CONFLICT));
//                return;
//            }
//
//            rollupMeta.put(job.getConfig().getId(), job.getConfig());
//            metadata.put(RollupField.ROLLUP_META, rollupMeta);
//            Map<String, Object> newMapping = mappings.getSourceAsMap();
//            newMapping.put("_meta", metadata);
//            PutMappingRequest request = new PutMappingRequest(indexName);
//            request.type(RollupField.TYPE_NAME);
//            request.source(newMapping);
//            client.execute(PutMappingAction.INSTANCE, request,
//                    ActionListener.wrap(putMappingResponse -> startPersistentTask(job, listener, persistentTasksService),
//                            listener::onFailure));
//        };
//
//        GetMappingsRequest request = new GetMappingsRequest();
//        client.execute(GetMappingsAction.INSTANCE, request, ActionListener.wrap(getMappingResponseHandler,
//                e -> {
//                    String msg = "Could not update mappings for rollup job [" + job.getConfig().getId() + "]";
//                    logger.error(msg);
//                    listener.onFailure(new RuntimeException(msg, e));
//                }));
//    }
//
    private void startPersistentTask(ReindexJob job, ActionListener<AcknowledgedResponse> listener) {

        // TODO: Task name
        persistentTasksService.sendStartRequest("", "reindex/job", job,
                ActionListener.wrap(
                        rollupConfigPersistentTask -> waitForReindexStarted(job, listener),
                        e -> {
                            if (e instanceof ResourceAlreadyExistsException) {
                                e = new ElasticsearchStatusException("Cannot create job [" + job.getId() +
                                        "] because it has already been created (task exists)", RestStatus.CONFLICT, e);
                            }
                            listener.onFailure(e);
                        }));
    }


    private void waitForReindexStarted(ReindexJob job, ActionListener<AcknowledgedResponse> listener) {
        persistentTasksService.waitForPersistentTaskCondition(job.getId(), Objects::nonNull, job.getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskListener<ReindexJob>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<ReindexJob> task) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchException("Creation of task for Reindex Job ID ["
                                + job.getId() + "] timed out after [" + timeout + "]"));
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(PutReindexJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
