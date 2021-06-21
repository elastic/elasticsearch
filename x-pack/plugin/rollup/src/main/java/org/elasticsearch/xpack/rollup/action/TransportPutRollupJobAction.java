/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.rollup.Rollup;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;

public class TransportPutRollupJobAction extends AcknowledgedTransportMasterNodeAction<PutRollupJobAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportPutRollupJobAction.class);

    private final PersistentTasksService persistentTasksService;
    private final Client client;
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportPutRollupJobAction.class);

    @Inject
    public TransportPutRollupJobAction(TransportService transportService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       ClusterService clusterService,
                                       PersistentTasksService persistentTasksService, Client client) {
        super(PutRollupJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutRollupJobAction.Request::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, PutRollupJobAction.Request request, ClusterState clusterState,
                                   ActionListener<AcknowledgedResponse> listener) {
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);
        checkForDeprecatedTZ(request);

        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest()
            .indices(request.getConfig().getIndexPattern())
            .fields(request.getConfig().getAllFields().toArray(new String[0]));
        fieldCapsRequest.setParentTask(clusterService.localNode().getId(), task.getId());

        client.fieldCaps(fieldCapsRequest, listener.delegateFailure((l, fieldCapabilitiesResponse) -> {
            ActionRequestValidationException validationException = request.validateMappings(fieldCapabilitiesResponse.get());
            if (validationException != null) {
                l.onFailure(validationException);
                return;
            }

            RollupJob job = createRollupJob(request.getConfig(), threadPool);
            createIndex(job, l, persistentTasksService, client, logger);
        }));
    }

    static void checkForDeprecatedTZ(PutRollupJobAction.Request request) {
        String timeZone = request.getConfig().getGroupConfig().getDateHistogram().getTimeZone();
        String modernTZ = DateUtils.DEPRECATED_LONG_TIMEZONES.get(timeZone);
        if (modernTZ != null) {
            deprecationLogger.deprecate(DeprecationCategory.PARSING, "deprecated_timezone",
                "Creating Rollup job [" + request.getConfig().getId() + "] with timezone ["
                    + timeZone + "], but [" + timeZone + "] has been deprecated by the IANA.  Use [" + modernTZ +"] instead.");
        }
    }

    private static RollupJob createRollupJob(RollupJobConfig config, ThreadPool threadPool) {
        // ensure we only filter for the allowed headers
        Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());
        return new RollupJob(config, filteredHeaders);
    }

    static void createIndex(RollupJob job, ActionListener<AcknowledgedResponse> listener,
                            PersistentTasksService persistentTasksService, Client client, Logger logger) {

        CreateIndexRequest request = new CreateIndexRequest(job.getConfig().getRollupIndex());
        try {
            XContentBuilder mapping = createMappings(job.getConfig());
            request.source(mapping);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        client.execute(CreateIndexAction.INSTANCE, request,
                ActionListener.wrap(createIndexResponse -> startPersistentTask(job, listener, persistentTasksService), e -> {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.debug("Rolled index already exists for rollup job [" + job.getConfig().getId() + "], updating metadata.");
                        updateMapping(job, listener, persistentTasksService, client, logger);
                    } else {
                        String msg = "Could not create index for rollup job [" + job.getConfig().getId() + "]";
                        logger.error(msg);
                        listener.onFailure(new RuntimeException(msg, e));
                    }
                }));
    }

    private static XContentBuilder createMappings(RollupJobConfig config) throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
                .startObject("mappings")
                    .startObject("_doc")
                        .startObject("_meta")
                            .field(Rollup.ROLLUP_TEMPLATE_VERSION_FIELD, Version.CURRENT.toString())
                            .startObject("_rollup")
                                .field(config.getId(), config)
                            .endObject()
                        .endObject()
                        .startArray("dynamic_templates")
                            .startObject()
                                .startObject("strings")
                                    .field("match_mapping_type", "string")
                                    .startObject("mapping")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                            .startObject()
                                .startObject("date_histograms")
                                    .field("path_match", "*.date_histogram.timestamp")
                                    .startObject("mapping")
                                        .field("type", "date")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endArray()
                    .endObject()
                .endObject()
            .endObject();
    }

    @SuppressWarnings("unchecked")
    static void updateMapping(RollupJob job, ActionListener<AcknowledgedResponse> listener,
                              PersistentTasksService persistentTasksService, Client client, Logger logger) {

        final String indexName = job.getConfig().getRollupIndex();

        CheckedConsumer<GetMappingsResponse, Exception> getMappingResponseHandler = getMappingResponse -> {
            MappingMetadata mappings = getMappingResponse.getMappings().get(indexName);
            Object m = mappings.getSourceAsMap().get("_meta");
            if (m == null) {
                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected " +
                    "to find _meta key in mapping of rollup index [" + indexName + "] but not found).";
                logger.error(msg);
                listener.onFailure(new RuntimeException(msg));
                return;
            }

            Map<String, Object> metadata = (Map<String, Object>) m;
            if (metadata.get(RollupField.ROLLUP_META) == null) {
                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected " +
                    "to find rollup meta key [" + RollupField.ROLLUP_META + "] in mapping of rollup index ["
                    + indexName + "] but not found).";
                logger.error(msg);
                listener.onFailure(new RuntimeException(msg));
                return;
            }

            Map<String, Object> rollupMeta = (Map<String, Object>)((Map<String, Object>) m).get(RollupField.ROLLUP_META);

            String stringVersion = (String)((Map<String, Object>) m).get(Rollup.ROLLUP_TEMPLATE_VERSION_FIELD);
            if (stringVersion == null) {
                listener.onFailure(new IllegalStateException("Could not determine version of existing rollup metadata for index ["
                    + indexName + "]"));
                return;
            }

            if (rollupMeta.get(job.getConfig().getId()) != null) {
                String msg = "Cannot create rollup job [" + job.getConfig().getId()
                        + "] because job was previously created (existing metadata).";
                logger.error(msg);
                listener.onFailure(new ElasticsearchStatusException(msg, RestStatus.CONFLICT));
                return;
            }

            rollupMeta.put(job.getConfig().getId(), job.getConfig());
            metadata.put(RollupField.ROLLUP_META, rollupMeta);
            Map<String, Object> newMapping = mappings.getSourceAsMap();
            newMapping.put("_meta", metadata);
            PutMappingRequest request = new PutMappingRequest(indexName);
            request.source(newMapping);
            client.execute(PutMappingAction.INSTANCE, request,
                    ActionListener.wrap(putMappingResponse -> startPersistentTask(job, listener, persistentTasksService),
                            listener::onFailure));
        };

        GetMappingsRequest request = new GetMappingsRequest();
        client.execute(GetMappingsAction.INSTANCE, request, ActionListener.wrap(getMappingResponseHandler,
                e -> {
                    String msg = "Could not update mappings for rollup job [" + job.getConfig().getId() + "]";
                    logger.error(msg);
                    listener.onFailure(new RuntimeException(msg, e));
                }));
    }

    static void startPersistentTask(RollupJob job, ActionListener<AcknowledgedResponse> listener,
                                    PersistentTasksService persistentTasksService) {
        assertNoAuthorizationHeader(job.getHeaders());
        persistentTasksService.sendStartRequest(job.getConfig().getId(), RollupField.TASK_NAME, job,
                ActionListener.wrap(
                        rollupConfigPersistentTask -> waitForRollupStarted(job, listener, persistentTasksService),
                        e -> {
                            if (e instanceof ResourceAlreadyExistsException) {
                                e = new ElasticsearchStatusException("Cannot create job [" + job.getConfig().getId() +
                                        "] because it has already been created (task exists)", RestStatus.CONFLICT, e);
                            }
                            listener.onFailure(e);
                        }));
    }


    private static void waitForRollupStarted(RollupJob job, ActionListener<AcknowledgedResponse> listener,
                                             PersistentTasksService persistentTasksService) {
        persistentTasksService.waitForPersistentTaskCondition(job.getConfig().getId(), Objects::nonNull, job.getConfig().getTimeout(),
                new PersistentTasksService.WaitForPersistentTaskListener<RollupJob>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetadata.PersistentTask<RollupJob> task) {
                        listener.onResponse(AcknowledgedResponse.TRUE);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchException("Creation of task for Rollup Job ID ["
                                + job.getConfig().getId() + "] timed out after [" + timeout + "]"));
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(PutRollupJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
