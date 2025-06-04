/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;
import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_KEY;
import static org.elasticsearch.xpack.rollup.Rollup.DEPRECATION_MESSAGE;

public class TransportPutRollupJobAction extends AcknowledgedTransportMasterNodeAction<PutRollupJobAction.Request> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutRollupJobAction.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(TransportPutRollupJobAction.class);
    private static final XContentParserConfiguration PARSER_CONFIGURATION = XContentParserConfiguration.EMPTY.withFiltering(
        null,
        Set.of("_doc._meta._rollup"),
        null,
        false
    );

    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportPutRollupJobAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        Client client
    ) {
        super(
            PutRollupJobAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutRollupJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.persistentTasksService = persistentTasksService;
        this.client = client;

    }

    @Override
    protected void masterOperation(
        Task task,
        PutRollupJobAction.Request request,
        ClusterState clusterState,
        ActionListener<AcknowledgedResponse> listener
    ) {
        DEPRECATION_LOGGER.warn(DeprecationCategory.API, DEPRECATION_KEY, DEPRECATION_MESSAGE);
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);
        checkForDeprecatedTZ(request);

        int numberOfCurrentRollupJobs = RollupUsageTransportAction.findNumberOfRollupJobs(clusterState);
        if (numberOfCurrentRollupJobs == 0) {
            try {
                boolean hasRollupIndices = hasRollupIndices(clusterState.getMetadata());
                if (hasRollupIndices == false) {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "new rollup jobs are not allowed in clusters that don't have any rollup usage, since rollup has been deprecated"
                        )
                    );
                    return;
                }
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
        }

        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices(request.indices())
            .fields(request.getConfig().getAllFields().toArray(new String[0]));
        fieldCapsRequest.setParentTask(clusterService.localNode().getId(), task.getId());

        client.fieldCaps(fieldCapsRequest, listener.delegateFailure((l, fieldCapabilitiesResponse) -> {
            ActionRequestValidationException validationException = request.validateMappings(fieldCapabilitiesResponse.get());
            if (validationException != null) {
                l.onFailure(validationException);
                return;
            }

            RollupJob job = createRollupJob(request.getConfig(), threadPool);
            createIndex(job, l, persistentTasksService, client, LOGGER);
        }));
    }

    static void checkForDeprecatedTZ(PutRollupJobAction.Request request) {
        String timeZone = request.getConfig().getGroupConfig().getDateHistogram().getTimeZone();
        String modernTZ = DateUtils.DEPRECATED_LONG_TIMEZONES.get(timeZone);
        if (modernTZ != null) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.PARSING,
                "deprecated_timezone",
                "Creating Rollup job ["
                    + request.getConfig().getId()
                    + "] with timezone ["
                    + timeZone
                    + "], but ["
                    + timeZone
                    + "] has been deprecated by the IANA.  Use ["
                    + modernTZ
                    + "] instead."
            );
        }
    }

    private RollupJob createRollupJob(RollupJobConfig config, ThreadPool threadPool) {
        // ensure we only filter for the allowed headers
        Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
            threadPool.getThreadContext(),
            clusterService.state()
        );
        return new RollupJob(config, filteredHeaders);
    }

    static void createIndex(
        RollupJob job,
        ActionListener<AcknowledgedResponse> listener,
        PersistentTasksService persistentTasksService,
        Client client,
        Logger logger
    ) {

        CreateIndexRequest request = new CreateIndexRequest(job.getConfig().getRollupIndex());
        try {
            XContentBuilder mapping = createMappings(job.getConfig());
            request.source(mapping);
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        client.execute(
            TransportCreateIndexAction.TYPE,
            request,
            ActionListener.wrap(createIndexResponse -> startPersistentTask(job, listener, persistentTasksService), e -> {
                if (e instanceof ResourceAlreadyExistsException) {
                    logger.debug("Rolled index already exists for rollup job [" + job.getConfig().getId() + "], updating metadata.");
                    updateMapping(job, listener, persistentTasksService, client, logger, request.masterNodeTimeout());
                } else {
                    String msg = "Could not create index for rollup job [" + job.getConfig().getId() + "]";
                    logger.error(msg);
                    listener.onFailure(new RuntimeException(msg, e));
                }
            })
        );
    }

    static XContentBuilder createMappings(RollupJobConfig config) throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("mappings")
            .startObject("_doc")
            .startObject("_meta")
            .field("rollup-version", "") // empty string to remain backwards compatible
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
    static void updateMapping(
        RollupJob job,
        ActionListener<AcknowledgedResponse> listener,
        PersistentTasksService persistentTasksService,
        Client client,
        Logger logger,
        TimeValue masterTimeout
    ) {

        final String indexName = job.getConfig().getRollupIndex();

        CheckedConsumer<GetMappingsResponse, Exception> getMappingResponseHandler = getMappingResponse -> {
            MappingMetadata mappings = getMappingResponse.getMappings().get(indexName);
            Object m = mappings.getSourceAsMap().get("_meta");
            if (m == null) {
                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected "
                    + "to find _meta key in mapping of rollup index ["
                    + indexName
                    + "] but not found).";
                logger.error(msg);
                listener.onFailure(new RuntimeException(msg));
                return;
            }

            Map<String, Object> metadata = (Map<String, Object>) m;
            if (metadata.get(RollupField.ROLLUP_META) == null) {
                String msg = "Rollup data cannot be added to existing indices that contain non-rollup data (expected "
                    + "to find rollup meta key ["
                    + RollupField.ROLLUP_META
                    + "] in mapping of rollup index ["
                    + indexName
                    + "] but not found).";
                logger.error(msg);
                listener.onFailure(new RuntimeException(msg));
                return;
            }

            Map<String, Object> rollupMeta = (Map<String, Object>) ((Map<String, Object>) m).get(RollupField.ROLLUP_META);

            if (rollupMeta.get(job.getConfig().getId()) != null) {
                String msg = "Cannot create rollup job ["
                    + job.getConfig().getId()
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
            client.execute(
                TransportPutMappingAction.TYPE,
                request,
                ActionListener.wrap(putMappingResponse -> startPersistentTask(job, listener, persistentTasksService), listener::onFailure)
            );
        };

        GetMappingsRequest request = new GetMappingsRequest(masterTimeout);
        client.execute(GetMappingsAction.INSTANCE, request, ActionListener.wrap(getMappingResponseHandler, e -> {
            String msg = "Could not update mappings for rollup job [" + job.getConfig().getId() + "]";
            logger.error(msg);
            listener.onFailure(new RuntimeException(msg, e));
        }));
    }

    static void startPersistentTask(
        RollupJob job,
        ActionListener<AcknowledgedResponse> listener,
        PersistentTasksService persistentTasksService
    ) {
        assertNoAuthorizationHeader(job.getHeaders());
        persistentTasksService.sendStartRequest(
            job.getConfig().getId(),
            RollupField.TASK_NAME,
            job,
            TimeValue.THIRTY_SECONDS /* TODO should this be configurable? longer by default? infinite? */,
            ActionListener.wrap(rollupConfigPersistentTask -> waitForRollupStarted(job, listener, persistentTasksService), e -> {
                if (e instanceof ResourceAlreadyExistsException) {
                    e = new ElasticsearchStatusException(
                        "Cannot create job [" + job.getConfig().getId() + "] because it has already been created (task exists)",
                        RestStatus.CONFLICT,
                        e
                    );
                }
                listener.onFailure(e);
            })
        );
    }

    private static void waitForRollupStarted(
        RollupJob job,
        ActionListener<AcknowledgedResponse> listener,
        PersistentTasksService persistentTasksService
    ) {
        persistentTasksService.waitForPersistentTaskCondition(
            job.getConfig().getId(),
            Objects::nonNull,
            job.getConfig().getTimeout(),
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
                    listener.onFailure(
                        new ElasticsearchException(
                            "Creation of task for Rollup Job ID [" + job.getConfig().getId() + "] timed out after [" + timeout + "]"
                        )
                    );
                }
            }
        );
    }

    static boolean hasRollupIndices(Metadata metadata) throws IOException {
        // Sniffing logic instead of invoking sourceAsMap(), which would materialize the entire mapping as map of maps.
        for (var imd : metadata.getProject()) {
            if (imd.mapping() == null) {
                continue;
            }

            try (var parser = XContentHelper.createParser(PARSER_CONFIGURATION, imd.mapping().source().compressedReference())) {
                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    if ("_doc".equals(parser.nextFieldName())) {
                        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                            if ("_meta".equals(parser.nextFieldName())) {
                                if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                                    if ("_rollup".equals(parser.nextFieldName())) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected ClusterBlockException checkBlock(PutRollupJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
