/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.stats.MappingVisitor;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.rollup.action.RollupShardPersistentTaskState;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.TimeSeriesParams.TIME_SERIES_METRIC_PARAM;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;

/**
 * The master rollup action that coordinates
 *  -  creating the rollup index
 *  -  instantiating {@link RollupShardIndexer}s to index rollup documents
 *  -  cleaning up state
 */
public class TransportDownsampleAction extends AcknowledgedTransportMasterNodeAction<DownsampleAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportDownsampleAction.class);

    private final Client client;
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<RollupClusterStateUpdateTask> taskQueue;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final ThreadContext threadContext;
    private final PersistentTasksService persistentTasksService;

    private static final Set<String> FORBIDDEN_SETTINGS = Set.of(
        IndexSettings.DEFAULT_PIPELINE.getKey(),
        IndexSettings.FINAL_PIPELINE.getKey(),
        IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()
    );

    private static final Set<String> OVERRIDE_SETTINGS = Set.of(DataTier.TIER_PREFERENCE_SETTING.getKey());

    /**
     * This is the cluster state task executor for cluster state update actions.
     */
    private static final SimpleBatchedExecutor<RollupClusterStateUpdateTask, Void> STATE_UPDATE_TASK_EXECUTOR =
        new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(RollupClusterStateUpdateTask task, ClusterState clusterState) throws Exception {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(RollupClusterStateUpdateTask task, Void unused) {
                task.listener.onResponse(AcknowledgedResponse.TRUE);
            }
        };

    @Inject
    public TransportDownsampleAction(
        Client client,
        IndicesService indicesService,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        MetadataCreateIndexService metadataCreateIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        PersistentTasksService persistentTasksService
    ) {
        super(
            DownsampleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DownsampleAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.threadContext = threadPool.getThreadContext();
        this.taskQueue = clusterService.createTaskQueue("rollup", Priority.URGENT, STATE_UPDATE_TASK_EXECUTOR);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DownsampleAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String sourceIndexName = request.getSourceIndex();

        final IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl != null) {
            final IndicesAccessControl.IndexAccessControl indexPermissions = indicesAccessControl.getIndexPermissions(sourceIndexName);
            if (indexPermissions != null) {
                boolean hasDocumentLevelPermissions = indexPermissions.getDocumentPermissions().hasDocumentLevelPermissions();
                boolean hasFieldLevelSecurity = indexPermissions.getFieldPermissions().hasFieldLevelSecurity();
                if (hasDocumentLevelPermissions || hasFieldLevelSecurity) {
                    listener.onFailure(
                        new ElasticsearchException(
                            "Rollup forbidden for index [" + sourceIndexName + "] with document level or field level security settings."
                        )
                    );
                    return;
                }
            }
        }
        // Assert source index exists
        IndexMetadata sourceIndexMetadata = state.getMetadata().index(sourceIndexName);
        if (sourceIndexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(sourceIndexName));
            return;
        }

        // Assert source index is a time_series index
        if (IndexSettings.MODE.get(sourceIndexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
            listener.onFailure(
                new ElasticsearchException(
                    "Rollup requires setting ["
                        + IndexSettings.MODE.getKey()
                        + "="
                        + IndexMode.TIME_SERIES
                        + "] for index ["
                        + sourceIndexName
                        + "]"
                )
            );
            return;
        }

        // Assert source index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndexName) == false) {
            listener.onFailure(
                new ElasticsearchException(
                    "Rollup requires setting [" + IndexMetadata.SETTING_BLOCKS_WRITE + " = true] for index [" + sourceIndexName + "]"
                )
            );
            return;
        }

        final String rollupIndexName = request.getTargetIndex();
        // Assert rollup index does not exist
        MetadataCreateIndexService.validateIndexName(rollupIndexName, state);

        // Rollup will perform the following tasks:
        // 1. Extract source index mappings
        // 2. Extract rollup config from index mappings
        // 3. Create the rollup index
        // 4. Run rollup indexer
        // 5. Make rollup index read-only and set replicas
        // 6. Refresh rollup index
        // 7. Mark rollup index as "completed successfully"
        // 8. Force-merge the rollup index to a single segment
        // At any point if there is an issue, delete the rollup index

        // 1. Extract source index mappings
        final TaskId parentTask = new TaskId(clusterService.localNode().getId(), task.getId());
        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(sourceIndexName);
        getMappingsRequest.setParentTask(parentTask);
        client.admin().indices().getMappings(getMappingsRequest, ActionListener.wrap(getMappingsResponse -> {
            final Map<String, Object> sourceIndexMappings = getMappingsResponse.mappings()
                .entrySet()
                .stream()
                .filter(entry -> sourceIndexName.equals(entry.getKey()))
                .findFirst()
                .map(mappingMetadata -> mappingMetadata.getValue().sourceAsMap())
                .orElseThrow(() -> new IllegalArgumentException("No mapping found for rollup source index [" + sourceIndexName + "]"));

            // 2. Extract rollup config from index mappings
            final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(sourceIndexMetadata);
            final CompressedXContent sourceIndexCompressedXContent = new CompressedXContent(sourceIndexMappings);
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, sourceIndexCompressedXContent, MapperService.MergeReason.INDEX_TEMPLATE);

            // Validate downsampling interval
            validateDownsamplingInterval(mapperService, request.getDownsampleConfig());

            final List<String> dimensionFields = new ArrayList<>();
            final List<String> metricFields = new ArrayList<>();
            final List<String> labelFields = new ArrayList<>();
            final TimeseriesFieldTypeHelper helper = new TimeseriesFieldTypeHelper.Builder(mapperService).build(
                request.getDownsampleConfig().getTimestampField()
            );
            MappingVisitor.visitMapping(sourceIndexMappings, (field, mapping) -> {
                if (helper.isTimeSeriesDimension(field, mapping)) {
                    dimensionFields.add(field);
                } else if (helper.isTimeSeriesMetric(field, mapping)) {
                    metricFields.add(field);
                } else if (helper.isTimeSeriesLabel(field, mapping)) {
                    labelFields.add(field);
                }
            });

            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (dimensionFields.isEmpty()) {
                validationException.addValidationError("Index [" + sourceIndexName + "] does not contain any dimension fields");
            }

            if (validationException.validationErrors().isEmpty() == false) {
                listener.onFailure(validationException);
                return;
            }

            final String mapping;
            try {
                mapping = createRollupIndexMapping(helper, request.getDownsampleConfig(), mapperService, sourceIndexMappings);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }
            // 3. Create rollup index
            createRollupIndex(rollupIndexName, sourceIndexMetadata, mapping, request, ActionListener.wrap(createIndexResp -> {
                if (createIndexResp.isAcknowledged()) {
                    // 3. Rollup index created. Run rollup indexer persistent task on each shard.
                    final int numberOfShards = sourceIndexMetadata.getNumberOfShards();
                    final Index sourceIndex = sourceIndexMetadata.getIndex();
                    // NOTE: before we set the number of replicas to 0, as a result here we are
                    // only dealing with primary shards.
                    for (int shardNum = 0; shardNum < numberOfShards; shardNum++) {
                        final ShardId shardId = new ShardId(sourceIndex, shardNum);
                        final String persistentRollupTaskId = createRollupShardTaskId(
                            rollupIndexName,
                            shardId,
                            request.getDownsampleConfig().getInterval()
                        );
                        final RollupShardTaskParams params = createRollupShardTaskParams(
                            request.getDownsampleConfig(),
                            sourceIndexMetadata,
                            rollupIndexName,
                            metricFields,
                            labelFields,
                            shardId
                        );
                        persistentTasksService.sendStartRequest(
                            persistentRollupTaskId,
                            RollupShardTask.TASK_NAME,
                            params,
                            ActionListener.wrap(
                                startedTask -> persistentTasksService.waitForPersistentTaskCondition(startedTask.getId(), runningTask -> {
                                    RollupShardPersistentTaskState runningPersistentTaskState = (RollupShardPersistentTaskState) runningTask
                                        .getState();
                                    return runningPersistentTaskState != null && runningPersistentTaskState.done();
                                },
                                    request.getDownsampleConfig().getTimeout(),
                                    new PersistentTasksService.WaitForPersistentTaskListener<>() {
                                        @Override
                                        public void onResponse(
                                            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTask
                                        ) {
                                            // 4. Make rollup index read-only and set the correct number of replicas
                                            final Settings.Builder settings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true);
                                            // Number of replicas had been previously set to 0 to speed up index population
                                            if (sourceIndexMetadata.getNumberOfReplicas() > 0) {
                                                settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceIndexMetadata.getNumberOfReplicas());
                                            }
                                            // Setting index.hidden has been initially set to true. We revert this to the value of the
                                            // source index
                                            if (sourceIndexMetadata.isHidden() == false) {
                                                if (sourceIndexMetadata.getSettings().keySet().contains(IndexMetadata.SETTING_INDEX_HIDDEN)) {
                                                    settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, false);
                                                } else {
                                                    settings.putNull(IndexMetadata.SETTING_INDEX_HIDDEN);
                                                }
                                            }
                                            UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(settings.build(), rollupIndexName);
                                            updateSettingsReq.setParentTask(parentTask);
                                            client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                                                if (updateSettingsResponse.isAcknowledged()) {
                                                    // 5. Refresh rollup index
                                                    refreshIndex(rollupIndexName, parentTask, ActionListener.wrap(refreshIndexResponse -> {
                                                        if (refreshIndexResponse.getFailedShards() == 0) {
                                                            // 6. Mark rollup index as "completed successfully"
                                                            updateRollupMetadata(rollupIndexName, request, ActionListener.wrap(resp -> {
                                                                if (resp.isAcknowledged()) {
                                                                    // 7. Force-merge the rollup index to a single segment
                                                                    forceMergeIndex(
                                                                        rollupIndexName,
                                                                        parentTask,
                                                                        ActionListener.wrap(mergeIndexResp -> listener.onResponse(AcknowledgedResponse.TRUE), t -> {
                                                                            /*
                                                                             * At this point rollup has been created
                                                                             * successfully even force merge fails.
                                                                             * So, we should not fail the rollup operation
                                                                             */
                                                                            logger.error("Failed to force-merge " + "rollup index [" + rollupIndexName + "]", t);
                                                                            listener.onResponse(AcknowledgedResponse.TRUE);
                                                                        })
                                                                    );
                                                                }
                                                            }, listener::onFailure));
                                                        }
                                                    }, listener::onFailure));
                                                }
                                            }, listener::onFailure));
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            logger.error("error while starting downsampling persistent task", e);
                                            listener.onFailure(e);
                                        }
                                    }
                                ),
                                e -> {
                                    if (e instanceof ResourceAlreadyExistsException) {
                                        waitForExistingTask(
                                            request,
                                            listener,
                                            persistentRollupTaskId
                                        );
                                    } else {
                                        listener.onFailure(e);
                                    }
                                }
                            )
                        );
                    }
                } else {
                    listener.onFailure(new ElasticsearchException("Failed to create rollup index [" + rollupIndexName + "]"));
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void waitForExistingTask(
        final DownsampleAction.Request request,
        final ActionListener<AcknowledgedResponse> listener,
        final String persistentRollupTaskId
    ) {
        logger.info("Downsampling persistent task [" + persistentRollupTaskId + "] already exists. Waiting...");
        persistentTasksService.waitForPersistentTasksCondition(existingTask -> {
            PersistentTasksCustomMetadata.PersistentTask<?> existingPersistentTask = existingTask.getTask(persistentRollupTaskId);
            final RollupShardPersistentTaskState existingPersistentTaskState = (RollupShardPersistentTaskState) existingPersistentTask
                .getState();
            return existingPersistentTaskState.done();
        },
            request.getDownsampleConfig().getTimeout(),
            ActionListener.wrap(
                response -> logger.info("Downsampling task [" + persistentRollupTaskId + " completed"),
                listener::onFailure
            )
        );
    }

    private static RollupShardTaskParams createRollupShardTaskParams(
        final DownsampleConfig downsampleConfig,
        final IndexMetadata sourceIndexMetadata,
        final String rollupIndexName,
        final List<String> metricFields,
        final List<String> labelFields,
        final ShardId shardId
    ) {
        return new RollupShardTaskParams(
            downsampleConfig,
            rollupIndexName,
            parseTimestamp(sourceIndexMetadata, IndexSettings.TIME_SERIES_START_TIME),
            parseTimestamp(sourceIndexMetadata, IndexSettings.TIME_SERIES_END_TIME),
            shardId,
            metricFields.toArray(new String[0]),
            labelFields.toArray(new String[0])
        );
    }

    private static long parseTimestamp(final IndexMetadata sourceIndexMetadata, final Setting<Instant> timestampSetting) {
        return OffsetDateTime.parse(sourceIndexMetadata.getSettings().get(timestampSetting.getKey()), DateTimeFormatter.ISO_DATE_TIME)
            .toInstant()
            .toEpochMilli();
    }

    private static String createRollupShardTaskId(
        final String rollupIndexName,
        final ShardId shardId,
        final DateHistogramInterval interval
    ) {
        return DOWNSAMPLED_INDEX_PREFIX + rollupIndexName + "-" + shardId.id() + "-" + interval;
    }

    @Override
    protected ClusterBlockException checkBlock(DownsampleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * This method creates the mapping for the rollup index, based on the
     * mapping (dimensions and metrics) from the source index, as well as the
     * rollup configuration.
     *
     * @param config the rollup configuration
     * @param sourceIndexMappings a map with the source index mapping
     * @return the mapping of the rollup index
     */
    public static String createRollupIndexMapping(
        final TimeseriesFieldTypeHelper helper,
        final DownsampleConfig config,
        final MapperService mapperService,
        final Map<String, Object> sourceIndexMappings
    ) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        addDynamicTemplates(builder);

        builder.startObject("properties");

        addTimestampField(config, sourceIndexMappings, builder);
        addMetricFields(helper, sourceIndexMappings, builder);

        builder.endObject(); // match initial startObject
        builder.endObject(); // match startObject("properties")

        final CompressedXContent rollupDiffXContent = CompressedXContent.fromJSON(
            XContentHelper.convertToJson(BytesReference.bytes(builder), false, XContentType.JSON)
        );
        return mapperService.merge(MapperService.SINGLE_MAPPING_NAME, rollupDiffXContent, MapperService.MergeReason.INDEX_TEMPLATE)
            .mappingSource()
            .uncompressed()
            .utf8ToString();
    }

    private static void addMetricFields(
        final TimeseriesFieldTypeHelper helper,
        final Map<String, Object> sourceIndexMappings,
        final XContentBuilder builder
    ) {
        MappingVisitor.visitMapping(sourceIndexMappings, (field, mapping) -> {
            if (helper.isTimeSeriesMetric(field, mapping)) {
                try {
                    addMetricFieldMapping(builder, field, mapping);
                } catch (IOException e) {
                    throw new ElasticsearchException("Error while adding metric for field [" + field + "]");
                }
            }
        });
    }

    private static void addTimestampField(
        final DownsampleConfig config,
        Map<String, Object> sourceIndexMappings,
        final XContentBuilder builder
    ) throws IOException {
        final String timestampField = config.getTimestampField();
        final String dateIntervalType = config.getIntervalType();
        final String dateInterval = config.getInterval().toString();
        final String timezone = config.getTimeZone();
        builder.startObject(timestampField);

        MappingVisitor.visitMapping(sourceIndexMappings, (field, mapping) -> {
            try {
                if (timestampField.equals(field)) {
                    final String timestampType = String.valueOf(mapping.get("type"));
                    builder.field("type", timestampType != null ? timestampType : DateFieldMapper.CONTENT_TYPE);
                    if (mapping.get("format") != null) {
                        builder.field("format", mapping.get("format"));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Unable to create timestamp field mapping for field [" + timestampField + "]", e);
            }
        });

        builder.startObject("meta")
            .field(dateIntervalType, dateInterval)
            .field(DownsampleConfig.TIME_ZONE, timezone)
            .endObject()
            .endObject();
    }

    private static void addMetricFieldMapping(final XContentBuilder builder, final String field, final Map<String, ?> fieldProperties)
        throws IOException {
        final TimeSeriesParams.MetricType metricType = TimeSeriesParams.MetricType.fromString(
            fieldProperties.get(TIME_SERIES_METRIC_PARAM).toString()
        );
        if (metricType == TimeSeriesParams.MetricType.COUNTER) {
            // For counters, we keep the same field type, because they store
            // only one value (the last value of the counter)
            builder.startObject(field).field("type", fieldProperties.get("type")).field(TIME_SERIES_METRIC_PARAM, metricType).endObject();
        } else {
            final List<String> supportedAggs = List.of(metricType.supportedAggs());
            // We choose max as the default metric
            final String defaultMetric = supportedAggs.contains("max") ? "max" : supportedAggs.get(0);
            builder.startObject(field)
                .field("type", AggregateDoubleMetricFieldMapper.CONTENT_TYPE)
                .stringListField(AggregateDoubleMetricFieldMapper.Names.METRICS, supportedAggs)
                .field(AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC, defaultMetric)
                .field(TIME_SERIES_METRIC_PARAM, metricType)
                .endObject();
        }
    }

    private static void validateDownsamplingInterval(MapperService mapperService, DownsampleConfig config) {
        MappedFieldType timestampFieldType = mapperService.fieldType(config.getTimestampField());
        assert timestampFieldType != null : "Cannot find timestamp field [" + config.getTimestampField() + "] in the mapping";
        ActionRequestValidationException e = new ActionRequestValidationException();

        Map<String, String> meta = timestampFieldType.meta();
        if (meta.isEmpty() == false) {
            String interval = meta.get(config.getIntervalType());
            if (interval != null) {
                try {
                    DownsampleConfig sourceConfig = new DownsampleConfig(new DateHistogramInterval(interval), config.getTimeout());
                    DownsampleConfig.validateSourceAndTargetIntervals(sourceConfig, config);
                } catch (IllegalArgumentException exception) {
                    e.addValidationError("Source index is a downsampled index. " + exception.getMessage());
                }
            }

            // Validate that timezones match
            String sourceTimezone = meta.get(DownsampleConfig.TIME_ZONE);
            if (sourceTimezone != null && sourceTimezone.equals(config.getTimeZone()) == false) {
                e.addValidationError(
                    "Source index is a downsampled index. Downsampling timezone ["
                        + config.getTimeZone()
                        + "] cannot be different than the source index timezone ["
                        + sourceTimezone
                        + "]."
                );
            }

            if (e.validationErrors().isEmpty() == false) {
                throw e;
            }
        }
    }

    /**
     * Copy index settings from the source index to the rollup index. Settings that
     * have already been set in the rollup index will not be overridden.
     */
    static IndexMetadata.Builder copyIndexMetadata(
        final IndexMetadata sourceIndexMetadata,
        final IndexMetadata rollupIndexMetadata,
        final IndexScopedSettings indexScopedSettings
    ) {
        // Copy index settings from the source index, but do not override the settings
        // that already have been set in the rollup index
        final Settings.Builder targetSettings = Settings.builder().put(rollupIndexMetadata.getSettings());
        for (final String key : sourceIndexMetadata.getSettings().keySet()) {
            final Setting<?> setting = indexScopedSettings.get(key);
            if (setting == null) {
                assert indexScopedSettings.isPrivateSetting(key) : "expected [" + key + "] to be private but it was not";
            } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                // we leverage the NotCopyableOnResize setting property for rollup, because
                // the same rules with resize apply
                continue;
            }
            // Do not copy index settings which are valid for the source index but not for the target index
            if (FORBIDDEN_SETTINGS.contains(key)) {
                continue;
            }

            if (OVERRIDE_SETTINGS.contains(key)) {
                targetSettings.put(key, sourceIndexMetadata.getSettings().get(key));
            }
            // Do not override settings that have already been set in the rollup index.
            if (targetSettings.keys().contains(key)) {
                continue;
            }

            targetSettings.copy(key, sourceIndexMetadata.getSettings());
        }

        /*
         * Add the source index name and UUID to the rollup index metadata.
         * If the source index is a rollup index, we will add the name and UUID
         * of the first index that we initially rolled up.
         */
        if (IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.exists(sourceIndexMetadata.getSettings()) == false
            || IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.exists(sourceIndexMetadata.getSettings()) == false) {
            Index sourceIndex = sourceIndexMetadata.getIndex();
            targetSettings.put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.getKey(), sourceIndex.getName())
                .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.getKey(), sourceIndex.getUUID());
        }

        return IndexMetadata.builder(rollupIndexMetadata).settings(targetSettings);
    }

    /**
     * Configure the dynamic templates to always map strings to the keyword field type.
     */
    private static void addDynamicTemplates(final XContentBuilder builder) throws IOException {
        builder.startArray("dynamic_templates")
            .startObject()
            .startObject("strings")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray();
    }

    private void createRollupIndex(
        String rollupIndexName,
        IndexMetadata sourceIndexMetadata,
        String mapping,
        DownsampleAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        /*
         * When creating the rollup index, we copy the index.number_of_shards from source index,
         * and we set the index.number_of_replicas to 0, to avoid replicating the index being built.
         * Also, we set the index.refresh_interval to -1.
         * We will set the correct number of replicas and refresh the index later.
         *
         * We should note that there is a risk of losing a node during the rollup process. In this
         * case rollup will fail.
         */
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, sourceIndexMetadata.getNumberOfShards())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.STARTED);
        if (sourceIndexMetadata.getSettings().hasValue(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())) {
            builder.put(
                MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(),
                sourceIndexMetadata.getSettings().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey())
            );
        }

        CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
            "rollup",
            rollupIndexName,
            rollupIndexName
        ).settings(builder.build()).mappings(mapping);
        var delegate = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        taskQueue.submitTask("create-rollup-index [" + rollupIndexName + "]", new RollupClusterStateUpdateTask(listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return metadataCreateIndexService.applyCreateIndexRequest(
                    currentState,
                    createIndexClusterStateUpdateRequest,
                    true,
                    // Copy index metadata from source index to rollup index
                    (builder, rollupIndexMetadata) -> builder.put(
                        copyIndexMetadata(sourceIndexMetadata, rollupIndexMetadata, indexScopedSettings)
                    ),
                    delegate.reroute()
                );
            }
        }, request.masterNodeTimeout());
    }

    private void updateRollupMetadata(
        String rollupIndexName,
        DownsampleAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // 6. Mark rollup index as "completed successfully" ("index.rollup.status": "success")
        taskQueue.submitTask("update-rollup-metadata [" + rollupIndexName + "]", new RollupClusterStateUpdateTask(listener) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                Metadata.Builder metadataBuilder = Metadata.builder(metadata);
                Index rollupIndex = metadata.index(rollupIndexName).getIndex();
                IndexMetadata rollupIndexMetadata = metadata.index(rollupIndex);

                metadataBuilder.updateSettings(
                    Settings.builder()
                        .put(rollupIndexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.SUCCESS)
                        .build(),
                    rollupIndexName
                );
                return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
            }
        }, request.masterNodeTimeout());
    }

    private void refreshIndex(String index, TaskId parentTask, ActionListener<RefreshResponse> listener) {
        RefreshRequest request = new RefreshRequest(index);
        request.setParentTask(parentTask);
        client.admin().indices().refresh(request, listener);
    }

    private void forceMergeIndex(String index, TaskId parentTask, ActionListener<ForceMergeResponse> listener) {
        ForceMergeRequest request = new ForceMergeRequest(index);
        request.maxNumSegments(1);
        request.setParentTask(parentTask);
        client.admin().indices().forceMerge(request, listener);
    }

    /**
     * A specialized cluster state update task that always takes a listener handling an
     * AcknowledgedResponse, as all template actions have simple acknowledged yes/no responses.
     */
    private abstract static class RollupClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;

        RollupClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        public abstract ClusterState execute(ClusterState currentState) throws Exception;

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
