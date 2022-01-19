/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Glob;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionGroupConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.elasticsearch.xpack.core.rollup.action.RollupActionRequestValidationException;
import org.elasticsearch.xpack.core.rollup.action.RollupIndexerAction;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricField;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The master rollup action that coordinates
 *  -  creating rollup temporary index
 *  -  calling {@link TransportRollupIndexerAction} to index rollup-ed documents
 *  -  cleaning up state
 */
public class TransportRollupAction extends AcknowledgedTransportMasterNodeAction<RollupAction.Request> {

    public static final String TMP_INDEX_PREFIX = ".rolluptmp-";
    private static final Settings WRITE_BLOCKED_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();

    private final Client client;
    private final ClusterService clusterService;
    private final MetadataCreateIndexService metadataCreateIndexService;

    @Inject
    public TransportRollupAction(
        Client client,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        MetadataCreateIndexService metadataCreateIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RollupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RollupAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    @Override
    protected void masterOperation(
        Task task,
        RollupAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String originalIndexName = request.getSourceIndex();
        IndexMetadata originalIndexMetadata = state.getMetadata().index(originalIndexName);
        if (originalIndexMetadata == null) {
            throw new InvalidIndexNameException(originalIndexName, "rollup origin index metadata missing");
        }

        final String rollupIndexName = request.getRollupIndex();
        validExistsRollupIndex(state, rollupIndexName);

        String tmpIndexName = TMP_INDEX_PREFIX + rollupIndexName;

        FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest().indices(originalIndexName)
            .fields(request.getRollupConfig().getAllFields().toArray(new String[0]));
        fieldCapsRequest.setParentTask(clusterService.localNode().getId(), task.getId());

        // 1. validate Rollup Config against Field Caps
        // 2. create hidden temporary index
        // 3. run rollup indexer
        // 4. make temp index read-only
        // 5. shrink index
        // 6. publish rollup metadata and add rollup index to datastream
        // 7. delete temporary index
        // at any point if there is an issue, then cleanup temp index

        // 1.
        client.fieldCaps(fieldCapsRequest, ActionListener.wrap(fieldCapsResponse -> {
            RollupActionRequestValidationException validationException = new RollupActionRequestValidationException();
            if (fieldCapsResponse.get().size() == 0) {
                validationException.addValidationError(
                    "Could not find any fields in the index [" + originalIndexName + "] that were configured in job"
                );
                listener.onFailure(validationException);
                return;
            }
            // rebuild rollup config, deal wildcard fields
            request.setRollupConfig(rebuildRollupConfig(request.getRollupConfig(), fieldCapsResponse.get()));
            request.getRollupConfig().validateMappings(fieldCapsResponse.get(), validationException);
            if (validationException.validationErrors().size() > 0) {
                listener.onFailure(validationException);
                return;
            }

            final XContentBuilder mapping;
            try {
                mapping = getMapping(request.getRollupConfig(), originalIndexMetadata.mapping());
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }

            // Add the source index name and UUID to the rollup index metadata. If the original index is a rollup index itself,
            // we will add the name and UUID of the raw index that we initially rolled up.
            String sourceIndexName = IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.exists(originalIndexMetadata.getSettings())
                ? IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.get(originalIndexMetadata.getSettings())
                : originalIndexName;
            String sourceIndexUuid = IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.exists(originalIndexMetadata.getSettings())
                ? IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.get(originalIndexMetadata.getSettings())
                : originalIndexMetadata.getIndexUUID();

            CreateIndexClusterStateUpdateRequest createIndexClusterStateUpdateRequest = new CreateIndexClusterStateUpdateRequest(
                "rollup",
                tmpIndexName,
                tmpIndexName
            ).settings(getSettings(originalIndexMetadata, request.getRollupConfig()))
                .mappings(XContentHelper.convertToJson(BytesReference.bytes(mapping), false, XContentType.JSON));

            // 2.
            clusterService.submitStateUpdateTask("rollup create index", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return metadataCreateIndexService.applyCreateIndexRequest(
                        currentState,
                        createIndexClusterStateUpdateRequest,
                        true,
                        (builder, indexMetadata) -> builder.put(
                            IndexMetadata.builder(indexMetadata)
                                .settings(
                                    Settings.builder()
                                        .put(indexMetadata.getSettings())
                                        .put(IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.getKey(), sourceIndexName)
                                        .put(IndexMetadata.INDEX_ROLLUP_SOURCE_UUID.getKey(), sourceIndexUuid)
                                )
                        )
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    // index created
                    // 3.
                    RollupIndexerAction.Request rollupIndexerRequest = new RollupIndexerAction.Request(request);
                    client.execute(RollupIndexerAction.INSTANCE, rollupIndexerRequest, ActionListener.wrap(indexerResp -> {
                        if (indexerResp.isCreated()) {
                            // 4.
                            UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(WRITE_BLOCKED_SETTINGS, tmpIndexName);
                            client.admin().indices().updateSettings(updateSettingsReq, ActionListener.wrap(updateSettingsResponse -> {
                                if (updateSettingsResponse.isAcknowledged()) {
                                    // 5.
                                    ResizeRequest resizeRequest = new ResizeRequest(rollupIndexName, tmpIndexName);
                                    resizeRequest.setResizeType(ResizeType.CLONE);
                                    resizeRequest.getTargetIndexRequest().settings(addRollupSettings(originalIndexMetadata.getSettings()));
                                    client.admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(resizeResponse -> {
                                        if (resizeResponse.isAcknowledged()) {
                                            // 6.
                                            publishMetadata(originalIndexName, tmpIndexName, rollupIndexName, listener);
                                        } else {
                                            // resize failed
                                            deleteTmpIndex(
                                                originalIndexName,
                                                tmpIndexName,
                                                listener,
                                                new ElasticsearchException("Unable to resize temp rollup index [" + tmpIndexName + "]")
                                            );
                                        }
                                    }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                                } else {
                                    // update settings failed
                                    deleteTmpIndex(
                                        originalIndexName,
                                        tmpIndexName,
                                        listener,
                                        new ElasticsearchException("Unable to update settings of temp rollup index [" + tmpIndexName + "]")
                                    );
                                }
                            }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                        } else {
                            // rollup failed
                            deleteTmpIndex(
                                originalIndexName,
                                tmpIndexName,
                                listener,
                                new ElasticsearchException("Unable to index into temp rollup index [" + tmpIndexName + "]")
                            );
                        }
                    }, e -> deleteTmpIndex(originalIndexName, tmpIndexName, listener, e)));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }, ClusterStateTaskExecutor.unbatched());
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(RollupAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void publishMetadata(
        String originalIndexName,
        String tmpIndexName,
        String rollupIndexName,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Update rollup metadata to include this index
        clusterService.submitStateUpdateTask("update-rollup-metadata", new ClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                // Everything went well, time to delete the temporary index
                deleteTmpIndex(originalIndexName, tmpIndexName, listener, null);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata rollupIndexMetadata = currentState.getMetadata().index(rollupIndexName);
                Index rollupIndex = rollupIndexMetadata.getIndex();
                IndexAbstraction originalIndex = currentState.getMetadata().getIndicesLookup().get(originalIndexName);

                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                if (originalIndex.getParentDataStream() != null) {
                    // If rolling up a backing index of a data stream, add rolled up index to backing data stream
                    DataStream originalDataStream = originalIndex.getParentDataStream().getDataStream();
                    List<Index> backingIndices = new ArrayList<>(originalDataStream.getIndices().size() + 1);
                    // Adding rollup indices to the beginning of the list will prevent rollup indices from ever being
                    // considered a write index
                    backingIndices.add(rollupIndex);
                    backingIndices.addAll(originalDataStream.getIndices());
                    DataStream dataStream = new DataStream(
                        originalDataStream.getName(),
                        originalDataStream.getTimeStampField(),
                        backingIndices,
                        originalDataStream.getGeneration(),
                        originalDataStream.getMetadata(),
                        originalDataStream.isHidden(),
                        originalDataStream.isReplicated(),
                        originalDataStream.isSystem(),
                        originalDataStream.isAllowCustomRouting()
                    );
                    metadataBuilder.put(dataStream);
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
            }

            @Override
            public void onFailure(Exception e) {
                // update rollup metadata failed
                // TODO if update rollup metadata failed, the rollup index has created, but maybe not in data stream indices list.
                deleteTmpIndex(
                    originalIndexName,
                    tmpIndexName,
                    listener,
                    new ElasticsearchException("failed to publish new cluster state with rollup metadata", e)
                );
            }
        }, ClusterStateTaskExecutor.unbatched());
    }

    private void deleteTmpIndex(String originalIndex, String tmpIndex, ActionListener<AcknowledgedResponse> listener, Exception e) {
        client.admin().indices().delete(new DeleteIndexRequest(tmpIndex), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                if (e == null && acknowledgedResponse.isAcknowledged()) {
                    listener.onResponse(acknowledgedResponse);
                } else {
                    listener.onFailure(new RollupExitException("Unable to rollup index [" + originalIndex + "]", e));
                }
            }

            @Override
            public void onFailure(Exception deleteException) {
                listener.onFailure(new RollupExitException("Unable to delete temp rollup index [" + tmpIndex + "]", e));
            }
        });
    }

    static Settings getSettings(IndexMetadata originalIndexMetadata, RollupActionConfig rollupActionConfig) {
        return Settings.builder()
            .put(filterSettings(originalIndexMetadata.getSettings()))
            .put(addTimeSeriesSettings(originalIndexMetadata, rollupActionConfig))
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    static Settings filterSettings(Settings originalSettings) {
        Settings.Builder settings = Settings.builder().put(originalSettings);

        // remove index inner settings
        settings.remove(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey());
        settings.remove(IndexMetadata.SETTING_VERSION_CREATED);
        settings.remove(IndexMetadata.SETTING_INDEX_UUID);
        settings.remove(IndexMetadata.SETTING_HISTORY_UUID);
        settings.remove(IndexMetadata.SETTING_INDEX_PROVIDED_NAME);
        settings.remove(IndexMetadata.SETTING_CREATION_DATE);

        // remove replica setting
        settings.remove(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);

        // remove ilm settings
        settings.remove(LifecycleSettings.LIFECYCLE_NAME);
        settings.remove(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE);

        // remove rollup source settings
        settings.remove(IndexMetadata.INDEX_ROLLUP_SOURCE_UUID_KEY);
        settings.remove(IndexMetadata.INDEX_ROLLUP_SOURCE_NAME_KEY);

        // remove resize settings
        settings.remove(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY);
        settings.remove(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY);

        // remove time_series settings
        settings.remove(IndexSettings.MODE.getKey());
        settings.remove(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        settings.remove(IndexSettings.TIME_SERIES_START_TIME.getKey());
        settings.remove(IndexSettings.TIME_SERIES_END_TIME.getKey());

        return settings.build();
    }

    /**
     * set rollup index to a time_series index
     * and deal the rollup index's routing path:
     * 1. if the origin index has no routing path, the rollup index will use the group terms field to set index path
     * 2. if the rollup group terms contian {@link TimeSeriesIdFieldMapper#NAME} field,
     *    the rollup index will reuse the origin index's routing path
     * 3. if the rollup group terms contain some fields that are not in the origin index's routing path,
     *    the rollup index will use the group terms field to set index path
     * 4. if the rollup group terms contain all the routing path fields, the rollup index will reuse the origin index's routing path
     */
    static Settings addTimeSeriesSettings(IndexMetadata originalIndexMetadata, RollupActionConfig config) {
        if (config == null) {
            return Settings.EMPTY;
        }

        RollupActionGroupConfig groupConfig = config.getGroupConfig();
        if (groupConfig == null || groupConfig.getTerms() == null || groupConfig.getDateHistogram() == null) {
            return Settings.EMPTY;
        }

        if (false == DataStreamTimestampFieldMapper.DEFAULT_PATH.equals(config.getGroupConfig().getDateHistogram().getField())) {
            return Settings.EMPTY;
        }

        String[] groupTerms = groupConfig.getTerms().getFields();
        Settings defaultTimeSeriesSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), groupTerms)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(1).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999 - 1).toString())
            .build();

        String indexMode = originalIndexMetadata.getSettings().get(IndexSettings.MODE.getKey());
        if (indexMode == null || false == indexMode.equals(IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))) {
            return defaultTimeSeriesSettings;
        }

        Set<String> terms = Set.of(groupTerms);
        List<String> routingPaths = originalIndexMetadata.getRoutingPaths();
        if (terms.contains(TimeSeriesIdFieldMapper.NAME)) {
            return Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
                .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPaths)
                .put(
                    IndexSettings.TIME_SERIES_START_TIME.getKey(),
                    originalIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_START_TIME.getKey())
                )
                .put(
                    IndexSettings.TIME_SERIES_END_TIME.getKey(),
                    originalIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_END_TIME.getKey())
                )
                .build();
        }

        List<String> newRoutingPaths = new ArrayList<>();
        boolean routingPathNotInTerms = false;
        for (String routingPath : routingPaths) {
            if (terms.contains(routingPath)) {
                newRoutingPaths.add(routingPath);
            } else if (routingPath.contains("*")) {
                boolean matchWildCard = false;
                for (String term : terms) {
                    if (Glob.globMatch(routingPath, term)) {
                        newRoutingPaths.add(routingPath);
                        matchWildCard = true;
                        break;
                    }
                }
                if (false == matchWildCard) {
                    routingPathNotInTerms = true;
                    break;
                }
            } else {
                routingPathNotInTerms = true;
                break;
            }
        }

        if (routingPathNotInTerms) {
            return defaultTimeSeriesSettings;
        }

        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name().toLowerCase(Locale.ROOT))
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), newRoutingPaths)
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                originalIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_START_TIME.getKey())
            )
            .put(
                IndexSettings.TIME_SERIES_END_TIME.getKey(),
                originalIndexMetadata.getSettings().get(IndexSettings.TIME_SERIES_END_TIME.getKey())
            )
            .build();
    }

    static XContentBuilder getMapping(RollupActionConfig config, MappingMetadata mappingMetadata) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        if (mappingMetadata != null) {
            getMetadataMapping(builder, mappingMetadata.getSourceAsMap());
        }
        getDynamicTemplates(builder);
        getProperties(builder, config);
        return builder.endObject();
    }

    /**
     * TODO: now we add origin index's _source mapping, maybe we can add more metadata mapping
     */
    private static void getMetadataMapping(XContentBuilder builder, Map<String, Object> mappings) throws IOException {
        if (mappings != null && mappings.containsKey(SourceFieldMapper.NAME)) {
            builder.field(SourceFieldMapper.NAME, mappings.get(SourceFieldMapper.NAME));
        }
    }

    /**
     * Configure the dynamic templates to always map strings to the keyword field type.
     */
    private static void getDynamicTemplates(XContentBuilder builder) throws IOException {
        builder.startArray("dynamic_templates")
            .startObject()
            .startObject("strings")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .field("time_series_dimension", "true")
            .endObject()
            .endObject()
            .endObject()
            .endArray();
    }

    /**
     * Creates the rollup mapping properties from the provided {@link RollupActionConfig}.
     */
    private static void getProperties(XContentBuilder builder, RollupActionConfig config) throws IOException {
        builder.startObject("properties");

        RollupActionGroupConfig groupConfig = config.getGroupConfig();
        RollupActionDateHistogramGroupConfig dateHistogramConfig = groupConfig.getDateHistogram();
        String dateField = dateHistogramConfig.getField();
        String dateIntervalType = dateHistogramConfig.getIntervalTypeName();
        String dateInterval = dateHistogramConfig.getInterval().toString();
        String tz = dateHistogramConfig.getTimeZone() != null
            ? dateHistogramConfig.getTimeZone()
            : RollupActionDateHistogramGroupConfig.DEFAULT_TIMEZONE;

        builder.startObject(dateField)
            .field("type", DateFieldMapper.CONTENT_TYPE)
            .startObject("meta")
            .field(dateIntervalType, dateInterval)
            .field(RollupActionDateHistogramGroupConfig.CalendarInterval.TIME_ZONE, tz)
            .endObject()
            .endObject();

        HistogramGroupConfig histogramGroupConfig = groupConfig.getHistogram();
        if (histogramGroupConfig != null) {
            for (String field : histogramGroupConfig.getFields()) {
                builder.startObject(field)
                    .field("type", NumberFieldMapper.NumberType.DOUBLE.typeName())
                    .startObject("meta")
                    .field(HistogramGroupConfig.INTERVAL, String.valueOf(histogramGroupConfig.getInterval()))
                    .endObject()
                    .endObject();
            }
        }

        TermsGroupConfig termsGroupConfig = groupConfig.getTerms();
        if (termsGroupConfig != null) {
            for (String field : termsGroupConfig.getFields()) {
                if (field.equals(TimeSeriesIdFieldMapper.NAME)) {
                    continue;
                }
                builder.startObject(field).field("type", "keyword").field("time_series_dimension", "true").endObject();
            }
        }

        List<MetricConfig> metricConfigs = config.getMetricsConfig();
        for (MetricConfig metricConfig : metricConfigs) {
            List<String> metrics = MetricField.normalizeMetrics(metricConfig.getMetrics());
            String defaultMetric = metrics.contains("value_count") ? "value_count" : metrics.get(0);
            builder.startObject(metricConfig.getField())
                .field("type", AggregateDoubleMetricFieldMapper.CONTENT_TYPE)
                .stringListField(AggregateDoubleMetricFieldMapper.Names.METRICS, metrics)
                .field(AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC, defaultMetric)
                .endObject();
        }

        builder.endObject();
    }

    private static void rebuildRollupField(String field, Map<String, Map<String, FieldCapabilities>> fieldCaps, Consumer<String> consumer) {
        Predicate<Map<String, FieldCapabilities>> predicate = (fieldCap) -> {
            boolean isValueType = true;
            for (Map.Entry<String, FieldCapabilities> entry : fieldCap.entrySet()) {
                String fieldType = entry.getKey();
                if (fieldType.equals(ObjectMapper.CONTENT_TYPE) || fieldType.equals(NestedObjectMapper.CONTENT_TYPE)) {
                    isValueType = false;
                    break;
                }
            }
            return isValueType;
        };
        if (field.contains("*")) {
            fieldCaps.forEach((key, value) -> {
                if (Glob.globMatch(field, key) && predicate.test(value)) {
                    consumer.accept(key);
                }
            });
        } else {
            consumer.accept(field);
        }
    }

    private static Set<String> rebuildRollupFields(List<String> groupFields, Map<String, Map<String, FieldCapabilities>> fieldCaps) {
        long wildcardCount = groupFields.stream().filter(field -> field.contains("*")).count();
        // no wildcard fields
        if (wildcardCount == 0) {
            Set.of(groupFields);
        }

        Set<String> newGroupFields = new LinkedHashSet<>();
        for (String field : groupFields) {
            rebuildRollupField(field, fieldCaps, (config) -> newGroupFields.add(config));
        }
        return newGroupFields;
    }

    private static RollupActionGroupConfig rebuildRollupGroupConfig(
        RollupActionGroupConfig groupConfig,
        Map<String, Map<String, FieldCapabilities>> fieldCaps
    ) {
        TermsGroupConfig termsGroupConfig;
        if (groupConfig.getTerms() != null) {
            Set<String> groupFields = rebuildRollupFields(List.of(groupConfig.getTerms().getFields()), fieldCaps);
            if (groupFields.size() == 0) {
                throw new IllegalArgumentException(
                    "Could not find a field match the group terms " + List.of(groupConfig.getTerms().getFields())
                );
            }
            termsGroupConfig = new TermsGroupConfig(groupFields.toArray(Strings.EMPTY_ARRAY));
        } else {
            termsGroupConfig = null;
        }

        HistogramGroupConfig histogramGroupConfig;
        if (groupConfig.getHistogram() != null) {
            Set<String> groupFields = rebuildRollupFields(List.of(groupConfig.getHistogram().getFields()), fieldCaps);
            if (groupFields.size() == 0) {
                throw new IllegalArgumentException(
                    "Could not find a field match the group histograms " + List.of(groupConfig.getHistogram().getFields())
                );
            }
            histogramGroupConfig = new HistogramGroupConfig(
                groupConfig.getHistogram().getInterval(),
                groupFields.toArray(Strings.EMPTY_ARRAY)
            );
        } else {
            histogramGroupConfig = null;
        }

        return new RollupActionGroupConfig(groupConfig.getDateHistogram(), histogramGroupConfig, termsGroupConfig);
    }

    private static List<MetricConfig> rebuildRollupMetricConfig(
        List<MetricConfig> metricConfigs,
        Map<String, Map<String, FieldCapabilities>> fieldCaps
    ) {
        long wildcardCount = metricConfigs.stream().filter(metric -> metric.getField().contains("*")).count();
        // no wildcard fields
        if (wildcardCount == 0) {
            return metricConfigs;
        }

        List<MetricConfig> newMetricConfigs = new ArrayList<>();
        for (MetricConfig metricConfig : metricConfigs) {
            String field = metricConfig.getField();
            rebuildRollupField(field, fieldCaps, (config) -> newMetricConfigs.add(new MetricConfig(config, metricConfig.getMetrics())));
        }
        if (newMetricConfigs.size() == 0) {
            throw new IllegalArgumentException(
                "Could not find a field match the metric fields "
                    + metricConfigs.stream().map(MetricConfig::getField).collect(Collectors.toSet())
            );
        }
        return newMetricConfigs;
    }

    static RollupActionConfig rebuildRollupConfig(RollupActionConfig config, Map<String, Map<String, FieldCapabilities>> fieldCaps) {
        RollupActionGroupConfig groupConfig = rebuildRollupGroupConfig(config.getGroupConfig(), fieldCaps);
        List<MetricConfig> metricConfigs = rebuildRollupMetricConfig(config.getMetricsConfig(), fieldCaps);
        return new RollupActionConfig(groupConfig, metricConfigs);
    }

    static Settings addRollupSettings(Settings originalSettings) {
        Settings.Builder builder = Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true);
        if (originalSettings.hasValue(LifecycleSettings.LIFECYCLE_NAME)) {
            builder.put(LifecycleSettings.LIFECYCLE_NAME, originalSettings.get(LifecycleSettings.LIFECYCLE_NAME));
        }
        if (originalSettings.hasValue(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE)) {
            builder.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, originalSettings.get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }

        return builder.build();
    }

    private void validExistsRollupIndex(ClusterState state, String rollupIndex) {
        if (state.metadata().hasIndex(rollupIndex)) {
            throw new InvalidIndexNameException(rollupIndex, "rollup index already exists");
        }

        if (state.metadata().hasAlias(rollupIndex)) {
            throw new InvalidIndexNameException(rollupIndex, "rollup index already exists as alias");
        }

        if (state.metadata().dataStreams().containsKey(rollupIndex)) {
            throw new InvalidIndexNameException(rollupIndex, "rollup index already exists as data stream");
        }
    }
}
