/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.rollover.MetadataRolloverService;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;

public class MetadataCreateDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataCreateDataStreamService.class);

    public static final String FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME = "data_streams.failure_store.refresh_interval";

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final boolean isDslOnlyMode;

    public MetadataCreateDataStreamService(
        ThreadPool threadPool,
        ClusterService clusterService,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.isDslOnlyMode = isDataStreamsLifecycleOnlyMode(clusterService.getSettings());
    }

    public void createDataStream(CreateDataStreamClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> finalListener) {
        AtomicReference<String> firstBackingIndexRef = new AtomicReference<>();
        AtomicReference<String> firstFailureStoreRef = new AtomicReference<>();
        ActionListener<AcknowledgedResponse> listener = finalListener.delegateFailureAndWrap((l, response) -> {
            if (response.isAcknowledged()) {
                String firstBackingIndexName = firstBackingIndexRef.get();
                assert firstBackingIndexName != null;
                String firstFailureStoreName = firstFailureStoreRef.get();
                var waitForIndices = firstFailureStoreName == null
                    ? new String[] { firstBackingIndexName }
                    : new String[] { firstBackingIndexName, firstFailureStoreName };
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    waitForIndices,
                    ActiveShardCount.DEFAULT,
                    request.masterNodeTimeout(),
                    l.map(shardsAcked -> AcknowledgedResponse.TRUE)
                );
            } else {
                l.onResponse(AcknowledgedResponse.FALSE);
            }
        });
        var delegate = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        submitUnbatchedTask(
            "create-data-stream [" + request.name + "]",
            new AckedClusterStateUpdateTask(Priority.HIGH, request, delegate.clusterStateUpdate()) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = createDataStream(
                        metadataCreateIndexService,
                        clusterService.getSettings(),
                        currentState,
                        isDslOnlyMode,
                        request,
                        delegate.reroute()
                    );
                    DataStream createdDataStream = clusterState.metadata().dataStreams().get(request.name);
                    firstBackingIndexRef.set(createdDataStream.getIndices().get(0).getName());
                    if (createdDataStream.getFailureIndices().isEmpty() == false) {
                        firstFailureStoreRef.set(createdDataStream.getFailureIndices().get(0).getName());
                    }
                    return clusterState;
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    public ClusterState createDataStream(
        CreateDataStreamClusterStateUpdateRequest request,
        ClusterState current,
        ActionListener<Void> rerouteListener
    ) throws Exception {
        return createDataStream(metadataCreateIndexService, clusterService.getSettings(), current, isDslOnlyMode, request, rerouteListener);
    }

    public static final class CreateDataStreamClusterStateUpdateRequest extends ClusterStateUpdateRequest<
        CreateDataStreamClusterStateUpdateRequest> {

        private final boolean performReroute;
        private final String name;
        private final long startTime;
        private final SystemDataStreamDescriptor descriptor;

        public CreateDataStreamClusterStateUpdateRequest(String name) {
            this(name, System.currentTimeMillis(), null, TimeValue.ZERO, TimeValue.ZERO, true);
        }

        public CreateDataStreamClusterStateUpdateRequest(
            String name,
            SystemDataStreamDescriptor systemDataStreamDescriptor,
            TimeValue masterNodeTimeout,
            TimeValue timeout,
            boolean performReroute
        ) {
            this(name, System.currentTimeMillis(), systemDataStreamDescriptor, masterNodeTimeout, timeout, performReroute);
        }

        public CreateDataStreamClusterStateUpdateRequest(
            String name,
            long startTime,
            SystemDataStreamDescriptor systemDataStreamDescriptor,
            TimeValue masterNodeTimeout,
            TimeValue timeout,
            boolean performReroute
        ) {
            this.name = name;
            this.startTime = startTime;
            this.descriptor = systemDataStreamDescriptor;
            this.performReroute = performReroute;
            masterNodeTimeout(masterNodeTimeout);
            ackTimeout(timeout);
        }

        public boolean isSystem() {
            return descriptor != null;
        }

        public boolean performReroute() {
            return performReroute;
        }

        public SystemDataStreamDescriptor getSystemDataStreamDescriptor() {
            return descriptor;
        }

        long getStartTime() {
            return startTime;
        }
    }

    static ClusterState createDataStream(
        MetadataCreateIndexService metadataCreateIndexService,
        Settings settings,
        ClusterState currentState,
        boolean isDslOnlyMode,
        CreateDataStreamClusterStateUpdateRequest request,
        ActionListener<Void> rerouteListener
    ) throws Exception {
        return createDataStream(
            metadataCreateIndexService,
            settings,
            currentState,
            isDslOnlyMode,
            request,
            List.of(),
            null,
            rerouteListener
        );
    }

    /**
     * Creates a data stream with the specified request, backing indices and write index.
     *
     * @param metadataCreateIndexService Used if a new write index must be created
     * @param currentState               Cluster state
     * @param request                    The create data stream request
     * @param backingIndices             List of backing indices. May be empty
     * @param writeIndex                 Write index for the data stream. If null, a new write index will be created.
     * @return                           Cluster state containing the new data stream
     */
    static ClusterState createDataStream(
        MetadataCreateIndexService metadataCreateIndexService,
        Settings settings,
        ClusterState currentState,
        boolean isDslOnlyMode,
        CreateDataStreamClusterStateUpdateRequest request,
        List<IndexMetadata> backingIndices,
        IndexMetadata writeIndex,
        ActionListener<Void> rerouteListener
    ) throws Exception {
        String dataStreamName = request.name;
        SystemDataStreamDescriptor systemDataStreamDescriptor = request.getSystemDataStreamDescriptor();
        boolean isSystemDataStreamName = metadataCreateIndexService.getSystemIndices().isSystemDataStream(request.name);
        assert (isSystemDataStreamName && systemDataStreamDescriptor != null)
            || (isSystemDataStreamName == false && systemDataStreamDescriptor == null)
            : "dataStream [" + request.name + "] is system but no system descriptor was provided!";

        Objects.requireNonNull(metadataCreateIndexService);
        Objects.requireNonNull(currentState);
        Objects.requireNonNull(backingIndices);
        if (currentState.metadata().dataStreams().containsKey(dataStreamName)) {
            throw new ResourceAlreadyExistsException("data_stream [" + dataStreamName + "] already exists");
        }

        MetadataCreateIndexService.validateIndexOrAliasName(
            dataStreamName,
            (s1, s2) -> new IllegalArgumentException("data_stream [" + s1 + "] " + s2)
        );

        if (dataStreamName.toLowerCase(Locale.ROOT).equals(dataStreamName) == false) {
            throw new IllegalArgumentException("data_stream [" + dataStreamName + "] must be lowercase");
        }
        if (dataStreamName.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            throw new IllegalArgumentException(
                "data_stream [" + dataStreamName + "] must not start with '" + DataStream.BACKING_INDEX_PREFIX + "'"
            );
        }
        if (dataStreamName.startsWith(DataStream.FAILURE_STORE_PREFIX)) {
            throw new IllegalArgumentException(
                "data_stream [" + dataStreamName + "] must not start with '" + DataStream.FAILURE_STORE_PREFIX + "'"
            );
        }

        final var metadata = currentState.metadata();
        final boolean isSystem = systemDataStreamDescriptor != null;
        final ComposableIndexTemplate template = isSystem
            ? systemDataStreamDescriptor.getComposableIndexTemplate()
            : lookupTemplateForDataStream(dataStreamName, currentState.metadata());
        // The initial backing index and the initial failure store index will have the same initial generation.
        // This is not a problem as both have different prefixes (`.ds-` vs `.fs-`) and both will be using the same `generation` field
        // when rolling over in the future.
        final long initialGeneration = 1;

        // If we need to create a failure store, do so first. Do not reroute during the creation since we will do
        // that as part of creating the backing index if required.
        IndexMetadata failureStoreIndex = null;
        if (template.getDataStreamTemplate().hasFailureStore()) {
            if (isSystem) {
                throw new IllegalArgumentException("Failure stores are not supported on system data streams");
            }
            String failureStoreIndexName = DataStream.getDefaultFailureStoreName(dataStreamName, initialGeneration, request.getStartTime());
            currentState = createFailureStoreIndex(
                metadataCreateIndexService,
                "initialize_data_stream",
                settings,
                currentState,
                request.getStartTime(),
                dataStreamName,
                template,
                failureStoreIndexName,
                null
            );
            failureStoreIndex = currentState.metadata().index(failureStoreIndexName);
        }

        if (writeIndex == null) {
            String firstBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, initialGeneration, request.getStartTime());
            currentState = createBackingIndex(
                metadataCreateIndexService,
                currentState,
                request,
                rerouteListener,
                dataStreamName,
                systemDataStreamDescriptor,
                isSystem,
                template,
                firstBackingIndexName
            );
            writeIndex = currentState.metadata().index(firstBackingIndexName);
        } else {
            rerouteListener.onResponse(null);
        }
        assert writeIndex != null;
        assert writeIndex.mapping() != null : "no mapping found for backing index [" + writeIndex.getIndex().getName() + "]";
        assert template.getDataStreamTemplate().hasFailureStore() == false || failureStoreIndex != null;
        assert failureStoreIndex == null || failureStoreIndex.mapping() != null
            : "no mapping found for failure store [" + failureStoreIndex.getIndex().getName() + "]";

        List<Index> dsBackingIndices = backingIndices.stream()
            .map(IndexMetadata::getIndex)
            .collect(Collectors.toCollection(ArrayList::new));
        dsBackingIndices.add(writeIndex.getIndex());
        boolean hidden = isSystem || template.getDataStreamTemplate().isHidden();
        final IndexMode indexMode = metadata.isTimeSeriesTemplate(template) ? IndexMode.TIME_SERIES : null;
        final DataStreamLifecycle lifecycle = isSystem
            ? MetadataIndexTemplateService.resolveLifecycle(template, systemDataStreamDescriptor.getComponentTemplates())
            : MetadataIndexTemplateService.resolveLifecycle(template, metadata.componentTemplates());
        List<Index> failureIndices = failureStoreIndex == null ? List.of() : List.of(failureStoreIndex.getIndex());
        DataStream newDataStream = new DataStream(
            dataStreamName,
            dsBackingIndices,
            initialGeneration,
            template.metadata() != null ? Map.copyOf(template.metadata()) : null,
            hidden,
            false,
            isSystem,
            template.getDataStreamTemplate().isAllowCustomRouting(),
            indexMode,
            lifecycle == null && isDslOnlyMode ? DataStreamLifecycle.DEFAULT : lifecycle,
            template.getDataStreamTemplate().hasFailureStore(),
            failureIndices,
            null
        );
        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(newDataStream);

        List<String> aliases = new ArrayList<>();
        var resolvedAliases = MetadataIndexTemplateService.resolveAliases(currentState.metadata(), template);
        for (var resolvedAliasMap : resolvedAliases) {
            for (var alias : resolvedAliasMap.values()) {
                aliases.add(alias.getAlias());
                builder.put(alias.getAlias(), dataStreamName, alias.writeIndex(), alias.filter() == null ? null : alias.filter().string());
            }
        }

        logger.info(
            "adding data stream [{}] with write index [{}], backing indices [{}], and aliases [{}]",
            dataStreamName,
            writeIndex.getIndex().getName(),
            Strings.arrayToCommaDelimitedString(backingIndices.stream().map(i -> i.getIndex().getName()).toArray()),
            Strings.collectionToCommaDelimitedString(aliases)
        );

        return ClusterState.builder(currentState).metadata(builder).build();
    }

    private static ClusterState createBackingIndex(
        MetadataCreateIndexService metadataCreateIndexService,
        ClusterState currentState,
        CreateDataStreamClusterStateUpdateRequest request,
        ActionListener<Void> rerouteListener,
        String dataStreamName,
        SystemDataStreamDescriptor systemDataStreamDescriptor,
        boolean isSystem,
        ComposableIndexTemplate template,
        String firstBackingIndexName
    ) throws Exception {
        CreateIndexClusterStateUpdateRequest createIndexRequest = new CreateIndexClusterStateUpdateRequest(
            "initialize_data_stream",
            firstBackingIndexName,
            firstBackingIndexName
        ).dataStreamName(dataStreamName)
            .systemDataStreamDescriptor(systemDataStreamDescriptor)
            .nameResolvedInstant(request.getStartTime())
            .performReroute(request.performReroute())
            .setMatchingTemplate(template);

        if (isSystem) {
            createIndexRequest.settings(SystemIndexDescriptor.DEFAULT_SETTINGS);
        } else {
            createIndexRequest.settings(MetadataRolloverService.HIDDEN_INDEX_SETTINGS);
        }

        try {
            currentState = metadataCreateIndexService.applyCreateIndexRequest(currentState, createIndexRequest, false, rerouteListener);
        } catch (ResourceAlreadyExistsException e) {
            // Rethrow as ElasticsearchStatusException, so that bulk transport action doesn't ignore it during
            // auto index/data stream creation.
            // (otherwise bulk execution fails later, because data stream will also not have been created)
            throw new ElasticsearchStatusException(
                "data stream could not be created because backing index [{}] already exists",
                RestStatus.BAD_REQUEST,
                e,
                firstBackingIndexName
            );
        }
        return currentState;
    }

    public static ClusterState createFailureStoreIndex(
        MetadataCreateIndexService metadataCreateIndexService,
        String cause,
        Settings settings,
        ClusterState currentState,
        long nameResolvedInstant,
        String dataStreamName,
        ComposableIndexTemplate template,
        String failureStoreIndexName,
        @Nullable BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer
    ) throws Exception {
        if (DataStream.isFailureStoreEnabled() == false) {
            return currentState;
        }

        var indexSettings = DataStreamFailureStoreDefinition.buildFailureStoreIndexSettings(
            MetadataRolloverService.HIDDEN_INDEX_SETTINGS,
            settings
        );

        CreateIndexClusterStateUpdateRequest createIndexRequest = new CreateIndexClusterStateUpdateRequest(
            cause,
            failureStoreIndexName,
            failureStoreIndexName
        ).dataStreamName(dataStreamName)
            .nameResolvedInstant(nameResolvedInstant)
            .performReroute(false)
            .setMatchingTemplate(template)
            .settings(indexSettings);

        try {
            currentState = metadataCreateIndexService.applyCreateIndexRequest(
                currentState,
                createIndexRequest,
                false,
                metadataTransformer,
                AllocationActionListener.rerouteCompletionIsNotRequired()
            );
        } catch (ResourceAlreadyExistsException e) {
            // Rethrow as ElasticsearchStatusException, so that bulk transport action doesn't ignore it during
            // auto index/data stream creation.
            // (otherwise bulk execution fails later, because data stream will also not have been created)
            throw new ElasticsearchStatusException(
                "data stream could not be created because failure store index [{}] already exists",
                RestStatus.BAD_REQUEST,
                e,
                failureStoreIndexName
            );
        }
        return currentState;
    }

    public static ComposableIndexTemplate lookupTemplateForDataStream(String dataStreamName, Metadata metadata) {
        final String v2Template = MetadataIndexTemplateService.findV2Template(metadata, dataStreamName, false);
        if (v2Template == null) {
            throw new IllegalArgumentException("no matching index template found for data stream [" + dataStreamName + "]");
        }
        ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(v2Template);
        if (composableIndexTemplate.getDataStreamTemplate() == null) {
            throw new IllegalArgumentException(
                "matching index template [" + v2Template + "] for data stream [" + dataStreamName + "] has no data stream template"
            );
        }
        return composableIndexTemplate;
    }

    public static void validateTimestampFieldMapping(MappingLookup mappingLookup) throws IOException {
        MetadataFieldMapper fieldMapper = (MetadataFieldMapper) mappingLookup.getMapper(DataStreamTimestampFieldMapper.NAME);
        assert fieldMapper != null : DataStreamTimestampFieldMapper.NAME + " meta field mapper must exist";
        // Sanity check: if this fails then somehow the mapping for _data_stream_timestamp has been overwritten and
        // that would be a bug.
        if (mappingLookup.isDataStreamTimestampFieldEnabled() == false) {
            throw new IllegalStateException("[" + DataStreamTimestampFieldMapper.NAME + "] meta field has been disabled");
        }
        // Sanity check (this validation logic should already have been executed when merging mappings):
        fieldMapper.validate(mappingLookup);
    }
}
