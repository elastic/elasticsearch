/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.createDataStream;

public class MetadataMigrateToDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataStreamService.class);

    private static final CompressedXContent TIMESTAMP_MAPPING;

    static {
        try {
            TIMESTAMP_MAPPING = new CompressedXContent(
                ((builder, params) -> builder.startObject(DataStreamTimestampFieldMapper.NAME).field("enabled", true).endObject())
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final ClusterService clusterService;
    private final IndicesService indexServices;
    private final ThreadContext threadContext;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final boolean isDslOnlyMode;

    public MetadataMigrateToDataStreamService(
        ThreadPool threadPool,
        ClusterService clusterService,
        IndicesService indexServices,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        this.clusterService = clusterService;
        this.indexServices = indexServices;
        this.threadContext = threadPool.getThreadContext();
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.isDslOnlyMode = isDataStreamsLifecycleOnlyMode(clusterService.getSettings());
    }

    public void migrateToDataStream(
        ProjectId projectId,
        MigrateToDataStreamClusterStateUpdateRequest request,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        metadataCreateIndexService.getSystemIndices().validateDataStreamAccess(request.aliasName, threadContext);
        AtomicReference<String> writeIndexRef = new AtomicReference<>();
        ActionListener<AcknowledgedResponse> listener = finalListener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged()) {
                String writeIndexName = writeIndexRef.get();
                assert writeIndexName != null;
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    projectId,
                    new String[] { writeIndexName },
                    ActiveShardCount.DEFAULT,
                    request.masterNodeTimeout(),
                    delegate.map(shardsAcknowledged -> AcknowledgedResponse.TRUE)
                );
            } else {
                delegate.onResponse(AcknowledgedResponse.FALSE);
            }
        });
        var delegate = new AllocationActionListener<>(listener, threadContext);
        submitUnbatchedTask(
            "migrate-to-data-stream [" + request.aliasName + "]",
            new AckedClusterStateUpdateTask(
                Priority.HIGH,
                request.masterNodeTimeout(),
                request.ackTimeout(),
                delegate.clusterStateUpdate()
            ) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = migrateToDataStream(currentState.projectState(projectId), isDslOnlyMode, indexMetadata -> {
                        try {
                            return indexServices.createIndexMapperServiceForValidation(indexMetadata);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }, request, metadataCreateIndexService, clusterService.getSettings(), delegate.reroute());
                    writeIndexRef.set(
                        clusterState.metadata().getProject(projectId).dataStreams().get(request.aliasName).getWriteIndex().getName()
                    );
                    return clusterState;
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState migrateToDataStream(
        ProjectState projectState,
        boolean isDslOnlyMode,
        Function<IndexMetadata, MapperService> mapperSupplier,
        MigrateToDataStreamClusterStateUpdateRequest request,
        MetadataCreateIndexService metadataCreateIndexService,
        Settings settings,
        ActionListener<Void> listener
    ) throws Exception {
        final var project = projectState.metadata();
        validateRequest(project, request);
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) project.getIndicesLookup().get(request.aliasName);

        validateBackingIndices(project, request.aliasName);
        ProjectMetadata.Builder mb = ProjectMetadata.builder(project);
        for (Index index : alias.getIndices()) {
            IndexMetadata im = project.index(index);
            prepareBackingIndex(mb, im, request.aliasName, mapperSupplier, true, false, false, Settings.EMPTY);
        }
        ClusterState updatedState = ClusterState.builder(projectState.cluster()).putProjectMetadata(mb).build();

        Index writeIndex = alias.getWriteIndex();
        List<IndexMetadata> backingIndices = alias.getIndices()
            .stream()
            .filter(x -> writeIndex == null || x.equals(writeIndex) == false)
            .map(x -> updatedState.metadata().getProject(project.id()).index(x))
            .toList();

        logger.info("submitting request to migrate alias [{}] to a data stream", request.aliasName);
        CreateDataStreamClusterStateUpdateRequest req = new CreateDataStreamClusterStateUpdateRequest(project.id(), request.aliasName);
        return createDataStream(
            metadataCreateIndexService,
            settings,
            updatedState,
            isDslOnlyMode,
            req,
            backingIndices,
            updatedState.metadata().getProject(project.id()).index(writeIndex),
            listener,
            // No need to initialize the failure store when migrating to a data stream.
            false
        );
    }

    // package-visible for testing
    static void validateRequest(ProjectMetadata project, MigrateToDataStreamClusterStateUpdateRequest request) {
        IndexAbstraction ia = project.getIndicesLookup().get(request.aliasName);
        if (ia == null || ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] does not exist");
        }
        if (ia.getWriteIndex() == null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] must specify a write index");
        }

        // check for "clean" alias without routing or filter query
        AliasMetadata aliasMetadata = AliasMetadata.getFirstAliasMetadata(project, ia);
        assert aliasMetadata != null : "alias metadata may not be null";
        if (aliasMetadata.filteringRequired() || aliasMetadata.getIndexRouting() != null || aliasMetadata.getSearchRouting() != null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] may not have custom filtering or routing");
        }
    }

    /**
     * Hides the index, optionally removes the alias, adds data stream timestamp field mapper, and configures any additional settings
     * needed for the index to be included within a data stream.
     * @param b Metadata.Builder to consume updates to the provided index
     * @param im IndexMetadata to be migrated to a data stream
     * @param dataStreamName The name of the data stream to migrate the index into
     * @param mapperSupplier A function that returns a MapperService for the given index
     * @param removeAlias <code>true</code> if the migration should remove any aliases present on the index, <code>false</code> if an
     *                    exception should be thrown in that case instead
     * @param failureStore <code>true</code> if the index is being migrated into the data stream's failure store, <code>false</code> if it
     *                     is being migrated into the data stream's backing indices
     * @param makeSystem <code>true</code> if the index is being migrated into the system data stream, <code>false</code> if it
     *                     is being migrated into non-system data stream
     * @param nodeSettings The settings for the current node
     */
    static void prepareBackingIndex(
        ProjectMetadata.Builder b,
        IndexMetadata im,
        String dataStreamName,
        Function<IndexMetadata, MapperService> mapperSupplier,
        boolean removeAlias,
        boolean failureStore,
        boolean makeSystem,
        Settings nodeSettings
    ) throws IOException {
        MappingMetadata mm = im.mapping();
        if (mm == null || mm.equals(MappingMetadata.EMPTY_MAPPINGS)) {
            throw new IllegalArgumentException("backing index [" + im.getIndex().getName() + "] must have mappings for a timestamp field");
        }

        MapperService mapperService = mapperSupplier.apply(im);
        mapperService.merge(im, MapperService.MergeReason.MAPPING_RECOVERY);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, TIMESTAMP_MAPPING, MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapper = mapperService.documentMapper();

        var imb = IndexMetadata.builder(im);
        if (removeAlias) {
            imb.removeAlias(dataStreamName);
        }

        Settings.Builder settingsUpdate = Settings.builder().put(im.getSettings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true);

        if (failureStore) {
            DataStreamFailureStoreDefinition.applyFailureStoreSettings(nodeSettings, settingsUpdate);
        }

        Settings maybeUpdatedSettings = settingsUpdate.build();
        if (IndexSettings.same(im.getSettings(), maybeUpdatedSettings) == false) {
            imb.settings(maybeUpdatedSettings).settingsVersion(im.getSettingsVersion() + 1);
        }
        imb.mappingVersion(im.getMappingVersion() + 1)
            .mappingsUpdatedVersion(IndexVersion.current())
            .putMapping(new MappingMetadata(mapper));
        imb.system(makeSystem);
        b.put(imb);
    }

    // package-visible for testing
    static void validateBackingIndices(ProjectMetadata project, String dataStreamName) {
        IndexAbstraction ia = project.getIndicesLookup().get(dataStreamName);
        if (ia == null || ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("alias [" + dataStreamName + "] does not exist");
        }
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) ia;

        // ensure that no other aliases reference indices
        List<String> indicesWithOtherAliases = new ArrayList<>();
        for (Index index : alias.getIndices()) {
            IndexMetadata im = project.index(index);
            if (im.getAliases().size() > 1 || im.getAliases().containsKey(alias.getName()) == false) {
                indicesWithOtherAliases.add(index.getName());
            }
        }
        if (indicesWithOtherAliases.size() > 0) {
            throw new IllegalArgumentException(
                "other aliases referencing indices ["
                    + Strings.collectionToCommaDelimitedString(indicesWithOtherAliases)
                    + "] must be removed before migrating to a data stream"
            );
        }
    }

    public record MigrateToDataStreamClusterStateUpdateRequest(String aliasName, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
        public MigrateToDataStreamClusterStateUpdateRequest {
            Objects.requireNonNull(aliasName);
            Objects.requireNonNull(masterNodeTimeout);
            Objects.requireNonNull(ackTimeout);
        }
    }

}
