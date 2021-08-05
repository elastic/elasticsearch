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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Map;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetadataMigrateToDataStreamService {

    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataStreamService.class);

    private final ClusterService clusterService;
    private final ActiveShardsObserver activeShardsObserver;
    private final IndicesService indexServices;
    private final ThreadContext threadContext;
    private final MetadataCreateIndexService metadataCreateIndexService;

    public MetadataMigrateToDataStreamService(ThreadPool threadPool,
                                              ClusterService clusterService,
                                              IndicesService indexServices,
                                              MetadataCreateIndexService metadataCreateIndexService) {
        this.clusterService = clusterService;
        this.indexServices = indexServices;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.threadContext = threadPool.getThreadContext();
        this.metadataCreateIndexService = metadataCreateIndexService;
    }

    public void migrateToDataStream(MigrateToDataStreamClusterStateUpdateRequest request,
                                    ActionListener<AcknowledgedResponse> finalListener) {
        metadataCreateIndexService.getSystemIndices().validateDataStreamAccess(request.aliasName, threadContext);
        AtomicReference<String> writeIndexRef = new AtomicReference<>();
        ActionListener<AcknowledgedResponse> listener = ActionListener.wrap(
            response -> {
                if (response.isAcknowledged()) {
                    String writeIndexName = writeIndexRef.get();
                    assert writeIndexName != null;
                    activeShardsObserver.waitForActiveShards(
                        new String[]{writeIndexName},
                        ActiveShardCount.DEFAULT,
                        request.masterNodeTimeout(),
                        shardsAcked -> {
                            finalListener.onResponse(AcknowledgedResponse.TRUE);
                        },
                        finalListener::onFailure);
                } else {
                    finalListener.onResponse(AcknowledgedResponse.FALSE);
                }
            },
            finalListener::onFailure
        );
        clusterService.submitStateUpdateTask("migrate-to-data-stream [" + request.aliasName + "]",
            new AckedClusterStateUpdateTask(Priority.HIGH, request, listener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState clusterState = migrateToDataStream(
                        currentState,
                        indexMetadata -> {
                            try {
                                return indexServices.createIndexMapperService(indexMetadata);
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        },
                        request,
                        threadContext,
                        metadataCreateIndexService);
                    writeIndexRef.set(clusterState.metadata().dataStreams().get(request.aliasName).getWriteIndex().getName());
                    return clusterState;
                }
            });
    }

    static ClusterState migrateToDataStream(ClusterState currentState,
                                            Function<IndexMetadata, MapperService> mapperSupplier,
                                            MigrateToDataStreamClusterStateUpdateRequest request,
                                            ThreadContext threadContext,
                                            MetadataCreateIndexService metadataCreateIndexService) throws Exception {
        if (currentState.nodes().getMinNodeVersion().before(Version.V_7_11_0)) {
            throw new IllegalStateException("data stream migration requires minimum node version of " + Version.V_7_11_0);
        }
        validateRequest(currentState, request);
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) currentState.metadata().getIndicesLookup().get(request.aliasName);

        validateBackingIndices(currentState, request.aliasName);
        Metadata.Builder mb = Metadata.builder(currentState.metadata());
        for (IndexMetadata im : alias.getIndices()) {
            prepareBackingIndex(mb, im, request.aliasName, mapperSupplier);
        }
        currentState = ClusterState.builder(currentState).metadata(mb).build();

        IndexMetadata writeIndex = alias.getWriteIndex();

        List<IndexMetadata> backingIndices = alias.getIndices()
            .stream()
            .filter(x -> writeIndex == null || x.getIndex().getName().equals(writeIndex.getIndex().getName()) == false)
            .collect(Collectors.toList());

        logger.info("submitting request to migrate alias [{}] to a data stream", request.aliasName);
        return MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, currentState, request.aliasName,
            backingIndices, writeIndex);
    }

    // package-visible for testing
    static void validateRequest(ClusterState currentState, MigrateToDataStreamClusterStateUpdateRequest request) {
        IndexAbstraction ia = currentState.metadata().getIndicesLookup().get(request.aliasName);
        if (ia == null || ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] does not exist");
        }
        if (ia.getWriteIndex() == null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] must specify a write index");
        }

        // check for "clean" alias without routing or filter query
        AliasMetadata aliasMetadata = AliasMetadata.getFirstAliasMetadata(ia);
        assert aliasMetadata != null : "alias metadata may not be null";
        if (aliasMetadata.filteringRequired() || aliasMetadata.getIndexRouting() != null || aliasMetadata.getSearchRouting() != null) {
            throw new IllegalArgumentException("alias [" + request.aliasName + "] may not have custom filtering or routing");
        }
    }

    private static void prepareBackingIndex(
        Metadata.Builder b,
        IndexMetadata im,
        String dataStreamName,
        Function<IndexMetadata, MapperService> mapperSupplier) throws IOException {
        // hides the index, removes the original alias, and adds data stream timestamp field mapper
        MappingMetadata mm = im.mapping();
        if (mm == null) {
            throw new IllegalArgumentException("backing index [" + im.getIndex().getName() + "] must have mappings for a timestamp field");
        }

        MapperService mapperService = mapperSupplier.apply(im);
        mapperService.merge(im, MapperService.MergeReason.MAPPING_RECOVERY);
        mapperService.merge("_doc", Map.of(DataStreamTimestampFieldMapper.NAME, Map.of("enabled", true)),
            MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapper = mapperService.documentMapper();

        b.put(IndexMetadata.builder(im)
            .removeAlias(dataStreamName)
            .settings(Settings.builder().put(im.getSettings()).put("index.hidden", "true").build())
            .settingsVersion(im.getSettingsVersion() + 1)
            .mappingVersion(im.getMappingVersion() + 1)
            .putMapping(new MappingMetadata(mapper)));
    }

    // package-visible for testing
    static void validateBackingIndices(ClusterState currentState, String dataStreamName) {
        IndexAbstraction ia = currentState.metadata().getIndicesLookup().get(dataStreamName);
        if (ia == null || ia.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("alias [" + dataStreamName + "] does not exist");
        }
        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) ia;

        // ensure that no other aliases reference indices
        List<String> indicesWithOtherAliases = new ArrayList<>();
        for (IndexMetadata im : alias.getIndices()) {
            if (im.getAliases().size() > 1 || im.getAliases().containsKey(alias.getName()) == false) {
                indicesWithOtherAliases.add(im.getIndex().getName());
            }
        }
        if (indicesWithOtherAliases.size() > 0) {
            throw new IllegalArgumentException("other aliases referencing indices [" +
                Strings.collectionToCommaDelimitedString(indicesWithOtherAliases) + "] must be removed before migrating to a data stream");
        }
    }

    @SuppressWarnings("rawtypes")
    public static final class MigrateToDataStreamClusterStateUpdateRequest extends ClusterStateUpdateRequest {

        private final String aliasName;

        public MigrateToDataStreamClusterStateUpdateRequest(String aliasName,
                                                            TimeValue masterNodeTimeout,
                                                            TimeValue timeout) {
            this.aliasName = aliasName;
            masterNodeTimeout(masterNodeTimeout);
            ackTimeout(timeout);
        }
    }

}
