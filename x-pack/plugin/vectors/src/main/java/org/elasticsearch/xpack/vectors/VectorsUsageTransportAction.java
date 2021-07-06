/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class VectorsUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Map<String, IndexVectorUsage> usageByIndexUuid = ConcurrentCollections.newConcurrentMap();

    // updated & read on cluster applier thread only, just used to skip work when the cluster state update didn't change the metadata
    private long lastMetadataVersion = Long.MIN_VALUE;

    @Inject
    public VectorsUsageTransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
            XPackUsageFeatureAction.VECTORS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver);
        clusterService.addListener(this::removeOldCacheEntries);
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener) {

        int numDenseVectorFields = 0;
        int numDenseVectorDims = 0;

        if (state != null) {
            for (IndexMetadata indexMetadata : state.metadata()) {

                final IndexVectorUsage usage = getUsage(indexMetadata);

                numDenseVectorFields += usage.getNumDenseVectorFields();
                numDenseVectorDims += usage.getNumDenseVectorDims();

                if (usage.isCached() == false) {
                    // Update the cache. We're fairly loose with the precise details of the contents of the cache, allowing for a certain
                    // amount of duplicated/discarded work in cases where there are a lot of mapping updates and concurrent calls to this
                    // API, on the grounds that this API is normally not called concurrently and that mapping updates are mostly rare.
                    usageByIndexUuid.compute(indexMetadata.getIndexUUID(), (indexUuid, currentUsage) -> {
                        if (currentUsage != null && usage.getMappingVersion() <= currentUsage.getMappingVersion()) {
                            return currentUsage;
                        } else {
                            return usage.forCache();
                        }
                    });
                }
            }
        }

        final int avgDenseVectorDims;
        if (numDenseVectorFields > 0) {
            avgDenseVectorDims = numDenseVectorDims / numDenseVectorFields;
        } else {
            avgDenseVectorDims = 0;
        }

        listener.onResponse(new XPackUsageFeatureResponse(new VectorsFeatureSetUsage(true, numDenseVectorFields, avgDenseVectorDims)));
    }

    private IndexVectorUsage getUsage(IndexMetadata indexMetadata) {
        final IndexVectorUsage cachedUsage = usageByIndexUuid.get(indexMetadata.getIndexUUID());
        if (cachedUsage != null && cachedUsage.getMappingVersion() == indexMetadata.getMappingVersion()) {
            assert cachedUsage.isCached();
            return cachedUsage;
        }

        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return new IndexVectorUsage(indexMetadata.getMappingVersion(), 0, 0);
        }

        final Map<String, Object> mappings = mappingMetadata.getSourceAsMap();
        if (mappings.containsKey("properties") == false) {
            return new IndexVectorUsage(indexMetadata.getMappingVersion(), 0, 0);
        }

        int numDenseVectorFields = 0;
        int numDenseVectorDims = 0;
        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> fieldMappings =
            (Map<String, Map<String, Object>>) mappings.get("properties");
        for (Map<String, Object> typeDefinition : fieldMappings.values()) {
            String fieldType = (String) typeDefinition.get("type");
            if (fieldType != null) {
                if (fieldType.equals(DenseVectorFieldMapper.CONTENT_TYPE)) {
                    numDenseVectorFields++;
                    numDenseVectorDims += (int) typeDefinition.get("dims");
                }
            }
        }

        return new IndexVectorUsage(indexMetadata.getMappingVersion(), numDenseVectorFields, numDenseVectorDims);
    }

    private void removeOldCacheEntries(ClusterChangedEvent event) {
        final Metadata metadata = event.state().metadata();
        assert lastMetadataVersion <= metadata.version();
        if (lastMetadataVersion < metadata.version()) {
            lastMetadataVersion = metadata.version();
            usageByIndexUuid.keySet().retainAll(StreamSupport.stream(metadata.indices().spliterator(), false)
                .map(i -> i.value.getIndexUUID())
                .collect(Collectors.toSet()));
        }
    }

    // exposed for tests
    int cacheSize() {
        return usageByIndexUuid.size();
    }

    private static class IndexVectorUsage {
        private final boolean cached;
        private final long mappingVersion;
        private final int numDenseVectorFields;
        private final int numDenseVectorDims;

        IndexVectorUsage(long mappingVersion, int numDenseVectorFields, int numDenseVectorDims) {
            this(false, mappingVersion, numDenseVectorFields, numDenseVectorDims);
        }

        private IndexVectorUsage(boolean cached, long mappingVersion, int numDenseVectorFields, int numDenseVectorDims) {
            this.cached = cached;
            this.mappingVersion = mappingVersion;
            this.numDenseVectorFields = numDenseVectorFields;
            this.numDenseVectorDims = numDenseVectorDims;
        }

        boolean isCached() {
            return cached;
        }

        long getMappingVersion() {
            return mappingVersion;
        }

        int getNumDenseVectorFields() {
            return numDenseVectorFields;
        }

        int getNumDenseVectorDims() {
            return numDenseVectorDims;
        }

        IndexVectorUsage forCache() {
            return new IndexVectorUsage(true, mappingVersion, numDenseVectorFields, numDenseVectorDims);
        }
    }

}
