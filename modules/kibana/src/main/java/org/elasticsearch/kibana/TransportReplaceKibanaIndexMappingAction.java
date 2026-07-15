/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Master-node action that installs a replacement mapping on a Kibana saved-objects system index.
 * <p>
 * The flow is:
 * <ol>
 *     <li>Validate the target is a single-shard {@code .kibana_*} system index.</li>
 *     <li>Force-flush the index so any translog operations that still reference dropped fields are folded into a
 *     Lucene commit and will not be re-parsed (against the shrunken mapping) by peer recovery or replica resync.</li>
 *     <li>Submit a cluster-state update that validates the submitted mapping in a fresh {@link MapperService}
 *     (crucially <em>not</em> pre-loaded with the existing mapping, which is what makes this a replacement instead
 *     of the usual additive merge) and writes it into {@link IndexMetadata} with a mapping version bump.</li>
 * </ol>
 * Data nodes rebuild their in-memory mapper verbatim from the published mapping (see
 * {@code MapperService#updateMapping}), so no server-side changes are required for the reduced mapping to take
 * effect cluster-wide.
 */
public class TransportReplaceKibanaIndexMappingAction extends TransportMasterNodeAction<
    ReplaceKibanaIndexMappingAction.Request,
    AcknowledgedResponse> {

    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;
    private final Client client;

    @Inject
    public TransportReplaceKibanaIndexMappingAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ProjectResolver projectResolver,
        Client client
    ) {
        super(
            ReplaceKibanaIndexMappingAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ReplaceKibanaIndexMappingAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indicesService = indicesService;
        this.projectResolver = projectResolver;
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        ReplaceKibanaIndexMappingAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final IndexMetadata indexMetadata = resolveKibanaSystemIndex(state, request.index());
        final FlushRequest flushRequest = new FlushRequest(indexMetadata.getIndex().getName()).force(true).waitIfOngoing(true);
        client.admin()
            .indices()
            .flush(flushRequest, listener.delegateFailureAndWrap((l, flushResponse) -> submitReplaceMappingTask(request, l)));
    }

    private void submitReplaceMappingTask(ReplaceKibanaIndexMappingAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("kibana-replace-mapping [" + request.index() + "]", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return applyReplacement(currentState, request);
            }
        });
    }

    @SuppressWarnings("deprecation") // submitUnbatchedStateUpdateTask is fine for this infrequent administrative operation
    private void submitUnbatchedTask(String source, AckedClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private ClusterState applyReplacement(ClusterState currentState, ReplaceKibanaIndexMappingAction.Request request) throws Exception {
        // Always re-resolve from the current state: the index may have changed since the request was validated.
        final IndexMetadata indexMetadata = resolveKibanaSystemIndex(currentState, request.index());
        final ProjectMetadata project = currentState.metadata().projectFor(indexMetadata.getIndex());
        // Strip the existing mapping from the metadata handed to createIndexMapperServiceForValidation: when the index
        // is live on this node, that method reuses the index's current DocumentMapper as an optimization, which would
        // turn the merge below back into the usual additive merge and silently retain the fields being dropped.
        final IndexMetadata unmappedIndexMetadata = IndexMetadata.builder(indexMetadata).putMapping((MappingMetadata) null).build();
        try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(unmappedIndexMetadata)) {
            // The fresh MapperService has no existing mapping, so this merge validates and builds the submitted
            // mapping exactly as-is; nothing from the current mapping is carried over.
            DocumentMapper newMapper = mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                new CompressedXContent(request.mappingSource()),
                MapperService.MergeReason.MAPPING_UPDATE
            );
            MappingMetadata newMapping = new MappingMetadata(newMapper);
            if (indexMetadata.mapping() != null && newMapping.source().equals(indexMetadata.mapping().source())) {
                return currentState;
            }
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                .putMapping(newMapping)
                .putInferenceFields(newMapper.mappers().inferenceFields())
                .mappingVersion(indexMetadata.getMappingVersion() + 1)
                .mappingsUpdatedVersion(IndexVersion.current());
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            metadataBuilder.getProject(project.id()).put(indexMetadataBuilder);
            return ClusterState.builder(currentState).metadata(metadataBuilder).build();
        }
    }

    private IndexMetadata resolveKibanaSystemIndex(ClusterState state, String indexName) {
        final ProjectMetadata project = state.metadata().getProject(projectResolver.getProjectId());
        final IndexMetadata indexMetadata = project.index(indexName);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(indexName);
        }
        if (indexMetadata.isSystem() == false
            || KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(indexMetadata.getIndex().getName()) == false) {
            throw new IllegalArgumentException(
                "mapping replacement is only supported for Kibana saved objects system indices, not [" + indexName + "]"
            );
        }
        if (indexMetadata.getNumberOfShards() != 1) {
            throw new IllegalArgumentException(
                "mapping replacement requires a single-shard index, but [" + indexName + "] has [" + indexMetadata.getNumberOfShards() + "]"
            );
        }
        return indexMetadata;
    }

    @Override
    protected ClusterBlockException checkBlock(ReplaceKibanaIndexMappingAction.Request request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE, new String[] { request.index() });
    }
}
