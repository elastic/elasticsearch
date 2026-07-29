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
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;

/**
 * Master-node action that installs a replacement mapping on a Kibana saved-objects system index.
 * <p>
 * The flow is:
 * <ol>
 *     <li>Validate the target is a single-shard {@code .kibana_*} system index.</li>
 *     <li>Force-flush the index so any translog operations that still reference fields not in the new mapping are folded
 *     into a Lucene commit and will not be re-parsed (against the reduced mapping) by peer recovery or replica resync.</li>
 *     <li>Fetch the set of Lucene field names from {@link org.apache.lucene.index.FieldInfos} on the primary shard.
 *     Lucene permanently records the shape (index options, doc-values type, etc.) of every field name a shard has ever
 *     indexed — even after all values are purged and merged away — so introducing a field name under a different type
 *     would be accepted by the mapping layer but fail on the first document write with a confusing shard-level error.</li>
 *     <li>Submit a cluster-state update that validates the submitted mapping in a fresh {@link MapperService}
 *     (crucially <em>not</em> pre-loaded with the existing mapping, which is what makes this a replacement instead of
 *     the usual additive merge) and checks that no net-new field in the replacement conflicts with the FieldInfos set
 *     collected in the previous step.</li>
 * </ol>
 * Data nodes rebuild their in-memory mapper verbatim from the published mapping (see
 * {@code MapperService#updateMapping}), so no server-side changes are required for the reduced mapping to take effect
 * cluster-wide.
 * <p>
 * No tombstone state is stored in index metadata. Lucene's own FieldInfos — which persist through merges for the
 * lifetime of a shard whose segments are never replaced — serve as the authoritative record of retired field names.
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
        client.admin().indices().flush(flushRequest, listener.delegateFailureAndWrap((l, ignored) -> {
            final KibanaGetFieldInfosAction.Request fieldInfosRequest = new KibanaGetFieldInfosAction.Request(
                indexMetadata.getIndex().getName()
            );
            client.execute(KibanaGetFieldInfosAction.INSTANCE, fieldInfosRequest, l.delegateFailureAndWrap((l2, fieldInfosResponse) -> {
                submitReplaceMappingTask(request, fieldInfosResponse.fieldNames(), l2);
            }));
        }));
    }

    private void submitReplaceMappingTask(
        ReplaceKibanaIndexMappingAction.Request request,
        Set<String> fieldInfoNames,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask("kibana-replace-mapping [" + request.index() + "]", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return applyReplacement(currentState, request, fieldInfoNames);
            }
        });
    }

    @SuppressWarnings("deprecation") // submitUnbatchedStateUpdateTask is fine for this infrequent administrative operation
    private void submitUnbatchedTask(String source, AckedClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private ClusterState applyReplacement(
        ClusterState currentState,
        ReplaceKibanaIndexMappingAction.Request request,
        Set<String> fieldInfoNames
    ) throws Exception {
        // Always re-resolve from the current state: the index may have changed since the request was validated.
        final IndexMetadata indexMetadata = resolveKibanaSystemIndex(currentState, request.index());
        final ProjectMetadata project = currentState.metadata().projectFor(indexMetadata.getIndex());
        // Strip the existing mapping from the metadata handed to createIndexMapperServiceForValidation: when the index
        // is live on this node, that method reuses the index's current DocumentMapper as an optimization, which would
        // turn the merge below back into the usual additive merge and silently retain the fields being dropped.
        final IndexMetadata unmappedIndexMetadata = IndexMetadata.builder(indexMetadata).putMapping((MappingMetadata) null).build();
        try (
            MapperService newMapperService = indicesService.createIndexMapperServiceForValidation(unmappedIndexMetadata);
            MapperService currentMapperService = indicesService.createIndexMapperServiceForValidation(unmappedIndexMetadata)
        ) {
            // The fresh MapperService has no existing mapping, so this merge validates and builds the submitted
            // mapping exactly as-is; nothing from the current mapping is carried over.
            DocumentMapper newMapper = newMapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                new CompressedXContent(request.mappingSource()),
                MapperService.MergeReason.MAPPING_UPDATE
            );
            MappingMetadata newMapping = new MappingMetadata(newMapper);
            if (indexMetadata.mapping() != null && newMapping.source().equals(indexMetadata.mapping().source())) {
                return currentState;
            }

            // Compute the set of field paths already present in the current live mapping so we can identify which
            // paths in the new mapping are net-new (being introduced rather than retained).
            Set<String> currentPaths = Set.of();
            if (indexMetadata.mapping() != null) {
                DocumentMapper currentMapper = currentMapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    indexMetadata.mapping().source(),
                    MapperService.MergeReason.MAPPING_RECOVERY
                );
                currentPaths = allFieldPaths(currentMapper);
            }

            checkNetNewFieldsAgainstFieldInfos(allFieldPaths(newMapper), currentPaths, fieldInfoNames, request.index());

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

    /**
     * Rejects any net-new field path (present in the new mapping but absent from the current mapping) whose name is
     * already committed to the shard's Lucene FieldInfos. Lucene permanently records field shapes for the lifetime of
     * the shard, so re-introducing a retired name would be accepted by the mapping layer but fail on the next write
     * that tries to index a value for that field.
     * <p>
     * Also rejects net-new paths nested beneath a flattened-type ancestor: a flattened field stores all sub-key data
     * under its own Lucene field name plus a {@code ._keyed} companion, so introducing a separately-mapped path beneath
     * it would create a shadowing conflict at query time even without a raw Lucene shape collision.
     */
    private static void checkNetNewFieldsAgainstFieldInfos(
        Set<String> newPaths,
        Set<String> currentPaths,
        Set<String> fieldInfoNames,
        String indexName
    ) {
        for (String path : newPaths) {
            if (currentPaths.contains(path)) {
                continue;
            }
            // Exact-name check: Lucene already has this field committed under some shape.
            if (fieldInfoNames.contains(path)) {
                throw new IllegalArgumentException(
                    "field ["
                        + path
                        + "] of ["
                        + indexName
                        + "] cannot be introduced: Lucene has permanently committed this field name from a previous mapping; "
                        + "use a new (versioned) field name instead"
                );
            }
            // Ancestor check for flattened container types: a flattened field stores all sub-key data under its own
            // Lucene field plus a ._keyed companion. A separately-mapped path beneath it would shadow that data.
            int dot = path.indexOf('.');
            while (dot != -1) {
                String ancestor = path.substring(0, dot);
                if (fieldInfoNames.contains(ancestor + "._keyed")) {
                    throw new IllegalArgumentException(
                        "field ["
                            + path
                            + "] of ["
                            + indexName
                            + "] cannot be introduced beneath ["
                            + ancestor
                            + "]: that path was previously a flattened field whose sub-key data is stored under the "
                            + "ancestor's Lucene field name; use a new (versioned) ancestor name instead"
                    );
                }
                dot = path.indexOf('.', dot + 1);
            }
        }
    }

    /**
     * Returns the set of all field paths (leaf field mappers and object mapper paths, excluding the root and metadata
     * fields) declared in the given document mapper. Both leaf paths and intermediate object paths are included so
     * that the net-new check can detect conflicts at any level of the mapping hierarchy.
     */
    private static Set<String> allFieldPaths(DocumentMapper mapper) {
        Set<String> paths = new HashSet<>();
        for (Mapper m : mapper.mappers().fieldMappers()) {
            if (m instanceof FieldMapper fm && m instanceof MetadataFieldMapper == false) {
                paths.add(fm.fullPath());
            }
        }
        for (String objectPath : mapper.mappers().objectMappers().keySet()) {
            if (objectPath.isEmpty() == false) {
                paths.add(objectPath);
            }
        }
        return paths;
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
