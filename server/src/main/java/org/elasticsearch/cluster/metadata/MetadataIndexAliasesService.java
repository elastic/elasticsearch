/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.AliasAction.NewAliasValidator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.CloseUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting add and remove aliases requests
 */
public class MetadataIndexAliasesService {

    private final IndicesService indicesService;

    private final NamedXContentRegistry xContentRegistry;

    private final ClusterStateTaskExecutor<ApplyAliasesTask> executor;
    private final MasterServiceTaskQueue<ApplyAliasesTask> taskQueue;
    private final ClusterService clusterService;

    @Inject
    public MetadataIndexAliasesService(
        ClusterService clusterService,
        IndicesService indicesService,
        NamedXContentRegistry xContentRegistry
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.xContentRegistry = xContentRegistry;
        this.executor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(ApplyAliasesTask applyAliasesTask, ClusterState clusterState) {
                return new Tuple<>(
                    applyAliasActions(
                        clusterState.projectState(applyAliasesTask.request().projectId()),
                        applyAliasesTask.request().actions()
                    ),
                    applyAliasesTask
                );
            }
        };
        this.taskQueue = clusterService.createTaskQueue("index-aliases", Priority.URGENT, this.executor);
    }

    public void indicesAliases(
        final IndicesAliasesClusterStateUpdateRequest request,
        final ActionListener<IndicesAliasesResponse> listener
    ) {
        taskQueue.submitTask("index-aliases", new ApplyAliasesTask(request, listener), null); // TODO use request.masterNodeTimeout() here?
    }

    /**
     * Handles the cluster state transition to a version that reflects the provided {@link AliasAction}s.
     */
    public ClusterState applyAliasActions(ProjectState projectState, Iterable<AliasAction> actions) {
        ClusterState currentState = projectState.cluster();
        ProjectId projectId = projectState.projectId();
        List<Index> indicesToClose = new ArrayList<>();
        Map<String, IndexService> indices = new HashMap<>();
        ProjectMetadata currentProjectMetadata = projectState.metadata();
        try {
            boolean changed = false;
            // Gather all the indexes that must be removed first so:
            // 1. We don't cause error when attempting to replace an index with a alias of the same name.
            // 2. We don't allow removal of aliases from indexes that we're just going to delete anyway. That'd be silly.
            Set<Index> indicesToDelete = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    IndexMetadata index = currentProjectMetadata.indices().get(action.getIndex());
                    if (index == null) {
                        throw new IndexNotFoundException(action.getIndex());
                    }
                    validateAliasTargetIsNotDSBackingIndex(currentProjectMetadata, action);
                    indicesToDelete.add(index.getIndex());
                    changed = true;
                }
            }
            // Remove the indexes if there are any to remove
            if (changed) {
                currentState = MetadataDeleteIndexService.deleteIndices(projectState, indicesToDelete, clusterService.getSettings());
                currentProjectMetadata = currentState.metadata().getProject(projectId);
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(currentProjectMetadata);
            // Run the remaining alias actions
            final Set<String> maybeModifiedIndices = new HashSet<>();
            for (AliasAction action : actions) {
                if (action.removeIndex()) {
                    // Handled above
                    continue;
                }

                /* It is important that we look up the index using the metadata builder we are modifying so we can remove an
                 * index and replace it with an alias. */
                Function<String, String> lookup = name -> {
                    IndexMetadata imd = metadata.get(name);
                    if (imd != null) {
                        return imd.getIndex().getName();
                    }
                    DataStream dataStream = metadata.dataStream(name);
                    if (dataStream != null) {
                        return dataStream.getName();
                    }
                    return null;
                };

                // Handle the actions that do data streams aliases separately:
                DataStream dataStream = metadata.dataStream(action.getIndex());
                if (dataStream != null) {
                    NewAliasValidator newAliasValidator = (alias, indexRouting, searchRouting, filter, writeIndex) -> {
                        AliasValidator.validateAlias(alias, action.getIndex(), indexRouting, lookup);
                        if (Strings.hasLength(filter)) {
                            for (Index index : dataStream.getIndices()) {
                                IndexMetadata imd = metadata.get(index.getName());
                                if (imd == null) {
                                    throw new IndexNotFoundException(action.getIndex());
                                }
                                IndexSettings.MODE.get(imd.getSettings()).validateAlias(indexRouting, searchRouting);
                                validateFilter(indicesToClose, indices, action, imd, alias, filter);
                            }
                        }
                    };
                    if (action.apply(newAliasValidator, metadata, null)) {
                        changed = true;
                    }
                    continue;
                }

                IndexMetadata index = metadata.get(action.getIndex());
                if (index == null) {
                    throw new IndexNotFoundException(action.getIndex());
                }
                validateAliasTargetIsNotDSBackingIndex(currentProjectMetadata, action);
                NewAliasValidator newAliasValidator = (alias, indexRouting, searchRouting, filter, writeIndex) -> {
                    AliasValidator.validateAlias(alias, action.getIndex(), indexRouting, lookup);
                    IndexSettings.MODE.get(index.getSettings()).validateAlias(indexRouting, searchRouting);
                    if (Strings.hasLength(filter)) {
                        validateFilter(indicesToClose, indices, action, index, alias, filter);
                    }
                };
                if (action.apply(newAliasValidator, metadata, index)) {
                    changed = true;
                    maybeModifiedIndices.add(index.getIndex().getName());
                }
            }

            for (final String maybeModifiedIndex : maybeModifiedIndices) {
                final IndexMetadata currentIndexMetadata = currentProjectMetadata.index(maybeModifiedIndex);
                final IndexMetadata newIndexMetadata = metadata.get(maybeModifiedIndex);
                // only increment the aliases version if the aliases actually changed for this index
                if (currentIndexMetadata.getAliases().equals(newIndexMetadata.getAliases()) == false) {
                    assert currentIndexMetadata.getAliasesVersion() == newIndexMetadata.getAliasesVersion();
                    metadata.put(new IndexMetadata.Builder(newIndexMetadata).aliasesVersion(1 + currentIndexMetadata.getAliasesVersion()));
                }
            }

            if (changed) {
                ProjectMetadata updatedMetadata = metadata.build();
                // even though changes happened, they resulted in 0 actual changes to metadata
                // i.e. remove and add the same alias to the same index
                if (updatedMetadata.equalsAliases(currentProjectMetadata) == false) {
                    return ClusterState.builder(currentState).putProjectMetadata(updatedMetadata).build();
                }
            }
            return currentState;
        } finally {
            for (Index index : indicesToClose) {
                indicesService.removeIndex(
                    index,
                    NO_LONGER_ASSIGNED,
                    "created for alias processing",
                    CloseUtils.NO_SHARDS_CREATED_EXECUTOR,
                    ActionListener.noop()
                );
            }
        }
    }

    // Visible for testing purposes
    ClusterStateTaskExecutor<ApplyAliasesTask> getExecutor() {
        return executor;
    }

    private void validateFilter(
        List<Index> indicesToClose,
        Map<String, IndexService> indices,
        AliasAction action,
        IndexMetadata index,
        String alias,
        String filter
    ) {
        IndexService indexService = indices.get(index.getIndex().getName());
        if (indexService == null) {
            indexService = indicesService.indexService(index.getIndex());
            if (indexService == null) {
                // temporarily create the index and add mappings so we can parse the filter
                try {
                    indexService = indicesService.createIndex(index, emptyList(), false);
                    indicesToClose.add(index.getIndex());
                } catch (IOException e) {
                    throw new ElasticsearchException("Failed to create temporary index for parsing the alias", e);
                }
                indexService.mapperService().merge(index, MapperService.MergeReason.MAPPING_RECOVERY);
            }
            indices.put(action.getIndex(), indexService);
        }
        // the context is only used for validation so it's fine to pass fake values for the shard id,
        // but the current timestamp should be set to real value as we may use `now` in a filtered alias
        AliasValidator.validateAliasFilter(
            alias,
            filter,
            indexService.newSearchExecutionContext(0, 0, null, System::currentTimeMillis, null, emptyMap()),
            xContentRegistry
        );
    }

    private static void validateAliasTargetIsNotDSBackingIndex(ProjectMetadata projectMetadata, AliasAction action) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(action.getIndex());
        assert indexAbstraction != null : "invalid cluster metadata. index [" + action.getIndex() + "] was not found";
        if (indexAbstraction.getParentDataStream() != null) {
            throw new IllegalArgumentException(
                "The provided index ["
                    + action.getIndex()
                    + "] is a backing index belonging to data stream ["
                    + indexAbstraction.getParentDataStream().getName()
                    + "]. Data stream backing indices don't support alias operations."
            );
        }
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    record ApplyAliasesTask(IndicesAliasesClusterStateUpdateRequest request, ActionListener<IndicesAliasesResponse> listener)
        implements
            ClusterStateTaskListener,
            ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(IndicesAliasesResponse.build(request.actionResults()));
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(IndicesAliasesResponse.NOT_ACKNOWLEDGED);
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(IndicesAliasesResponse.NOT_ACKNOWLEDGED);
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }
    }
}
