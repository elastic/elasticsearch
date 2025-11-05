/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexGraveyard.IndexGraveyardDiff;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * An event received by the local node, signaling that the cluster state has changed.
 */
public class ClusterChangedEvent {

    private final String source;

    private final ClusterState previousState;

    private final ClusterState state;

    private final DiscoveryNodes.Delta nodesDelta;

    private final ProjectsDelta projectsDelta;

    /**
     * Constructs a new cluster changed event with the specified source, new state, and previous state.
     * This constructor automatically calculates the differences between the states for efficient comparison.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClusterChangedEvent event = new ClusterChangedEvent("source", newState, oldState);
     * if (event.routingTableChanged()) {
     *     // Handle routing changes
     * }
     * }</pre>
     *
     * @param source the descriptive source that caused this cluster state change (e.g., "reroute", "create-index")
     * @param state the new cluster state after the change
     * @param previousState the cluster state before the change
     * @throws NullPointerException if any parameter is null
     */
    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(previousState, "previousState must not be null");
        this.source = source;
        this.state = state;
        this.previousState = previousState;
        this.nodesDelta = state.nodes().delta(previousState.nodes());
        this.projectsDelta = calculateProjectDelta(previousState.metadata(), state.metadata());
    }

    /**
     * Returns the source that caused this cluster event to be raised.
     * The source is a descriptive string indicating the operation or action that triggered the cluster state change.
     *
     * @return the source description (e.g., "reroute", "create-index", "node-join")
     */
    public String source() {
        return this.source;
    }

    /**
     * Returns the new cluster state that caused this change event.
     * This represents the current state of the cluster after the change has been applied.
     *
     * @return the new cluster state
     */
    public ClusterState state() {
        return this.state;
    }

    /**
     * Returns the previous cluster state before this change event.
     * This represents the state of the cluster immediately before the change was applied.
     *
     * @return the previous cluster state
     */
    public ClusterState previousState() {
        return this.previousState;
    }

    /**
     * Determines if the routing tables for all indices have changed between the previous and current cluster states.
     * This uses object reference equality rather than deep equals comparison for efficiency, relying on the
     * immutability of {@link GlobalRoutingTable}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (event.routingTableChanged()) {
     *     logger.info("Routing table has changed, updating shard allocations");
     * }
     * }</pre>
     *
     * @return {@code true} if the routing tables have changed, {@code false} otherwise
     */
    public boolean routingTableChanged() {
        // GlobalRoutingTable.routingTables is immutable, meaning that we can simply test the reference equality of the global routing
        // table.
        return state.globalRoutingTable() != previousState.globalRoutingTable();
    }

    /**
     * Determines if the routing table has changed for the specified index.
     * This uses object reference equality rather than deep equals comparison for efficiency.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Index myIndex = new Index("my-index", "uuid");
     * if (event.indexRoutingTableChanged(myIndex)) {
     *     // Handle index-specific routing changes
     * }
     * }</pre>
     *
     * @param index the index to check for routing table changes
     * @return {@code true} if the routing table for the specified index has changed, {@code false} otherwise
     * @throws NullPointerException if index is null
     */
    public boolean indexRoutingTableChanged(Index index) {
        Objects.requireNonNull(index, "index must not be null");
        final Optional<IndexRoutingTable> indexRoutingTable = state.globalRoutingTable().indexRouting(state.metadata(), index);

        final Optional<IndexRoutingTable> previousIndexRoutingTable = previousState.globalRoutingTable()
            .indexRouting(previousState.metadata(), index);

        if (indexRoutingTable.isEmpty() || previousIndexRoutingTable.isEmpty()) {
            return indexRoutingTable.isEmpty() != previousIndexRoutingTable.isEmpty();
        }
        return indexRoutingTable.get() != previousIndexRoutingTable.get();
    }

    /**
     * Returns the list of indices that were deleted in this cluster state change.
     * The method determines deletions using either tombstones (if the cluster state is not fully recovered)
     * or by comparing index metadata between the previous and current states.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Index> deletedIndices = event.indicesDeleted();
     * for (Index index : deletedIndices) {
     *     logger.info("Index {} was deleted", index.getName());
     * }
     * }</pre>
     *
     * @return a list of deleted indices, or an empty list if no indices were deleted
     */
    public List<Index> indicesDeleted() {
        if (previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // working off of a non-initialized previous state, so use the tombstones for index deletions
            return indicesDeletedFromTombstones();
        } else {
            // examine the diffs in index metadata between the previous and new cluster states to get the deleted indices
            return indicesDeletedFromClusterState();
        }
    }

    /**
     * Determines if the cluster metadata has changed between the previous and current cluster states.
     * This uses object reference equality rather than deep equals comparison for efficiency, relying
     * on the immutability of the {@link Metadata} objects.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (event.metadataChanged()) {
     *     // Handle metadata changes such as index settings or mappings updates
     * }
     * }</pre>
     *
     * @return {@code true} if the cluster metadata has changed, {@code false} otherwise
     */
    public boolean metadataChanged() {
        return state.metadata() != previousState.metadata();
    }

    /**
     * Returns the set of custom metadata type names that have changed at the cluster level.
     * A custom metadata type is included if it has been added, updated, or removed between
     * the previous and current cluster states.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> changedTypes = event.changedCustomClusterMetadataSet();
     * if (changedTypes.contains("my-custom-metadata")) {
     *     // Handle changes to specific custom metadata
     * }
     * }</pre>
     *
     * @return a set of custom metadata type names that have changed, or an empty set if none changed
     */
    public Set<String> changedCustomClusterMetadataSet() {
        return changedCustoms(state.metadata().customs(), previousState.metadata().customs());
    }

    /**
     * Returns the set of custom metadata type names that have changed at the project level.
     * A custom metadata type is included if it has been added, updated, or removed for any project
     * between the previous and current cluster states.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> changedTypes = event.changedCustomProjectMetadataSet();
     * for (String type : changedTypes) {
     *     logger.info("Project metadata type {} changed", type);
     * }
     * }</pre>
     *
     * @return a set of custom metadata type names that have changed, or an empty set if none changed
     */
    public Set<String> changedCustomProjectMetadataSet() {
        // TODO: none of the usages of these `changedCustom` methods actually need the full list; they just want to know if a specific entry
        // changed.
        Set<String> result = new HashSet<>();
        for (ProjectMetadata project : state.metadata().projects().values()) {
            ProjectMetadata previousProject = previousState.metadata().projects().get(project.id());
            if (previousProject == null) {
                result.addAll(project.customs().keySet());
                continue;
            }
            result.addAll(changedCustoms(project.customs(), previousProject.customs()));
        }
        for (ProjectMetadata previousProject : previousState.metadata().projects().values()) {
            ProjectMetadata project = state.metadata().projects().get(previousProject.id());
            if (project != null) {
                continue;
            }
            result.addAll(previousProject.customs().keySet());
        }
        return result;
    }

    /**
     * Determines if a specific custom metadata type has changed for a given project.
     * Custom metadata is considered changed if it has been added, updated, or removed between
     * the previous and current cluster states.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ProjectId projectId = ProjectId.DEFAULT;
     * if (event.customMetadataChanged(projectId, "my-custom-type")) {
     *     // Handle the change to this specific custom metadata
     * }
     * }</pre>
     *
     * @param projectId the project identifier to check
     * @param customMetadataType the custom metadata type name to check
     * @return {@code true} if the custom metadata has changed for the specified project, {@code false} otherwise
     */
    public boolean customMetadataChanged(ProjectId projectId, String customMetadataType) {
        ProjectMetadata previousProject = previousState.metadata().projects().get(projectId);
        ProjectMetadata project = state.metadata().projects().get(projectId);
        Object previousValue = previousProject == null ? null : previousProject.customs().get(customMetadataType);
        Object value = project == null ? null : project.customs().get(customMetadataType);
        return Objects.equals(previousValue, value) == false;
    }

    private <C extends Metadata.MetadataCustom<C>> Set<String> changedCustoms(
        Map<String, C> currentCustoms,
        Map<String, C> previousCustoms
    ) {
        Set<String> result = new HashSet<>();
        if (currentCustoms.equals(previousCustoms) == false) {
            for (var currentCustomMetadata : currentCustoms.entrySet()) {
                // new custom md added or existing custom md changed
                if (previousCustoms.containsKey(currentCustomMetadata.getKey()) == false
                    || currentCustomMetadata.getValue().equals(previousCustoms.get(currentCustomMetadata.getKey())) == false) {
                    result.add(currentCustomMetadata.getKey());
                }
            }
            // existing custom md deleted
            for (var previousCustomMetadata : previousCustoms.entrySet()) {
                if (currentCustoms.containsKey(previousCustomMetadata.getKey()) == false) {
                    result.add(previousCustomMetadata.getKey());
                }
            }
        }
        return result;
    }

    /**
     * Determines if the {@link IndexMetadata} has changed between two metadata instances.
     * This uses object reference equality rather than deep equals comparison for efficiency.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * IndexMetadata oldMeta = previousState.metadata().index("my-index");
     * IndexMetadata newMeta = currentState.metadata().index("my-index");
     * if (ClusterChangedEvent.indexMetadataChanged(oldMeta, newMeta)) {
     *     // Handle index metadata changes
     * }
     * }</pre>
     *
     * @param metadata1 the first index metadata instance
     * @param metadata2 the second index metadata instance
     * @return {@code true} if the metadata instances are different, {@code false} if they are the same
     * @throws AssertionError if either metadata parameter is null (when assertions are enabled)
     */
    public static boolean indexMetadataChanged(IndexMetadata metadata1, IndexMetadata metadata2) {
        assert metadata1 != null && metadata2 != null;
        // no need to check on version, since disco modules will make sure to use the
        // same instance if its a version match
        return metadata1 != metadata2;
    }

    /**
     * Determines if the cluster-level blocks have changed between the previous and current cluster states.
     * This uses object reference equality rather than deep equals comparison for efficiency.
     *
     * @return {@code true} if the cluster blocks have changed, {@code false} otherwise
     */
    public boolean blocksChanged() {
        return state.blocks() != previousState.blocks();
    }

    /**
     * Determines if the local node is the elected master node of the cluster in the current state.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (event.localNodeMaster()) {
     *     // Execute master-specific logic
     * }
     * }</pre>
     *
     * @return {@code true} if the local node is the elected master, {@code false} otherwise
     */
    public boolean localNodeMaster() {
        return state.nodes().isLocalNodeElectedMaster();
    }

    /**
     * Returns the delta of node changes between the previous and current cluster states.
     * The delta includes information about nodes that were added, removed, or had their master status change.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DiscoveryNodes.Delta delta = event.nodesDelta();
     * for (DiscoveryNode node : delta.addedNodes()) {
     *     logger.info("Node {} joined the cluster", node.getName());
     * }
     * }</pre>
     *
     * @return the discovery nodes delta representing node changes
     */
    public DiscoveryNodes.Delta nodesDelta() {
        return this.nodesDelta;
    }

    /**
     * Determines if any nodes have been removed from the cluster since the previous cluster state.
     *
     * @return {@code true} if one or more nodes were removed, {@code false} otherwise
     */
    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }

    /**
     * Determines if any nodes have been added to the cluster since the previous cluster state.
     *
     * @return {@code true} if one or more nodes were added, {@code false} otherwise
     */
    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    /**
     * Determines if the set of nodes in the cluster has changed (either added or removed)
     * since the previous cluster state.
     *
     * @return {@code true} if any nodes were added or removed, {@code false} otherwise
     */
    public boolean nodesChanged() {
        return nodesRemoved() || nodesAdded();
    }

    /**
     * Returns the delta of project changes between the previous and current cluster states.
     * The delta includes information about projects that were added or removed.
     *
     * @return the projects delta representing project changes
     */
    public ProjectsDelta projectDelta() {
        return projectsDelta;
    }

    /**
     * Determines if the current cluster state represents an entirely new cluster.
     * This occurs when a node joins a cluster for the first time, or when the node receives
     * a cluster state update from a brand new cluster with a different UUID. This typically
     * happens when a master node is elected that has never been part of the cluster before
     * or has had its data directory wiped.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (event.isNewCluster()) {
     *     logger.warn("New cluster detected, previous cluster UUID was lost");
     * }
     * }</pre>
     *
     * @return {@code true} if this represents a new cluster with a different UUID, {@code false} otherwise
     */
    public boolean isNewCluster() {
        final String prevClusterUUID = previousState.metadata().clusterUUID();
        final String currClusterUUID = state.metadata().clusterUUID();
        return prevClusterUUID.equals(currClusterUUID) == false;
    }

    // Get the deleted indices by comparing the index metadatas in the previous and new cluster states.
    // If an index exists in the previous cluster state, but not in the new cluster state, it must have been deleted.
    private List<Index> indicesDeletedFromClusterState() {
        // If the new cluster state has a new cluster UUID, the likely scenario is that a node was elected
        // master that has had its data directory wiped out, in which case we don't want to delete the indices and lose data;
        // rather we want to import them as dangling indices instead. So we check here if the cluster UUID differs from the previous
        // cluster UUID, in which case, we don't want to delete indices that the master erroneously believes shouldn't exist.
        // See test DiscoveryWithServiceDisruptionsIT.testIndicesDeleted()
        // See discussion on https://github.com/elastic/elasticsearch/pull/9952 and
        // https://github.com/elastic/elasticsearch/issues/11665
        if (metadataChanged() == false || isNewCluster()) {
            return Collections.emptyList();
        }
        Set<Index> deleted = null;
        final Metadata previousMetadata = previousState.metadata();
        final Metadata currentMetadata = state.metadata();

        for (ProjectMetadata project : currentMetadata.projects().values()) {
            ProjectMetadata previousProject = previousMetadata.projects().get(project.id());
            // No indices could have been deleted if this project didn't exist in the previous cluster state.
            if (previousProject == null) {
                continue;
            }
            if (project.indices() != previousProject.indices()) {
                for (IndexMetadata index : previousProject.indices().values()) {
                    IndexMetadata current = project.index(index.getIndex());
                    if (current == null) {
                        if (deleted == null) {
                            deleted = new HashSet<>();
                        }
                        deleted.add(index.getIndex());
                    }
                }
            }

            final IndexGraveyard currentGraveyard = project.indexGraveyard();
            final IndexGraveyard previousGraveyard = previousProject.indexGraveyard();

            // Look for new entries in the index graveyard, where there's no corresponding index in the
            // previous metadata. This indicates that a dangling index has been explicitly deleted, so
            // each node should make sure to delete any related data.
            if (currentGraveyard != previousGraveyard) {
                final IndexGraveyardDiff indexGraveyardDiff = (IndexGraveyardDiff) currentGraveyard.diff(previousGraveyard);
                final List<IndexGraveyard.Tombstone> added = indexGraveyardDiff.getAdded();
                if (added.isEmpty() == false) {
                    if (deleted == null) {
                        deleted = new HashSet<>();
                    }
                    for (IndexGraveyard.Tombstone tombstone : added) {
                        deleted.add(tombstone.getIndex());
                    }
                }
            }
        }

        // If a project is removed, we remove all its indices as well.
        for (ProjectMetadata previousProject : previousMetadata.projects().values()) {
            if (currentMetadata.projects().containsKey(previousProject.id())) {
                continue;
            }
            for (IndexMetadata index : previousProject.indices().values()) {
                if (deleted == null) {
                    deleted = new HashSet<>();
                }
                deleted.add(index.getIndex());
            }
        }

        return deleted == null ? Collections.<Index>emptyList() : new ArrayList<>(deleted);
    }

    private List<Index> indicesDeletedFromTombstones() {
        // We look at the full tombstones list to see which indices need to be deleted. In the case of
        // a valid previous cluster state, indicesDeletedFromClusterState() will be used to get the deleted
        // list, so a diff doesn't make sense here. When a node (re)joins the cluster, its possible for it
        // to re-process the same deletes or process deletes about indices it never knew about. This is not
        // an issue because there are safeguards in place in the delete store operation in case the index
        // folder doesn't exist on the file system.
        return state.metadata()
            .projects()
            .values()
            .stream()
            .flatMap(project -> project.indexGraveyard().getTombstones().stream().map(IndexGraveyard.Tombstone::getIndex))
            .toList();
    }

    private static ProjectsDelta calculateProjectDelta(Metadata previousMetadata, Metadata currentMetadata) {
        if (previousMetadata == currentMetadata
            || (previousMetadata.projects().size() == 1
                && previousMetadata.hasProject(ProjectId.DEFAULT)
                && currentMetadata.projects().size() == 1
                && currentMetadata.hasProject(ProjectId.DEFAULT))) {
            return ProjectsDelta.EMPTY;
        }

        final Set<ProjectId> added = Collections.unmodifiableSet(
            Sets.difference(currentMetadata.projects().keySet(), previousMetadata.projects().keySet())
        );
        final Set<ProjectId> removed = Collections.unmodifiableSet(
            Sets.difference(previousMetadata.projects().keySet(), currentMetadata.projects().keySet())
        );
        // TODO: Enable the following assertions once tests no longer add or remove default projects
        // assert added.contains(ProjectId.DEFAULT) == false;
        // assert removed.contains(ProjectId.DEFAULT) == false;
        return new ProjectsDelta(added, removed);
    }

    public record ProjectsDelta(Set<ProjectId> added, Set<ProjectId> removed) {
        private static final ProjectsDelta EMPTY = new ProjectsDelta(Set.of(), Set.of());

        public boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }
    }
}
