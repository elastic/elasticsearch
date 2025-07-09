/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.DiffableUtils.KeySerializer;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.MultiProjectPendingException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A routing table for the whole cluster (potentially containing multiple projects)
 * The global routing table holds a separate {@link RoutingTable} for each project that exists in the cluster
 */
public class GlobalRoutingTable implements Iterable<RoutingTable>, Diffable<GlobalRoutingTable> {

    public static final GlobalRoutingTable EMPTY_ROUTING_TABLE = new GlobalRoutingTable(ImmutableOpenMap.of());

    private final ImmutableOpenMap<ProjectId, RoutingTable> routingTables;

    public GlobalRoutingTable(ImmutableOpenMap<ProjectId, RoutingTable> routingTables) {
        this.routingTables = routingTables;
    }

    /**
     * Constructs a new routing table with the project routing tables rebuilt based on the provided {@link RoutingNodes} parameter.
     */
    public GlobalRoutingTable rebuild(RoutingNodes routingNodes, Metadata metadata) {
        // Step 1: Iterable over all ShardRouting entries in the nodes and split them by owning project-id
        final Map<ProjectId, List<ShardRouting>> byProject = Maps.transformValues(this.routingTables, ignore -> new ArrayList<>());
        for (RoutingNode routingNode : routingNodes) {
            final String nodeContext = "Node [" + routingNode + "]";
            for (ShardRouting shardRoutingEntry : routingNode) {
                // every relocating shard has a double entry, ignore the target one.
                if (shardRoutingEntry.initializing() && shardRoutingEntry.relocatingNodeId() != null) {
                    continue;
                }
                collectProjectEntry(shardRoutingEntry, byProject, metadata, nodeContext);
            }
        }
        for (ShardRouting shardRoutingEntry : routingNodes.unassigned()) {
            collectProjectEntry(shardRoutingEntry, byProject, metadata, "unassigned-shards");
        }
        for (ShardRouting shardRoutingEntry : routingNodes.unassigned().ignored()) {
            collectProjectEntry(shardRoutingEntry, byProject, metadata, "ignored-shards");
        }

        // Step 2: Where necessary, build a new routing table for each project based on the shard routing
        final Builder builder = builder(this);
        for (var entry : byProject.entrySet()) {
            var project = entry.getKey();
            final RoutingTable oldTable = this.routingTables.get(project);
            final RoutingTable rebuiltTable = RoutingTable.of(entry.getValue());
            if (oldTable.indicesRouting().equals(rebuiltTable.indicesRouting()) == false) {
                // Only use the replacement routing table if it is different - this causes diffs to be smaller. This is necessary because
                // RoutingTable instances with the same state are not considered "equal" (unless they are the same instance).
                // This means that the MapDiff will treat the new instance as an update and produce a substantially larger diff than needed.
                builder.put(project, rebuiltTable);
            }
        }
        return builder.build();
    }

    /**
     * For the provided {@link ShardRouting}, determine the correct project (using {@link Metadata#lookupProject(Index)}),
     * and then add the {@link ShardRouting} to the correct list within the provided {@code Map}.
     *
     * @param shardRouting        The shard to add
     * @param projectRoutingLists The map to add to
     * @param metadata            The cluster metadata (used to resolve the owning project)
     * @param context             The context in which the shard routing entry was found - used to build error messages
     */
    private static void collectProjectEntry(
        ShardRouting shardRouting,
        Map<ProjectId, List<ShardRouting>> projectRoutingLists,
        Metadata metadata,
        String context
    ) {
        ProjectMetadata project = metadata.lookupProject(shardRouting.index())
            .orElseThrow(
                () -> new IllegalStateException(
                    "Found shard [" + shardRouting.shardId() + "] in " + context + ", but the index does not belong to any project"
                )
            );
        final List<ShardRouting> routingSet = projectRoutingLists.get(project.id());
        if (routingSet == null) {
            throw new IllegalStateException(
                "Shard ["
                    + shardRouting.shardId()
                    + "] is part of project ["
                    + project
                    + "]"
                    + " but the global routing table does not have an entry for that project-id"
            );
        }
        routingSet.add(shardRouting);
    }

    /**
     * TODO: Remove this method, replace with routingTable(ProjectId)
     *
     * @return <ul>
     *     <li>If this routing table is empty (has no projects), then an empty {@link RoutingTable}<li>
     *     <li>If this routing table has one element (a single project), then the {@link RoutingTable} for that project<li>
     *     <li>Otherwise throws an exception<li>
     * </ul>
     */
    @Deprecated
    public RoutingTable getRoutingTable() {
        return switch (routingTables.size()) {
            case 0 -> RoutingTable.EMPTY_ROUTING_TABLE;
            case 1 -> routingTables.values().iterator().next();
            default -> throw new MultiProjectPendingException("There are multiple project routing tables [" + routingTables.keySet() + "]");
        };
    }

    /**
     * @return THe routing table for project {@code projectId}, or throws an exception if the project does not exist in this routing table
     * @see #routingTables()
     */
    public RoutingTable routingTable(ProjectId projectId) {
        return this.routingTables.computeIfAbsent(projectId, ignore -> {
            throw new IllegalStateException("No routing table for project [" + projectId + "]");
        });
    }

    /**
     * @return A {@link Map} of project-scoped {@link RoutingTable} instances
     */
    public ImmutableOpenMap<ProjectId, RoutingTable> routingTables() {
        return this.routingTables;
    }

    /**
     * @return The number of projects in this routing table
     * @see #totalIndexCount()
     */
    public int size() {
        return routingTables.size();
    }

    @Override
    public Iterator<RoutingTable> iterator() {
        return routingTables.values().iterator();
    }

    @Override
    public Diff<GlobalRoutingTable> diff(GlobalRoutingTable previousState) {
        return new GlobalRoutingTableDiff(previousState, this);
    }

    public static Diff<GlobalRoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return new GlobalRoutingTableDiff(in);
    }

    public static GlobalRoutingTable readFrom(StreamInput in) throws IOException {
        final var table = in.readImmutableOpenMap(ProjectId::readFrom, RoutingTable::readFrom);
        return new GlobalRoutingTable(table);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(routingTables);
    }

    /**
     * @return {@code true} if this table has exactly the same {@link RoutingTable#indicesRouting() index routing} as the {@code other}
     * routing table. In this case "same" refers to object-identity, not just equality - this method is intended to be used as an
     * <em>efficient</em> shortcut to determine if two {@link GlobalRoutingTable} instances have identical state.
     */
    public boolean hasSameIndexRouting(GlobalRoutingTable other) {
        if (this.routingTables.size() != other.routingTables.size()) {
            return false;
        }
        for (var entry : this.routingTables.entrySet()) {
            var projectId = entry.getKey();
            var thisTable = entry.getValue();
            var thatTable = other.routingTables.get(projectId);
            if (thatTable == null) {
                return false;
            }
            if (thisTable.indicesRouting() != thatTable.indicesRouting()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return The total number of indices in the cluster (across all projects)
     */
    int totalIndexCount() {
        int sum = 0;
        for (var table : this) {
            sum += table.indicesRouting().size();
        }
        return sum;
    }

    /**
     * @return If this routing table contains {@code project},
     * then a new routing table that is a clone of this routing table with the specified project removed
     * Otherwise returns this routing table unchanged.
     */
    public GlobalRoutingTable removeProject(ProjectId project) {
        if (this.routingTables.containsKey(project) == false) {
            return this;
        }
        final ImmutableOpenMap.Builder<ProjectId, RoutingTable> builder = ImmutableOpenMap.builder(this.routingTables.size() - 1);
        for (var entry : this.routingTables.entrySet()) {
            if (entry.getKey().equals(project) == false) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        return new GlobalRoutingTable(builder.build());
    }

    /**
     * @return A routing table that contains all of the projects in this routing-table <i>plus</i>, an empty routing table for any project
     * in {@code projectIds} that does not already exist in this routing table.
     * The returned routing table may be {@code this} object if it satisfies these conditions.
     */
    public GlobalRoutingTable initializeProjects(Set<ProjectId> projectIds) {
        if (this.routingTables.keySet().containsAll(projectIds)) {
            return this;
        } else {
            final Map<ProjectId, RoutingTable> newTable = Maps.newMapWithExpectedSize(this.size() + projectIds.size());
            newTable.putAll(this.routingTables);
            projectIds.forEach(id -> newTable.computeIfAbsent(id, ignore -> RoutingTable.EMPTY_ROUTING_TABLE));
            return new GlobalRoutingTable(ImmutableOpenMap.builder(newTable).build());
        }
    }

    /**
     * Validates that this routing table is consistent with the set of projects that exist in the {@link Metadata}.
     *
     * @return A {@code boolean} so that this method can be used in an {@code assert} statement.
     * This will be {@code true} if the routing table and metadata are in-sync.
     * Will never return {@code false} because validation execptions always throw an exception.
     * @throws IllegalStateException if validation fails
     */
    public boolean validate(Metadata metadata) {
        Map<ProjectId, ProjectMetadata> metadataProjects = metadata.projects();
        if (metadataProjects.size() != this.routingTables.size()) {
            throw new IllegalStateException(
                "routing table has ["
                    + routingTables.size()
                    + "] projects ["
                    + routingTables.keySet()
                    + "] but metadata has ["
                    + metadataProjects.size()
                    + "] ["
                    + metadataProjects.keySet()
                    + "]"
            );
        }

        for (var entry : routingTables.entrySet()) {
            final ProjectId projectId = entry.getKey();
            final ProjectMetadata projectMetadata = metadataProjects.get(projectId);
            if (projectMetadata == null) {
                throw new IllegalStateException("Routing table has an entry for project [" + projectId + "] but metadata does not");
            }
            if (entry.getValue().validate(projectMetadata) == false) {
                // should never happen - RoutingTable.validate will throw an exception rather than return false
                throw new IllegalStateException("Routing table for project [" + projectId + "] is not valid");
            }
        }
        return true;
    }

    public Iterable<IndexRoutingTable> indexRouting() {
        return Iterables.flatten(this);
    }

    public Optional<IndexRoutingTable> indexRouting(Metadata metadata, Index index) {
        return metadata.lookupProject(index).flatMap(pm -> indexRouting(pm.id(), index));
    }

    public Optional<IndexRoutingTable> indexRouting(ProjectId id, Index index) {
        return Optional.ofNullable(routingTable(id)).map(rt -> rt.index(index));
    }

    public boolean hasIndices() {
        return routingTables().values().stream().anyMatch(rt -> rt.indicesRouting().isEmpty() == false);
    }

    private static class GlobalRoutingTableDiff implements Diff<GlobalRoutingTable> {

        private static final KeySerializer<ProjectId> PROJECT_ID_KEY_SERIALIZER = DiffableUtils.getWriteableKeySerializer(ProjectId.READER);
        private static final DiffableUtils.DiffableValueReader<ProjectId, RoutingTable> DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(RoutingTable::readFrom, RoutingTable::readDiffFrom);

        private final Diff<ImmutableOpenMap<ProjectId, RoutingTable>> routingTable;
        private final Diff<RoutingTable> singleProjectForBwc;

        GlobalRoutingTableDiff(GlobalRoutingTable before, GlobalRoutingTable after) {
            this.routingTable = DiffableUtils.diff(before.routingTables, after.routingTables, PROJECT_ID_KEY_SERIALIZER);
            if (before.size() == 1 && after.size() == 1) {
                var afterTable = after.routingTables.values().iterator().next();
                var beforeTable = before.routingTables.values().iterator().next();
                this.singleProjectForBwc = afterTable.diff(beforeTable);
            } else {
                this.singleProjectForBwc = null;
            }
        }

        GlobalRoutingTableDiff(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                this.routingTable = DiffableUtils.readImmutableOpenMapDiff(in, PROJECT_ID_KEY_SERIALIZER, DIFF_VALUE_READER);
                this.singleProjectForBwc = null;
            } else {
                this.routingTable = null;
                this.singleProjectForBwc = RoutingTable.readDiffFrom(in);
            }
        }

        @Override
        public GlobalRoutingTable apply(GlobalRoutingTable part) {
            // BWC
            if (this.routingTable == null && singleProjectForBwc != null) {
                if (part.size() != 1) {
                    throw new IllegalStateException(
                        "Attempt to apply BWC (single project) diff to a table with [" + part.size() + "] projects"
                    );
                }
                final Map.Entry<ProjectId, RoutingTable> entry = part.routingTables.entrySet().iterator().next();
                final RoutingTable updatedTable = singleProjectForBwc.apply(entry.getValue());
                return new GlobalRoutingTable(ImmutableOpenMap.builder(Map.of(entry.getKey(), updatedTable)).build());
            } else {
                final ImmutableOpenMap<ProjectId, RoutingTable> updatedTable = routingTable.apply(part.routingTables);
                if (updatedTable == part.routingTables) {
                    return part;
                }

                return new GlobalRoutingTable(updatedTable);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                routingTable.writeTo(out);
            } else {
                if (singleProjectForBwc != null) {
                    singleProjectForBwc.writeTo(out);
                } else {
                    throw new IllegalStateException(
                        "Cannot write a multi-project diff to a stream with version [" + out.getTransportVersion() + "]"
                    );
                }
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(GlobalRoutingTable routingTable) {
        return new Builder(routingTable);
    }

    public static class Builder {
        private final ImmutableOpenMap.Builder<ProjectId, RoutingTable> projectRouting;

        public Builder(GlobalRoutingTable init) {
            this.projectRouting = ImmutableOpenMap.builder(init.routingTables);
        }

        public Builder() {
            this.projectRouting = ImmutableOpenMap.builder();
        }

        public Builder put(ProjectId id, RoutingTable routing) {
            this.projectRouting.put(id, routing);
            return this;
        }

        public Builder put(ProjectId id, RoutingTable.Builder routing) {
            return put(id, routing.build());
        }

        public Builder removeProject(ProjectId projectId) {
            this.projectRouting.remove(projectId);
            return this;
        }

        public Builder clear() {
            this.projectRouting.clear();
            return this;
        }

        public GlobalRoutingTable build() {
            return new GlobalRoutingTable(projectRouting.build());
        }
    }

    @Override
    public String toString() {
        return "global_routing_table{" + routingTables + "}";
    }

}
