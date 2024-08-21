/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.DiffableUtils.KeySerializer;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A routing table for the whole cluster (potentially containing multiple projects)
 * The global routing table holds a separate {@link RoutingTable} for each project that exists in the cluster
 */
public class GlobalRoutingTable implements Iterable<RoutingTable>, Diffable<GlobalRoutingTable> {

    public static final GlobalRoutingTable EMPTY_ROUTING_TABLE = new GlobalRoutingTable(0, ImmutableOpenMap.of());
    private final long version;

    private final ImmutableOpenMap<ProjectId, RoutingTable> routingTables;

    public GlobalRoutingTable(long version, ImmutableOpenMap<ProjectId, RoutingTable> routingTables) {
        this.version = version;
        this.routingTables = routingTables;
    }

    public GlobalRoutingTable withIncrementedVersion() {
        return new GlobalRoutingTable(version + 1, routingTables);
    }

    /**
     * Returns the version of the {@link RoutingTable}.
     *
     * @return version of the {@link RoutingTable}
     */
    public long version() {
        return this.version;
    }

    /**
     * TODO: Remove this method, replace with routingTable(ProjectId)
     * @return
     * <ul>
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
            default -> throw new IllegalStateException("There are multiple project routing tables [" + routingTables.keySet() + "]");
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
        final long version = in.readLong();
        final var table = in.readImmutableOpenMap(ProjectId::new, RoutingTable::readFrom);
        return new GlobalRoutingTable(version, table);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
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
     *   then a new routing table that is a clone of this routing table with the specified project removed
     *   Otherwise returns this routing table unchanged.
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
        return new GlobalRoutingTable(version + 1, builder.build());
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
            return new GlobalRoutingTable(this.version + 1, ImmutableOpenMap.builder(newTable).build());
        }
    }

    private static class GlobalRoutingTableDiff implements Diff<GlobalRoutingTable> {

        private static final KeySerializer<ProjectId> PROJECT_ID_KEY_SERIALIZER = DiffableUtils.getWriteableKeySerializer(ProjectId.READER);
        private static final DiffableUtils.DiffableValueReader<ProjectId, RoutingTable> DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(RoutingTable::readFrom, RoutingTable::readDiffFrom);

        private final long version;

        private final Diff<ImmutableOpenMap<ProjectId, RoutingTable>> routingTable;
        private final Diff<RoutingTable> singleProjectForBwc;

        GlobalRoutingTableDiff(GlobalRoutingTable before, GlobalRoutingTable after) {
            this.version = after.version;
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
                this.version = in.readLong();
                this.routingTable = DiffableUtils.readImmutableOpenMapDiff(in, PROJECT_ID_KEY_SERIALIZER, DIFF_VALUE_READER);
                this.singleProjectForBwc = null;
            } else {
                this.version = -1;
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
                return new GlobalRoutingTable(
                    updatedTable.version(),
                    ImmutableOpenMap.builder(Map.of(entry.getKey(), updatedTable)).build()
                );
            } else {
                final ImmutableOpenMap<ProjectId, RoutingTable> updatedTable = routingTable.apply(part.routingTables);
                if (part.version == version && updatedTable == part.routingTables) {
                    return part;
                }

                return new GlobalRoutingTable(version, updatedTable);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                out.writeLong(version);
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
        private long version;
        private final ImmutableOpenMap.Builder<ProjectId, RoutingTable> projectRouting;

        public Builder(GlobalRoutingTable init) {
            this.version = init.version;
            this.projectRouting = ImmutableOpenMap.builder(init.routingTables);
        }

        public Builder() {
            this.version = 0L;
            this.projectRouting = ImmutableOpenMap.builder();
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder incrementVersion() {
            return version(version + 1);
        }

        public Builder put(ProjectId id, RoutingTable routing) {
            this.projectRouting.put(id, routing);
            return this;
        }

        public Builder put(ProjectId id, RoutingTable.Builder routing) {
            return put(id, routing.build());
        }

        public Builder clear() {
            this.projectRouting.clear();
            return this;
        }

        public GlobalRoutingTable build() {
            return new GlobalRoutingTable(version, projectRouting.build());
        }
    }

    @Override
    public String toString() {
        return "global_routing_table{v" + version + "," + routingTables + "}";
    }
}
