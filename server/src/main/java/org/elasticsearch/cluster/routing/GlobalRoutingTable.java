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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

public class GlobalRoutingTable implements Iterable<RoutingTable>, Diffable<GlobalRoutingTable> {

    public static final GlobalRoutingTable EMPTY_ROUTING_TABLE = new GlobalRoutingTable(0, RoutingTable.EMPTY_ROUTING_TABLE);
    private final long version;

    private final RoutingTable routingTable;

    public GlobalRoutingTable(long version, RoutingTable routingTable) {
        this.version = version;
        this.routingTable = routingTable;
    }

    public GlobalRoutingTable withIncrementedVersion() {
        return new GlobalRoutingTable(version + 1, routingTable);
    }

    /**
     * Returns the version of the {@link RoutingTable}.
     *
     * @return version of the {@link RoutingTable}
     */
    public long version() {
        return this.version;
    }

    /*
     * TODO: Remove this method, replace with routingTable(ProjectId)
     */
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    @Override
    public Iterator<RoutingTable> iterator() {
        return Iterators.single(routingTable);
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
        final RoutingTable table = RoutingTable.readFrom(in);
        return new GlobalRoutingTable(version, table);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        out.writeWriteable(routingTable);
    }

    public boolean hasSameIndexRouting(GlobalRoutingTable other) {
        return this.getRoutingTable().indicesRouting() == other.getRoutingTable().indicesRouting();
    }

    private static class GlobalRoutingTableDiff implements Diff<GlobalRoutingTable> {

        private final long version;

        private final Diff<RoutingTable> routingTable;

        GlobalRoutingTableDiff(GlobalRoutingTable before, GlobalRoutingTable after) {
            this.version = after.version;
            this.routingTable = after.routingTable.diff(before.routingTable);
        }

        GlobalRoutingTableDiff(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                this.version = in.readLong();
            } else {
                this.version = -1;
            }
            this.routingTable = RoutingTable.readDiffFrom(in);
        }

        @Override
        public GlobalRoutingTable apply(GlobalRoutingTable part) {
            final RoutingTable updatedTable = routingTable.apply(part.routingTable);
            if (part.version == version && updatedTable == part.routingTable) {
                return part;
            }
            return new GlobalRoutingTable(version == -1 ? updatedTable.version() : version, updatedTable);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                out.writeLong(version);
            }
            routingTable.writeTo(out);
        }
    }

    @Override
    public String toString() {
        return "global_routing_table{v" + version + "," + routingTable + "}";
    }
}
