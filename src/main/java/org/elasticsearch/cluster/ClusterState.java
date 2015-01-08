/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 *
 */
public class ClusterState extends CompositeClusterStatePart<ClusterState> implements ToXContent {

    public static enum ClusterStateStatus {
        UNKNOWN((byte) 0),
        RECEIVED((byte) 1),
        BEING_APPLIED((byte) 2),
        APPLIED((byte) 3);

        private final byte id;

        ClusterStateStatus(byte id) {
            this.id = id;
        }

        public byte id() {
            return this.id;
        }
    }

    public static final String TYPE = "cluster";

    public static final Factory FACTORY = new Factory();

    public static class Factory extends AbstractCompositeFactory<ClusterState> {

        @Override
        public ClusterState fromParts(long version, String uuid, ImmutableOpenMap.Builder<String, ClusterStatePart> parts) {
            return new ClusterState(version, uuid, parts.build());
        }

        @Override
        public String partType() {
            return TYPE;
        }
    }

    static {
        FACTORY.registerFactory(ClusterName.FACTORY);
        FACTORY.registerFactory(DiscoveryNodes.FACTORY);
        FACTORY.registerFactory(ClusterBlocks.FACTORY);
        FACTORY.registerFactory(RoutingTable.FACTORY);
        FACTORY.registerFactory(MetaData.FACTORY);
    }

    public static class ClusterStateDiff {
        private long version;
        private ClusterState.Diff<ClusterState> diff;

        public ClusterStateDiff(long version, ClusterState.Diff<ClusterState> diff) {
            this.version = version;
            this.diff = diff;
        }

        public ClusterState apply(ClusterState previous) throws IncompatibleClusterStateVersionException {
            return diff.apply(previous);
        }

        public long version() {
            return version;
        }
    }

    public static final long UNKNOWN_VERSION = -1;

    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final ClusterName clusterName;

    // built on demand
    private volatile RoutingNodes routingNodes;

    private volatile ClusterStateStatus status;

    public ClusterState(long version, String uuid, ImmutableOpenMap<String, ClusterStatePart> parts) {
        super(version, uuid, parts);
        this.clusterName = get(ClusterName.TYPE);
        this.routingTable = get(RoutingTable.TYPE);
        this.metaData = get(MetaData.TYPE);
        this.nodes = get(DiscoveryNodes.TYPE);
        this.blocks = get(ClusterBlocks.TYPE);
        this.status = ClusterStateStatus.UNKNOWN;
    }

    public ClusterStateStatus status() {
        return status;
    }

    public ClusterState status(ClusterStateStatus newStatus) {
        this.status = newStatus;
        return this;
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return version();
    }

    public DiscoveryNodes nodes() {
        return this.nodes;
    }

    public DiscoveryNodes getNodes() {
        return nodes();
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public RoutingNodes routingNodes() {
        return routingTable.routingNodes(this);
    }

    public RoutingNodes getRoutingNodes() {
        return readOnlyRoutingNodes();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return blocks();
    }

    public ClusterName getClusterName() {
        return clusterName();
    }

    public ClusterName clusterName() {
        return this.clusterName;
    }

    /**
     * Returns a built (on demand) routing nodes view of the routing table. <b>NOTE, the routing nodes
     * are mutable, use them just for read operations</b>
     */
    public RoutingNodes readOnlyRoutingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        routingNodes = routingTable.routingNodes(this);
        return routingNodes;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("version: ").append(version).append("\n");
        sb.append("meta data version: ").append(metaData.version()).append("\n");
        sb.append(nodes().prettyPrint());
        sb.append(routingTable().prettyPrint());
        sb.append(readOnlyRoutingNodes().prettyPrint());
        return sb.toString();
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public static class Metrics {

        public static final String VERSION = "version";
        public static final String MASTER_NODE = "master_node";
        public static final String NODES = DiscoveryNodes.TYPE;
        public static final String ROUTING_TABLE = RoutingTable.TYPE;
        public static final String METADATA = MetaData.TYPE;
        public static final String BLOCKS = ClusterBlocks.TYPE;


        private final ImmutableSet<String> includes;
        private final ImmutableSet<String> excludes;

        public Metrics(String params) {
            if (params.equals("_all")) {
                this.includes = ImmutableSet.of();
                this.excludes = ImmutableSet.of();
            } else {
                ImmutableSet.Builder<String> includes = ImmutableSet.builder();
                ImmutableSet.Builder<String> excludes = ImmutableSet.builder();
                String[] metrics = Strings.splitStringByCommaToArray(params);
                for (String metric : metrics) {
                    if (metric.startsWith("+")) {
                        includes.add(metric.substring(1));
                    } else if (metric.startsWith("-")) {
                        excludes.add(metric.substring(1));
                    } else {
                        includes.add(metric);
                    }
                }
                this.includes = includes.build();
                this.excludes = excludes.build();
            }
        }

        public boolean matches(String metric) {
            if (includes.isEmpty()) {
                return !excludes.contains(metric);
            }
            return includes.contains(metric) && !excludes.contains(metric);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Metrics metrics = new Metrics(params.param("metric", "_all"));

        if (metrics.matches(Metrics.VERSION)) {
            builder.field("version", version);
        }

        if (metrics.matches(Metrics.MASTER_NODE)) {
            builder.field("master_node", nodes().masterNodeId());
        }

        for(ObjectObjectCursor<String, ClusterStatePart> partIter : parts) {
            if (metrics.matches(partIter.key)) {
                builder.startObject(partIter.key);
                FACTORY.lookupFactorySafe(partIter.key).toXContent(partIter.value, builder, params);
                builder.endObject();
            }
        }

        // routing nodes
        if (metrics.matches(Metrics.ROUTING_TABLE)) {
            builder.startObject("routing_nodes");
            builder.startArray("unassigned");
            for (ShardRouting shardRouting : readOnlyRoutingNodes().unassigned()) {
                shardRouting.toXContent(builder, params);
            }
            builder.endArray();

            builder.startObject("nodes");
            for (RoutingNode routingNode : readOnlyRoutingNodes()) {
                builder.startArray(routingNode.nodeId(), XContentBuilder.FieldCaseConversion.NONE);
                for (ShardRouting shardRouting : routingNode) {
                    shardRouting.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }

        return builder;
    }

    public static Builder builder(ClusterName clusterName) {
        return new Builder(clusterName);
    }

    public static Builder builder(ClusterState state) {
        return new Builder(state);
    }

    public static class Builder {
        private long version = 0;
        private String uuid = null;
        private final ImmutableOpenMap.Builder<String, ClusterStatePart> parts;


        public Builder(ClusterState state) {
            this.version = state.version();
            this.uuid = state.uuid();
            this.parts = ImmutableOpenMap.builder(state.parts);
            putPart(ClusterName.TYPE, state.getClusterName());
            putPart(MetaData.TYPE, state.metaData());
            putPart(RoutingTable.TYPE, state.routingTable());
            putPart(DiscoveryNodes.TYPE, state.nodes());
            putPart(ClusterBlocks.TYPE, state.blocks());
        }

        public Builder(ClusterName clusterName) {
            parts = ImmutableOpenMap.builder();
            putPart(ClusterName.TYPE, clusterName);
            putPart(MetaData.TYPE, MetaData.EMPTY_META_DATA);
            putPart(RoutingTable.TYPE, RoutingTable.EMPTY_ROUTING_TABLE);
            putPart(DiscoveryNodes.TYPE, DiscoveryNodes.EMPTY_NODES);
            putPart(ClusterBlocks.TYPE, ClusterBlocks.EMPTY_CLUSTER_BLOCK);
        }


        public Builder nodes(DiscoveryNodes.Builder nodesBuilder) {
            return nodes(nodesBuilder.build());
        }

        public Builder nodes(DiscoveryNodes nodes) {
            putPart(DiscoveryNodes.TYPE, nodes);
            return this;
        }

        public Builder routingTable(RoutingTable.Builder routingTable) {
            return routingTable(routingTable.build());
        }

        public Builder routingResult(RoutingAllocation.Result routingResult) {
            putPart(RoutingTable.TYPE, routingResult.routingTable());
            return this;
        }

        public Builder routingTable(RoutingTable routingTable) {
            putPart(RoutingTable.TYPE, routingTable);
            return this;
        }

        public Builder metaData(MetaData.Builder metaDataBuilder) {
            return metaData(metaDataBuilder.build());
        }

        public Builder metaData(MetaData metaData) {
            putPart(MetaData.TYPE, metaData);
            return this;
        }

        public Builder blocks(ClusterBlocks.Builder blocksBuilder) {
            return blocks(blocksBuilder.build());
        }

        public Builder blocks(ClusterBlocks blocks) {
            putPart(ClusterBlocks.TYPE, blocks);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder incrementVersion() {
            version = version + 1;
            uuid = Strings.randomBase64UUID();
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public <T extends ClusterStatePart> T getPart(String type) {
            return (T)parts.get(type);
        }

        public Builder putPart(String type, ClusterStatePart custom) {
            parts.put(type, custom);
            return this;
        }

        public Builder removePart(String type) {
            parts.remove(type);
            return this;
        }

        public ClusterState build() {
            if (uuid == null) {
                uuid = Strings.randomBase64UUID();
            }
            return new ClusterState(version, uuid, parts.build());
        }

        public static byte[] toBytes(ClusterState state) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            writeTo(state, os);
            return os.bytes().toBytes();
        }

        /**
         * @param data               input bytes
         * @param localNode          used to set the local node in the cluster state.
         */
        public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode) throws IOException {
            return readFrom(new BytesStreamInput(data, false), localNode);
        }

        public static void writeTo(ClusterState state, StreamOutput out) throws IOException {
            FACTORY.writeTo(state, out);
        }

        /**
         * @param in                 input stream
         * @param localNode          used to set the local node in the cluster state. can be null.
         */
        public static ClusterState readFrom(StreamInput in, @Nullable DiscoveryNode localNode) throws IOException {
            return FACTORY.readFrom(in, new LocalContext(localNode));
        }

        public static ClusterStateDiff readDiffFrom(StreamInput in, @Nullable DiscoveryNode localNode) throws IOException {
            long version = in.readVLong();
            LocalContext localContext = new LocalContext(localNode);
            return new ClusterStateDiff(version, FACTORY.readDiffFrom(in, localContext));
        }

        public static void writeDiffTo(ClusterStateDiff diff, StreamOutput out) throws IOException {
            out.writeVLong(diff.version);
            FACTORY.writeDiffsTo(diff.diff, out);
        }

        public static ClusterStateDiff diff(ClusterState before, ClusterState after) {
            return new ClusterStateDiff(after.version(), ClusterState.FACTORY.diff(before, after) );
        }

        public static byte[] toDiffBytes(ClusterState before, ClusterState after) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            writeDiffTo(diff(before, after), os);
            return os.bytes().toBytes();
        }

    }

}
