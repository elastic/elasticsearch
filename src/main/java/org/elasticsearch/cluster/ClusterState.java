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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
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
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;

/**
 *
 */
public class ClusterState implements ToXContent {

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

    public static final long UNKNOWN_VERSION = -1;

    private final long version;

    private final String uuid;

    private final ClusterName clusterName;
    
    private final ClusterStateParts parts;

    // built on demand
    private volatile RoutingNodes routingNodes;

    private volatile ClusterStateStatus status;

    public ClusterState(ClusterName clusterName, long version, String uuid, ClusterStateParts parts) {
        this.version = version;
        this.clusterName = clusterName;
        this.parts = parts;
        this.status = ClusterStateStatus.UNKNOWN;
        this.uuid = uuid;
    }

    public ClusterState(long version, String uuid, ClusterState state) {
        this(state.clusterName, version, uuid, state.parts);
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
        return (DiscoveryNodes)parts.get(DiscoveryNodes.TYPE);
    }

    public DiscoveryNodes getNodes() {
        return nodes();
    }

    public MetaData metaData() {
        return (MetaData)parts.get(MetaData.TYPE);
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public RoutingTable routingTable() {
        return (RoutingTable)parts.get(RoutingTable.TYPE);
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public RoutingNodes routingNodes() {
        return routingTable().routingNodes(this);
    }

    public RoutingNodes getRoutingNodes() {
        return readOnlyRoutingNodes();
    }

    public ClusterBlocks blocks() {
        return (ClusterBlocks)parts.get(ClusterBlocks.TYPE);
    }

    public ClusterBlocks getBlocks() {
        return blocks();
    }

    public ClusterName getClusterName() {
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
        routingNodes = routingTable().routingNodes(this);
        return routingNodes;
    }

    public ClusterState settingsFilter(SettingsFilter settingsFilter) {
        metaData().settingsFilter(settingsFilter);
        return this;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("version: ").append(version).append("\n");
        sb.append("meta data version: ").append(metaData().version()).append("\n");
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

    public enum Metric {
        VERSION("version"),
        MASTER_NODE("master_node"),
        BLOCKS("blocks"),
        NODES("nodes"),
        METADATA("metadata"),
        ROUTING_TABLE("routing_table"),
        CUSTOMS("customs");

        private static Map<String, Metric> valueToEnum;

        static {
            valueToEnum = new HashMap<>();
            for (Metric metric : Metric.values()) {
                valueToEnum.put(metric.value, metric);
            }
        }

        private final String value;

        private Metric(String value) {
            this.value = value;
        }

        public static EnumSet<Metric> parseString(String param, boolean ignoreUnknown) {
            String[] metrics = Strings.splitStringByCommaToArray(param);
            EnumSet<Metric> result = EnumSet.noneOf(Metric.class);
            for (String metric : metrics) {
                if ("_all".equals(metric)) {
                    result = EnumSet.allOf(Metric.class);
                    break;
                }
                Metric m = valueToEnum.get(metric);
                if (m == null) {
                    if (!ignoreUnknown) {
                        throw new ElasticsearchIllegalArgumentException("Unknown metric [" + metric + "]");
                    }
                } else {
                    result.add(m);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return value;
        }
    }



    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        EnumSet<Metric> metrics = Metric.parseString(params.param("metric", "_all"), true);
        Set<String> metricStrings = newHashSet();
        for (Metric metric : metrics) {
            metricStrings.add(metric.value);
        }

        if (metrics.contains(Metric.VERSION)) {
            builder.field("version", version);
        }

        if (metrics.contains(Metric.MASTER_NODE)) {
            builder.field("master_node", nodes().masterNodeId());
        }

        for(ObjectObjectCursor<String, ClusterStatePart> partIter : parts.parts()) {
            if (metricStrings.contains(partIter.key)) {
                builder.startObject(partIter.key);
                partIter.value.toXContent(builder, params);
                builder.endObject();
            }
        }

        // routing nodes
        if (metrics.contains(Metric.ROUTING_TABLE)) {
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

        private final ClusterName clusterName;
        private long version = 0;
        private String uuid = null;
        private final ImmutableOpenMap.Builder<String, ClusterStatePart> parts;


        public Builder(ClusterState state) {
            this.clusterName = state.clusterName;
            this.version = state.version();
            this.parts = ImmutableOpenMap.builder(state.parts.parts);
            putPart(MetaData.TYPE, state.metaData());
            putPart(RoutingTable.TYPE, state.routingTable());
            putPart(DiscoveryNodes.TYPE, state.nodes());
            putPart(ClusterBlocks.TYPE, state.blocks());
        }

        public Builder(ClusterName clusterName) {
            this.clusterName = clusterName;
            parts = ImmutableOpenMap.builder();
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
            return new ClusterState(clusterName, version, uuid, new ClusterStateParts(parts.build()));
        }

        public static byte[] toBytes(ClusterState state) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            writeTo(state, os);
            return os.bytes().toBytes();
        }

        /**
         * @param data               input bytes
         * @param localNode          used to set the local node in the cluster state.
         * @param defaultClusterName this cluster name will be used of if the deserialized cluster state does not have a name set
         *                           (which is only introduced in version 1.1.1)
         */
        public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode, ClusterName defaultClusterName) throws IOException {
            return readFrom(new BytesStreamInput(data, false), localNode, defaultClusterName);
        }

        public static void writeTo(ClusterState state, StreamOutput out) throws IOException {
            out.writeBoolean(state.clusterName != null);
            if (state.clusterName != null) {
                state.clusterName.writeTo(out);
            }
            out.writeLong(state.version);
            out.writeString(state.uuid);
            state.parts.writeTo(out);
        }

        /**
         * @param in                 input stream
         * @param localNode          used to set the local node in the cluster state. can be null.
         * @param defaultClusterName this cluster name will be used of receiving a cluster state from a node on version older than 1.1.1
         *                           or if the sending node did not set a cluster name
         */
        public static ClusterState readFrom(StreamInput in, @Nullable DiscoveryNode localNode, @Nullable ClusterName defaultClusterName) throws IOException {
            ClusterName clusterName = defaultClusterName;
            if (in.readBoolean()) {
                clusterName = ClusterName.readClusterName(in);
            }
            Builder builder = new Builder(clusterName);
            builder.version = in.readLong();
            builder.uuid = in.readString();
            builder.parts.putAll(ClusterStateParts.FACTORY.readFrom(in).parts());
            //TODO: Hack!!!! Need to find a better way to handle localNode
            DiscoveryNodes discoveryNodes = builder.getPart(DiscoveryNodes.TYPE);
            if (localNode != null) {
                builder.nodes(DiscoveryNodes.builder(discoveryNodes).localNodeId(localNode.id()));
            }
            return builder.build();
        }

        public static ClusterStateDiff readDiffFrom(StreamInput in, @Nullable DiscoveryNode localNode, @Nullable ClusterName defaultClusterName) throws IOException {
            ClusterName clusterName = defaultClusterName;
            if (in.readBoolean()) {
                clusterName = ClusterName.readClusterName(in);
            }
            long version = in.readLong();
            String previousUuid = in.readString();
            String newUuid = in.readString();
            ClusterStatePart.Diff<ClusterStateParts> diff = ClusterStateParts.FACTORY.readDiffFrom(in);
            return new ClusterStateDiff(clusterName, version, previousUuid, newUuid, localNode, diff);

        }

        public static ClusterStateDiff diff(ClusterState before, ClusterState after) {
            return new ClusterStateDiff(after.clusterName, after.version, before.uuid, after.uuid, after.nodes().localNode(), ClusterStateParts.FACTORY.diff(before.parts, after.parts) );
        }

        public static byte[] toDiffBytes(ClusterState before, ClusterState after) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            diff(before, after).writeTo(os);
            return os.bytes().toBytes();
        }

        public static ClusterState fromDiffBytes(ClusterState before, byte[] data, DiscoveryNode localNode, ClusterName defaultClusterName) throws IOException {
            ClusterStateDiff diff = readDiffFrom(new BytesStreamInput(data, false), localNode, defaultClusterName);
            return diff.apply(before);
        }



    }

    public static class ClusterStateDiff {
        private ClusterName clusterName;
        private long version;
        private String previousUuid;
        private String newUuid;
        private DiscoveryNode localNode;
        private ClusterStatePart.Diff<ClusterStateParts> diff;

        public ClusterStateDiff(ClusterName clusterName, long version, String previousUuid, String newUuid, @Nullable DiscoveryNode localNode, ClusterStatePart.Diff<ClusterStateParts> diff) {
            this.clusterName = clusterName;
            this.version = version;
            this.previousUuid = previousUuid;
            this.newUuid = newUuid;
            this.localNode = localNode;
            this.diff = diff;
        }

        public ClusterState apply(ClusterState previous) throws IncompatibleClusterStateVersionException {
            if (!previousUuid.equals(previous.uuid)) {
                throw new IncompatibleClusterStateVersionException("Expected version " + (previous.version + 1) +"/" + previous.uuid + " got version " + version + "/" + previousUuid);
            }
            Builder builder = new Builder(clusterName);
            builder.version = version;
            builder.uuid = newUuid;
            builder.parts.putAll(diff.apply(previous.parts).parts);
            //TODO: Is there a better way to handle it?
            DiscoveryNodes discoveryNodes = builder.getPart(DiscoveryNodes.TYPE);
            if (localNode != null) {
                builder.nodes(DiscoveryNodes.builder(discoveryNodes).localNodeId(localNode.id()));
            }
            return  builder.build();
        }

        public void writeTo(StreamOutput out) throws IOException {
            if (clusterName != null) {
                out.writeBoolean(true);
                clusterName.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            out.writeLong(version);
            out.writeString(previousUuid);
            out.writeString(newUuid);
            diff.writeTo(out);

        }

        public long version() {
            return version;
        }
    }

}
