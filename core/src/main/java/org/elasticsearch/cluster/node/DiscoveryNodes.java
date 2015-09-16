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

package org.elasticsearch.cluster.node;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.util.*;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 */
public class DiscoveryNodes extends AbstractDiffable<DiscoveryNodes> implements Iterable<DiscoveryNode> {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();
    public static final DiscoveryNodes PROTO = EMPTY_NODES;

    private final ImmutableOpenMap<String, DiscoveryNode> nodes;
    private final ImmutableOpenMap<String, DiscoveryNode> dataNodes;
    private final ImmutableOpenMap<String, DiscoveryNode> masterNodes;

    private final String masterNodeId;
    private final String localNodeId;
    private final Version minNodeVersion;
    private final Version minNonClientNodeVersion;

    private DiscoveryNodes(ImmutableOpenMap<String, DiscoveryNode> nodes, ImmutableOpenMap<String, DiscoveryNode> dataNodes, ImmutableOpenMap<String, DiscoveryNode> masterNodes, String masterNodeId, String localNodeId, Version minNodeVersion, Version minNonClientNodeVersion) {
        this.nodes = nodes;
        this.dataNodes = dataNodes;
        this.masterNodes = masterNodes;
        this.masterNodeId = masterNodeId;
        this.localNodeId = localNodeId;
        this.minNodeVersion = minNodeVersion;
        this.minNonClientNodeVersion = minNonClientNodeVersion;
    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.valuesIt();
    }

    /**
     * Is this a valid nodes that has the minimal information set. The minimal set is defined
     * by the localNodeId being set.
     */
    public boolean valid() {
        return localNodeId != null;
    }

    /**
     * Returns <tt>true</tt> if the local node is the master node.
     */
    public boolean localNodeMaster() {
        if (localNodeId == null) {
            // we don't know yet the local node id, return false
            return false;
        }
        return localNodeId.equals(masterNodeId);
    }

    /**
     * Get the number of known nodes
     *
     * @return number of nodes
     */
    public int size() {
        return nodes.size();
    }

    /**
     * Get the number of known nodes
     *
     * @return number of nodes
     */
    public int getSize() {
        return size();
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> nodes() {
        return this.nodes;
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getNodes() {
        return nodes();
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> dataNodes() {
        return this.dataNodes;
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getDataNodes() {
        return dataNodes();
    }

    /**
     * Get a {@link Map} of the discovered master nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> masterNodes() {
        return this.masterNodes;
    }

    /**
     * Get a {@link Map} of the discovered master nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getMasterNodes() {
        return masterNodes();
    }

    /**
     * Get a {@link Map} of the discovered master and data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master and data nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> masterAndDataNodes() {
        ImmutableOpenMap.Builder<String, DiscoveryNode> nodes = ImmutableOpenMap.builder(dataNodes);
        nodes.putAll(masterNodes);
        return nodes.build();
    }

    /**
     * Get a node by its id
     *
     * @param nodeId id of the wanted node
     * @return wanted node if it exists. Otherwise <code>null</code>
     */
    public DiscoveryNode get(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Determine if a given node exists
     *
     * @param nodeId id of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    /**
     * Get the id of the master node
     *
     * @return id of the master
     */
    public String masterNodeId() {
        return this.masterNodeId;
    }

    /**
     * Get the id of the master node
     *
     * @return id of the master
     */
    public String getMasterNodeId() {
        return masterNodeId();
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String localNodeId() {
        return this.localNodeId;
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String getLocalNodeId() {
        return localNodeId();
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode localNode() {
        return nodes.get(localNodeId);
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode getLocalNode() {
        return localNode();
    }

    /**
     * Get the master node
     *
     * @return master node
     */
    public DiscoveryNode masterNode() {
        return nodes.get(masterNodeId);
    }

    /**
     * Get the master node
     *
     * @return master node
     */
    public DiscoveryNode getMasterNode() {
        return masterNode();
    }

    /**
     * Get a node by its address
     *
     * @param address {@link TransportAddress} of the wanted node
     * @return node identified by the given address or <code>null</code> if no such node exists
     */
    public DiscoveryNode findByAddress(TransportAddress address) {
        for (ObjectCursor<DiscoveryNode> cursor : nodes.values()) {
            DiscoveryNode node = cursor.value;
            if (node.address().equals(address)) {
                return node;
            }
        }
        return null;
    }

    public boolean isAllNodes(String... nodesIds) {
        return nodesIds == null || nodesIds.length == 0 || (nodesIds.length == 1 && nodesIds[0].equals("_all"));
    }


    /**
     * Returns the version of the node with the oldest version in the cluster
     *
     * @return the oldest version in the cluster
     */
    public Version smallestVersion() {
       return minNodeVersion;
    }

    /**
     * Returns the version of the node with the oldest version in the cluster that is not a client node
     *
     * @return the oldest version in the cluster
     */
    public Version smallestNonClientNodeVersion() {
        return minNonClientNodeVersion;
    }

    /**
     * Resolve a node with a given id
     *
     * @param node id of the node to discover
     * @return discovered node matching the given id
     * @throws IllegalArgumentException if more than one node matches the request or no nodes have been resolved
     */
    public DiscoveryNode resolveNode(String node) {
        String[] resolvedNodeIds = resolveNodesIds(node);
        if (resolvedNodeIds.length > 1) {
            throw new IllegalArgumentException("resolved [" + node + "] into [" + resolvedNodeIds.length + "] nodes, where expected to be resolved to a single node");
        }
        if (resolvedNodeIds.length == 0) {
            throw new IllegalArgumentException("failed to resolve [" + node + " ], no matching nodes");
        }
        return nodes.get(resolvedNodeIds[0]);
    }

    public String[] resolveNodesIds(String... nodesIds) {
        if (isAllNodes(nodesIds)) {
            int index = 0;
            nodesIds = new String[nodes.size()];
            for (DiscoveryNode node : this) {
                nodesIds[index++] = node.id();
            }
            return nodesIds;
        } else {
            ObjectHashSet<String> resolvedNodesIds = new ObjectHashSet<>(nodesIds.length);
            for (String nodeId : nodesIds) {
                if (nodeId.equals("_local")) {
                    String localNodeId = localNodeId();
                    if (localNodeId != null) {
                        resolvedNodesIds.add(localNodeId);
                    }
                } else if (nodeId.equals("_master")) {
                    String masterNodeId = masterNodeId();
                    if (masterNodeId != null) {
                        resolvedNodesIds.add(masterNodeId);
                    }
                } else if (nodeExists(nodeId)) {
                    resolvedNodesIds.add(nodeId);
                } else {
                    // not a node id, try and search by name
                    for (DiscoveryNode node : this) {
                        if (Regex.simpleMatch(nodeId, node.name())) {
                            resolvedNodesIds.add(node.id());
                        }
                    }
                    for (DiscoveryNode node : this) {
                        if (Regex.simpleMatch(nodeId, node.getHostAddress())) {
                            resolvedNodesIds.add(node.id());
                        } else if (Regex.simpleMatch(nodeId, node.getHostName())) {
                            resolvedNodesIds.add(node.id());
                        }
                    }
                    int index = nodeId.indexOf(':');
                    if (index != -1) {
                        String matchAttrName = nodeId.substring(0, index);
                        String matchAttrValue = nodeId.substring(index + 1);
                        if ("data".equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(dataNodes.keys());
                            } else {
                                resolvedNodesIds.removeAll(dataNodes.keys());
                            }
                        } else if ("master".equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(masterNodes.keys());
                            } else {
                                resolvedNodesIds.removeAll(masterNodes.keys());
                            }
                        } else {
                            for (DiscoveryNode node : this) {
                                for (Map.Entry<String, String> entry : node.attributes().entrySet()) {
                                    String attrName = entry.getKey();
                                    String attrValue = entry.getValue();
                                    if (Regex.simpleMatch(matchAttrName, attrName) && Regex.simpleMatch(matchAttrValue, attrValue)) {
                                        resolvedNodesIds.add(node.id());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return resolvedNodesIds.toArray(String.class);
        }
    }

    public DiscoveryNodes removeDeadMembers(Set<String> newNodes, String masterNodeId) {
        Builder builder = new Builder().masterNodeId(masterNodeId).localNodeId(localNodeId);
        for (DiscoveryNode node : this) {
            if (newNodes.contains(node.id())) {
                builder.put(node);
            }
        }
        return builder.build();
    }

    public DiscoveryNodes newNode(DiscoveryNode node) {
        return new Builder(this).put(node).build();
    }

    /**
     * Returns the changes comparing this nodes to the provided nodes.
     */
    public Delta delta(DiscoveryNodes other) {
        List<DiscoveryNode> removed = new ArrayList<>();
        List<DiscoveryNode> added = new ArrayList<>();
        for (DiscoveryNode node : other) {
            if (!this.nodeExists(node.id())) {
                removed.add(node);
            }
        }
        for (DiscoveryNode node : this) {
            if (!other.nodeExists(node.id())) {
                added.add(node);
            }
        }
        DiscoveryNode previousMasterNode = null;
        DiscoveryNode newMasterNode = null;
        if (masterNodeId != null) {
            if (other.masterNodeId == null || !other.masterNodeId.equals(masterNodeId)) {
                previousMasterNode = other.masterNode();
                newMasterNode = masterNode();
            }
        }
        return new Delta(previousMasterNode, newMasterNode, localNodeId, Collections.unmodifiableList(removed), Collections.unmodifiableList(added));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (DiscoveryNode node : this) {
            sb.append(node).append(',');
        }
        sb.append("}");
        return sb.toString();
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes: \n");
        for (DiscoveryNode node : this) {
            sb.append("   ").append(node);
            if (node == localNode()) {
                sb.append(", local");
            }
            if (node == masterNode()) {
                sb.append(", master");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public Delta emptyDelta() {
        return new Delta(null, null, localNodeId, DiscoveryNode.EMPTY_LIST, DiscoveryNode.EMPTY_LIST);
    }

    public static class Delta {

        private final String localNodeId;
        private final DiscoveryNode previousMasterNode;
        private final DiscoveryNode newMasterNode;
        private final List<DiscoveryNode> removed;
        private final List<DiscoveryNode> added;

        public Delta(String localNodeId, List<DiscoveryNode> removed, List<DiscoveryNode> added) {
            this(null, null, localNodeId, removed, added);
        }

        public Delta(@Nullable DiscoveryNode previousMasterNode, @Nullable DiscoveryNode newMasterNode, String localNodeId, List<DiscoveryNode> removed, List<DiscoveryNode> added) {
            this.previousMasterNode = previousMasterNode;
            this.newMasterNode = newMasterNode;
            this.localNodeId = localNodeId;
            this.removed = removed;
            this.added = added;
        }

        public boolean hasChanges() {
            return masterNodeChanged() || !removed.isEmpty() || !added.isEmpty();
        }

        public boolean masterNodeChanged() {
            return newMasterNode != null;
        }

        public DiscoveryNode previousMasterNode() {
            return previousMasterNode;
        }

        public DiscoveryNode newMasterNode() {
            return newMasterNode;
        }

        public boolean removed() {
            return !removed.isEmpty();
        }

        public List<DiscoveryNode> removedNodes() {
            return removed;
        }

        public boolean added() {
            return !added.isEmpty();
        }

        public List<DiscoveryNode> addedNodes() {
            return added;
        }

        public String shortSummary() {
            StringBuilder sb = new StringBuilder();
            if (!removed() && masterNodeChanged()) {
                if (newMasterNode.id().equals(localNodeId)) {
                    // we are the master, no nodes we removed, we are actually the first master
                    sb.append("new_master ").append(newMasterNode());
                } else {
                    // we are not the master, so we just got this event. No nodes were removed, so its not a *new* master
                    sb.append("detected_master ").append(newMasterNode());
                }
            } else {
                if (masterNodeChanged()) {
                    sb.append("master {new ").append(newMasterNode());
                    if (previousMasterNode() != null) {
                        sb.append(", previous ").append(previousMasterNode());
                    }
                    sb.append("}");
                }
                if (removed()) {
                    if (masterNodeChanged()) {
                        sb.append(", ");
                    }
                    sb.append("removed {");
                    for (DiscoveryNode node : removedNodes()) {
                        sb.append(node).append(',');
                    }
                    sb.append("}");
                }
            }
            if (added()) {
                // don't print if there is one added, and it is us
                if (!(addedNodes().size() == 1 && addedNodes().get(0).id().equals(localNodeId))) {
                    if (removed() || masterNodeChanged()) {
                        sb.append(", ");
                    }
                    sb.append("added {");
                    for (DiscoveryNode node : addedNodes()) {
                        if (!node.id().equals(localNodeId)) {
                            // don't print ourself
                            sb.append(node).append(',');
                        }
                    }
                    sb.append("}");
                }
            }
            return sb.toString();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (masterNodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(masterNodeId);
        }
        out.writeVInt(nodes.size());
        for (DiscoveryNode node : this) {
            node.writeTo(out);
        }
    }

    public DiscoveryNodes readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        Builder builder = new Builder();
        if (in.readBoolean()) {
            builder.masterNodeId(in.readString());
        }
        if (localNode != null) {
            builder.localNodeId(localNode.id());
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DiscoveryNode node = DiscoveryNode.readNode(in);
            if (localNode != null && node.id().equals(localNode.id())) {
                // reuse the same instance of our address and local node id for faster equality
                node = localNode;
            }
            builder.put(node);
        }
        return builder.build();
    }

    @Override
    public DiscoveryNodes readFrom(StreamInput in) throws IOException {
        return readFrom(in, localNode());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(DiscoveryNodes nodes) {
        return new Builder(nodes);
    }

    public static class Builder {

        private final ImmutableOpenMap.Builder<String, DiscoveryNode> nodes;
        private String masterNodeId;
        private String localNodeId;

        public Builder() {
            nodes = ImmutableOpenMap.builder();
        }

        public Builder(DiscoveryNodes nodes) {
            this.masterNodeId = nodes.masterNodeId();
            this.localNodeId = nodes.localNodeId();
            this.nodes = ImmutableOpenMap.builder(nodes.nodes());
        }

        public Builder put(DiscoveryNode node) {
            nodes.put(node.id(), node);
            return this;
        }

        public Builder remove(String nodeId) {
            nodes.remove(nodeId);
            return this;
        }

        public Builder masterNodeId(String masterNodeId) {
            this.masterNodeId = masterNodeId;
            return this;
        }

        public Builder localNodeId(String localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        public DiscoveryNodes build() {
            ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodesBuilder = ImmutableOpenMap.builder();
            ImmutableOpenMap.Builder<String, DiscoveryNode> masterNodesBuilder = ImmutableOpenMap.builder();
            Version minNodeVersion = Version.CURRENT;
            Version minNonClientNodeVersion = Version.CURRENT;
            for (ObjectObjectCursor<String, DiscoveryNode> nodeEntry : nodes) {
                if (nodeEntry.value.dataNode()) {
                    dataNodesBuilder.put(nodeEntry.key, nodeEntry.value);
                    minNonClientNodeVersion = Version.smallest(minNonClientNodeVersion, nodeEntry.value.version());
                }
                if (nodeEntry.value.masterNode()) {
                    masterNodesBuilder.put(nodeEntry.key, nodeEntry.value);
                    minNonClientNodeVersion = Version.smallest(minNonClientNodeVersion, nodeEntry.value.version());
                }
                minNodeVersion = Version.smallest(minNodeVersion, nodeEntry.value.version());
            }

            return new DiscoveryNodes(nodes.build(), dataNodesBuilder.build(), masterNodesBuilder.build(), masterNodeId, localNodeId, minNodeVersion, minNonClientNodeVersion);
        }

        public static DiscoveryNodes readFrom(StreamInput in, @Nullable DiscoveryNode localNode) throws IOException {
            return PROTO.readFrom(in, localNode);
        }
    }
}
