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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 */
public class DiscoveryNodes extends AbstractDiffable<DiscoveryNodes> implements Iterable<DiscoveryNode> {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();

    private final ImmutableOpenMap<String, DiscoveryNode> nodes;
    private final ImmutableOpenMap<String, DiscoveryNode> dataNodes;
    private final ImmutableOpenMap<String, DiscoveryNode> masterNodes;
    private final ImmutableOpenMap<String, DiscoveryNode> ingestNodes;

    private final String masterNodeId;
    private final String localNodeId;
    private final Version minNonClientNodeVersion;
    private final Version maxNonClientNodeVersion;
    private final Version maxNodeVersion;
    private final Version minNodeVersion;

    private DiscoveryNodes(ImmutableOpenMap<String, DiscoveryNode> nodes, ImmutableOpenMap<String, DiscoveryNode> dataNodes,
                           ImmutableOpenMap<String, DiscoveryNode> masterNodes, ImmutableOpenMap<String, DiscoveryNode> ingestNodes,
                           String masterNodeId, String localNodeId, Version minNonClientNodeVersion, Version maxNonClientNodeVersion,
                           Version maxNodeVersion, Version minNodeVersion) {
        this.nodes = nodes;
        this.dataNodes = dataNodes;
        this.masterNodes = masterNodes;
        this.ingestNodes = ingestNodes;
        this.masterNodeId = masterNodeId;
        this.localNodeId = localNodeId;
        this.minNonClientNodeVersion = minNonClientNodeVersion;
        this.maxNonClientNodeVersion = maxNonClientNodeVersion;
        this.minNodeVersion = minNodeVersion;
        this.maxNodeVersion = maxNodeVersion;
    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.valuesIt();
    }

    /**
     * Returns {@code true} if the local node is the elected master node.
     */
    public boolean isLocalNodeElectedMaster() {
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
    public int getSize() {
        return nodes.size();
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getNodes() {
        return this.nodes;
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getDataNodes() {
        return this.dataNodes;
    }

    /**
     * Get a {@link Map} of the discovered master nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getMasterNodes() {
        return this.masterNodes;
    }

    /**
     * @return All the ingest nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getIngestNodes() {
        return ingestNodes;
    }

    /**
     * Get a {@link Map} of the discovered master and data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master and data nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getMasterAndDataNodes() {
        ImmutableOpenMap.Builder<String, DiscoveryNode> nodes = ImmutableOpenMap.builder(dataNodes);
        nodes.putAll(masterNodes);
        return nodes.build();
    }

    /**
     * Get a {@link Map} of the coordinating only nodes (nodes which are neither master, nor data, nor ingest nodes) arranged by their ids
     *
     * @return {@link Map} of the coordinating only nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getCoordinatingOnlyNodes() {
        ImmutableOpenMap.Builder<String, DiscoveryNode> nodes = ImmutableOpenMap.builder(this.nodes);
        nodes.removeAll(masterNodes.keys());
        nodes.removeAll(dataNodes.keys());
        nodes.removeAll(ingestNodes.keys());
        return nodes.build();
    }

    /**
     * Returns a stream of all nodes, with master nodes at the front
     */
    public Stream<DiscoveryNode> mastersFirstStream() {
        return Stream.concat(StreamSupport.stream(masterNodes.spliterator(), false).map(cur -> cur.value),
            StreamSupport.stream(this.spliterator(), false).filter(n -> n.isMasterNode() == false));
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
     * Determine if a given node id exists
     *
     * @param nodeId id of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    /**
     * Determine if a given node exists
     *
     * @param node of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(DiscoveryNode node) {
        DiscoveryNode existing = nodes.get(node.getId());
        return existing != null && existing.equals(node);
    }

    /**
     * Get the id of the master node
     *
     * @return id of the master
     */
    public String getMasterNodeId() {
        return this.masterNodeId;
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String getLocalNodeId() {
        return this.localNodeId;
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode getLocalNode() {
        return nodes.get(localNodeId);
    }

    /**
     * Returns the master node, or {@code null} if there is no master node
     */
    @Nullable
    public DiscoveryNode getMasterNode() {
        if (masterNodeId != null) {
            return nodes.get(masterNodeId);
        }
        return null;
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
            if (node.getAddress().equals(address)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the version of the node with the oldest version in the cluster that is not a client node
     *
     * If there are no non-client nodes, Version.CURRENT will be returned.
     *
     * @return the oldest version in the cluster
     */
    public Version getSmallestNonClientNodeVersion() {
        return minNonClientNodeVersion;
    }

    /**
     * Returns the version of the node with the youngest version in the cluster that is not a client node.
     *
     * If there are no non-client nodes, Version.CURRENT will be returned.
     *
     * @return the youngest version in the cluster
     */
    public Version getLargestNonClientNodeVersion() {
        return maxNonClientNodeVersion;
    }

    /**
     * Returns the version of the node with the oldest version in the cluster.
     *
     * @return the oldest version in the cluster
     */
    public Version getMinNodeVersion() {
        return minNodeVersion;
    }

    /**
     * Returns the version of the node with the youngest version in the cluster
     *
     * @return the youngest version in the cluster
     */
    public Version getMaxNodeVersion() {
        return maxNodeVersion;
    }

    /**
     * Resolve a node with a given id
     *
     * @param node id of the node to discover
     * @return discovered node matching the given id
     * @throws IllegalArgumentException if more than one node matches the request or no nodes have been resolved
     */
    public DiscoveryNode resolveNode(String node) {
        String[] resolvedNodeIds = resolveNodes(node);
        if (resolvedNodeIds.length > 1) {
            throw new IllegalArgumentException("resolved [" + node + "] into [" + resolvedNodeIds.length
                + "] nodes, where expected to be resolved to a single node");
        }
        if (resolvedNodeIds.length == 0) {
            throw new IllegalArgumentException("failed to resolve [" + node + "], no matching nodes");
        }
        return nodes.get(resolvedNodeIds[0]);
    }

    /**
     * resolves a set of node "descriptions" to concrete and existing node ids. "descriptions" can be (resolved in this order):
     * - "_local" or "_master" for the relevant nodes
     * - a node id
     * - a wild card pattern that will be matched against node names
     * - a "attr:value" pattern, where attr can be a node role (master, data, ingest etc.) in which case the value can be true or false,
     *   or a generic node attribute name in which case value will be treated as a wildcard and matched against the node attribute values.
     */
    public String[] resolveNodes(String... nodes) {
        if (nodes == null || nodes.length == 0) {
            return StreamSupport.stream(this.spliterator(), false).map(DiscoveryNode::getId).toArray(String[]::new);
        } else {
            ObjectHashSet<String> resolvedNodesIds = new ObjectHashSet<>(nodes.length);
            for (String nodeId : nodes) {
                if (nodeId.equals("_local")) {
                    String localNodeId = getLocalNodeId();
                    if (localNodeId != null) {
                        resolvedNodesIds.add(localNodeId);
                    }
                } else if (nodeId.equals("_master")) {
                    String masterNodeId = getMasterNodeId();
                    if (masterNodeId != null) {
                        resolvedNodesIds.add(masterNodeId);
                    }
                } else if (nodeExists(nodeId)) {
                    resolvedNodesIds.add(nodeId);
                } else {
                    for (DiscoveryNode node : this) {
                        if ("_all".equals(nodeId)
                                || Regex.simpleMatch(nodeId, node.getName())
                                || Regex.simpleMatch(nodeId, node.getHostAddress())
                                || Regex.simpleMatch(nodeId, node.getHostName())) {
                            resolvedNodesIds.add(node.getId());
                        }
                    }
                    int index = nodeId.indexOf(':');
                    if (index != -1) {
                        String matchAttrName = nodeId.substring(0, index);
                        String matchAttrValue = nodeId.substring(index + 1);
                        if (DiscoveryNodeRole.DATA_ROLE.roleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(dataNodes.keys());
                            } else {
                                resolvedNodesIds.removeAll(dataNodes.keys());
                            }
                        } else if (DiscoveryNodeRole.MASTER_ROLE.roleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(masterNodes.keys());
                            } else {
                                resolvedNodesIds.removeAll(masterNodes.keys());
                            }
                        } else if (DiscoveryNodeRole.INGEST_ROLE.roleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(ingestNodes.keys());
                            } else {
                                resolvedNodesIds.removeAll(ingestNodes.keys());
                            }
                        } else if (DiscoveryNode.COORDINATING_ONLY.equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(getCoordinatingOnlyNodes().keys());
                            } else {
                                resolvedNodesIds.removeAll(getCoordinatingOnlyNodes().keys());
                            }
                        } else {
                            for (DiscoveryNode node : this) {
                                for (DiscoveryNodeRole role : Sets.difference(node.getRoles(), DiscoveryNodeRole.BUILT_IN_ROLES)) {
                                    if (role.roleName().equals(matchAttrName)) {
                                        if (Booleans.parseBoolean(matchAttrValue, true)) {
                                            resolvedNodesIds.add(node.getId());
                                        } else {
                                            resolvedNodesIds.remove(node.getId());
                                        }
                                    }
                                }
                            }
                            for (DiscoveryNode node : this) {
                                for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                                    String attrName = entry.getKey();
                                    String attrValue = entry.getValue();
                                    if (Regex.simpleMatch(matchAttrName, attrName) && Regex.simpleMatch(matchAttrValue, attrValue)) {
                                        resolvedNodesIds.add(node.getId());
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

    public DiscoveryNodes newNode(DiscoveryNode node) {
        return new Builder(this).add(node).build();
    }

    /**
     * Returns the changes comparing this nodes to the provided nodes.
     */
    public Delta delta(DiscoveryNodes other) {
        final List<DiscoveryNode> removed = new ArrayList<>();
        final List<DiscoveryNode> added = new ArrayList<>();
        for (DiscoveryNode node : other) {
            if (this.nodeExists(node) == false) {
                removed.add(node);
            }
        }
        for (DiscoveryNode node : this) {
            if (other.nodeExists(node) == false) {
                added.add(node);
            }
        }

        return new Delta(other.getMasterNode(), getMasterNode(), localNodeId, Collections.unmodifiableList(removed),
            Collections.unmodifiableList(added));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes: \n");
        for (DiscoveryNode node : this) {
            sb.append("   ").append(node);
            if (node == getLocalNode()) {
                sb.append(", local");
            }
            if (node == getMasterNode()) {
                sb.append(", master");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static class Delta {

        private final String localNodeId;
        @Nullable private final DiscoveryNode previousMasterNode;
        @Nullable private final DiscoveryNode newMasterNode;
        private final List<DiscoveryNode> removed;
        private final List<DiscoveryNode> added;

        private Delta(@Nullable DiscoveryNode previousMasterNode, @Nullable DiscoveryNode newMasterNode, String localNodeId,
                     List<DiscoveryNode> removed, List<DiscoveryNode> added) {
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
            return Objects.equals(newMasterNode, previousMasterNode) == false;
        }

        @Nullable
        public DiscoveryNode previousMasterNode() {
            return previousMasterNode;
        }

        @Nullable
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
            final StringBuilder summary = new StringBuilder();
            if (masterNodeChanged()) {
                summary.append("master node changed {previous [");
                if (previousMasterNode() != null) {
                    summary.append(previousMasterNode());
                }
                summary.append("], current [");
                if (newMasterNode() != null) {
                    summary.append(newMasterNode());
                }
                summary.append("]}");
            }
            if (removed()) {
                if (summary.length() > 0) {
                    summary.append(", ");
                }
                summary.append("removed {");
                for (DiscoveryNode node : removedNodes()) {
                    summary.append(node).append(',');
                }
                summary.append("}");
            }
            if (added()) {
                // don't print if there is one added, and it is us
                if (!(addedNodes().size() == 1 && addedNodes().get(0).getId().equals(localNodeId))) {
                    if (summary.length() > 0) {
                        summary.append(", ");
                    }
                    summary.append("added {");
                    for (DiscoveryNode node : addedNodes()) {
                        if (!node.getId().equals(localNodeId)) {
                            // don't print ourself
                            summary.append(node).append(',');
                        }
                    }
                    summary.append("}");
                }
            }
            return summary.toString();
        }
    }

    @Override
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

    public static DiscoveryNodes readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        Builder builder = new Builder();
        if (in.readBoolean()) {
            builder.masterNodeId(in.readString());
        }
        if (localNode != null) {
            builder.localNodeId(localNode.getId());
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            DiscoveryNode node = new DiscoveryNode(in);
            if (localNode != null && node.getId().equals(localNode.getId())) {
                // reuse the same instance of our address and local node id for faster equality
                node = localNode;
            }
            // some one already built this and validated it's OK, skip the n2 scans
            assert builder.validateAdd(node) == null : "building disco nodes from network doesn't pass preflight: "
                + builder.validateAdd(node);
            builder.putUnsafe(node);
        }
        return builder.build();
    }

    public static Diff<DiscoveryNodes> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return AbstractDiffable.readDiffFrom(in1 -> readFrom(in1, localNode), in);
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
            this.masterNodeId = nodes.getMasterNodeId();
            this.localNodeId = nodes.getLocalNodeId();
            this.nodes = ImmutableOpenMap.builder(nodes.getNodes());
        }

        /**
         * adds a disco node to the builder. Will throw an {@link IllegalArgumentException} if
         * the supplied node doesn't pass the pre-flight checks performed by {@link #validateAdd(DiscoveryNode)}
         */
        public Builder add(DiscoveryNode node) {
            final String preflight = validateAdd(node);
            if (preflight != null) {
                throw new IllegalArgumentException(preflight);
            }
            putUnsafe(node);
            return this;
        }

        /**
         * Get a node by its id
         *
         * @param nodeId id of the wanted node
         * @return wanted node if it exists. Otherwise <code>null</code>
         */
        @Nullable public DiscoveryNode get(String nodeId) {
            return nodes.get(nodeId);
        }

        private void putUnsafe(DiscoveryNode node) {
            nodes.put(node.getId(), node);
        }

        public Builder remove(String nodeId) {
            nodes.remove(nodeId);
            return this;
        }

        public Builder remove(DiscoveryNode node) {
            if (node.equals(nodes.get(node.getId()))) {
                nodes.remove(node.getId());
            }
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

        /**
         * Checks that a node can be safely added to this node collection.
         *
         * @return null if all is OK or an error message explaining why a node can not be added.
         *
         * Note: if this method returns a non-null value, calling {@link #add(DiscoveryNode)} will fail with an
         * exception
         */
        private String validateAdd(DiscoveryNode node) {
            for (ObjectCursor<DiscoveryNode> cursor : nodes.values()) {
                final DiscoveryNode existingNode = cursor.value;
                if (node.getAddress().equals(existingNode.getAddress()) &&
                    node.getId().equals(existingNode.getId()) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode + " with same address";
                }
                if (node.getId().equals(existingNode.getId()) &&
                    node.equals(existingNode) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode
                        + " with the same id but is a different node instance";
                }
            }
            return null;
        }

        public DiscoveryNodes build() {
            ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodesBuilder = ImmutableOpenMap.builder();
            ImmutableOpenMap.Builder<String, DiscoveryNode> masterNodesBuilder = ImmutableOpenMap.builder();
            ImmutableOpenMap.Builder<String, DiscoveryNode> ingestNodesBuilder = ImmutableOpenMap.builder();
            Version minNodeVersion = null;
            Version maxNodeVersion = null;
            Version minNonClientNodeVersion = null;
            Version maxNonClientNodeVersion = null;
            for (ObjectObjectCursor<String, DiscoveryNode> nodeEntry : nodes) {
                if (nodeEntry.value.isDataNode()) {
                    dataNodesBuilder.put(nodeEntry.key, nodeEntry.value);
                }
                if (nodeEntry.value.isMasterNode()) {
                    masterNodesBuilder.put(nodeEntry.key, nodeEntry.value);
                }
                final Version version = nodeEntry.value.getVersion();
                if (nodeEntry.value.isDataNode() || nodeEntry.value.isMasterNode()) {
                    if (minNonClientNodeVersion == null) {
                        minNonClientNodeVersion = version;
                        maxNonClientNodeVersion = version;
                    } else {
                        minNonClientNodeVersion = Version.min(minNonClientNodeVersion, version);
                        maxNonClientNodeVersion = Version.max(maxNonClientNodeVersion, version);
                    }
                }
                if (nodeEntry.value.isIngestNode()) {
                    ingestNodesBuilder.put(nodeEntry.key, nodeEntry.value);
                }
                minNodeVersion = minNodeVersion == null ? version : Version.min(minNodeVersion, version);
                maxNodeVersion = maxNodeVersion == null ? version : Version.max(maxNodeVersion, version);
            }

            return new DiscoveryNodes(
                nodes.build(), dataNodesBuilder.build(), masterNodesBuilder.build(), ingestNodesBuilder.build(),
                masterNodeId, localNodeId, minNonClientNodeVersion == null ? Version.CURRENT : minNonClientNodeVersion,
                maxNonClientNodeVersion == null ? Version.CURRENT : maxNonClientNodeVersion,
                maxNodeVersion == null ? Version.CURRENT : maxNodeVersion,
                minNodeVersion == null ? Version.CURRENT : minNodeVersion
            );
        }

        public boolean isLocalNodeElectedMaster() {
            return masterNodeId != null && masterNodeId.equals(localNodeId);
        }
    }
}
