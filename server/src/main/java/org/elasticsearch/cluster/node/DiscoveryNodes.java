/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 */
public class DiscoveryNodes extends AbstractCollection<DiscoveryNode> implements SimpleDiffable<DiscoveryNodes> {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();

    private final ImmutableOpenMap<String, DiscoveryNode> nodes;
    private final ImmutableOpenMap<String, DiscoveryNode> dataNodes;
    private final ImmutableOpenMap<String, DiscoveryNode> masterNodes;
    private final ImmutableOpenMap<String, DiscoveryNode> ingestNodes;

    @Nullable
    private final String masterNodeId;
    @Nullable
    private final DiscoveryNode masterNode;
    @Nullable
    private final String localNodeId;
    @Nullable
    private final DiscoveryNode localNode;
    private final Version minNonClientNodeVersion;
    private final Version maxNodeVersion;
    private final Version minNodeVersion;

    private DiscoveryNodes(
        ImmutableOpenMap<String, DiscoveryNode> nodes,
        ImmutableOpenMap<String, DiscoveryNode> dataNodes,
        ImmutableOpenMap<String, DiscoveryNode> masterNodes,
        ImmutableOpenMap<String, DiscoveryNode> ingestNodes,
        @Nullable String masterNodeId,
        @Nullable String localNodeId,
        Version minNonClientNodeVersion,
        Version maxNodeVersion,
        Version minNodeVersion
    ) {
        this.nodes = nodes;
        this.dataNodes = dataNodes;
        this.masterNodes = masterNodes;
        this.ingestNodes = ingestNodes;
        this.masterNodeId = masterNodeId;
        this.masterNode = masterNodeId == null ? null : nodes.get(masterNodeId);
        assert (masterNodeId == null) == (masterNode == null);
        this.localNodeId = localNodeId;
        this.localNode = localNodeId == null ? null : nodes.get(localNodeId);
        this.minNonClientNodeVersion = minNonClientNodeVersion;
        this.minNodeVersion = minNodeVersion;
        this.maxNodeVersion = maxNodeVersion;
        assert (localNodeId == null) == (localNode == null);
    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.values().iterator();
    }

    @Override
    public int size() {
        return nodes.size();
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
        nodes.putAllFromMap(masterNodes);
        return nodes.build();
    }

    /**
     * Get a {@link Map} of the coordinating only nodes (nodes which are neither master, nor data, nor ingest nodes) arranged by their ids
     *
     * @return {@link Map} of the coordinating only nodes arranged by their ids
     */
    public ImmutableOpenMap<String, DiscoveryNode> getCoordinatingOnlyNodes() {
        ImmutableOpenMap.Builder<String, DiscoveryNode> nodes = ImmutableOpenMap.builder(this.nodes);
        nodes.removeAllFromCollection(masterNodes.keySet());
        nodes.removeAllFromCollection(dataNodes.keySet());
        nodes.removeAllFromCollection(ingestNodes.keySet());
        return nodes.build();
    }

    /**
     * Returns a stream of all nodes, with master nodes at the front
     */
    public Stream<DiscoveryNode> mastersFirstStream() {
        return Stream.concat(masterNodes.values().stream(), stream().filter(n -> n.isMasterNode() == false));
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
     * Determine if the given node exists and has the right roles. Supported roles vary by version, and our local cluster state might
     * have come via an older master, so the roles may differ even if the node is otherwise identical.
     */
    public boolean nodeExistsWithSameRoles(DiscoveryNode discoveryNode) {
        final DiscoveryNode existing = nodes.get(discoveryNode.getId());
        return existing != null && existing.equals(discoveryNode) && existing.getRoles().equals(discoveryNode.getRoles());
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
        return localNode;
    }

    /**
     * Returns the master node, or {@code null} if there is no master node
     */
    @Nullable
    public DiscoveryNode getMasterNode() {
        return masterNode;
    }

    /**
     * Get a node by its address
     *
     * @param address {@link TransportAddress} of the wanted node
     * @return node identified by the given address or <code>null</code> if no such node exists
     */
    public DiscoveryNode findByAddress(TransportAddress address) {
        for (DiscoveryNode node : nodes.values()) {
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
            throw new IllegalArgumentException(
                "resolved [" + node + "] into [" + resolvedNodeIds.length + "] nodes, where expected to be resolved to a single node"
            );
        }
        if (resolvedNodeIds.length == 0) {
            throw new IllegalArgumentException("failed to resolve [" + node + "], no matching nodes");
        }
        return nodes.get(resolvedNodeIds[0]);
    }

    /**
     * Resolves a set of nodes according to the given sequence of node specifications. Implements the logic in various APIs that allow the
     * user to run the action on a subset of the nodes in the cluster. See [Node specification] in the reference manual for full details.
     *
     * Works by tracking the current set of nodes and applying each node specification in sequence. The set starts out empty and each node
     * specification may either add or remove nodes. For instance:
     *
     * - _local, _master and _all respectively add to the subset the local node, the currently-elected master, and all the nodes
     * - node IDs, names, hostnames and IP addresses all add to the subset any nodes which match
     * - a wildcard-based pattern of the form "attr*:value*" adds to the subset all nodes with a matching attribute with a matching value
     * - role:true adds to the subset all nodes with a matching role
     * - role:false removes from the subset all nodes with a matching role.
     *
     * An empty sequence of node specifications returns all nodes, since the corresponding actions run on all nodes by default.
     */
    public String[] resolveNodes(String... nodes) {
        if (nodes == null || nodes.length == 0) {
            return stream().map(DiscoveryNode::getId).toArray(String[]::new);
        } else {
            Set<String> resolvedNodesIds = new HashSet<>(nodes.length);
            for (String nodeId : nodes) {
                if (nodeId == null) {
                    // don't silence the underlying issue, it is a bug, so lets fail if assertions are enabled
                    assert nodeId != null : "nodeId should not be null";
                    continue;
                } else if (nodeId.equals("_local")) {
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
                        if (DiscoveryNodeRole.roles().stream().map(DiscoveryNodeRole::roleName).anyMatch(s -> s.equals(matchAttrName))) {
                            final DiscoveryNodeRole role = DiscoveryNodeRole.getRoleFromRoleName(matchAttrName);
                            final Predicate<Set<DiscoveryNodeRole>> predicate;
                            if (role.equals(DiscoveryNodeRole.DATA_ROLE)) {
                                // if the node has *any* role that can contain data, then it matches the data attribute
                                predicate = s -> s.stream().anyMatch(DiscoveryNodeRole::canContainData);
                            } else if (role.canContainData()) {
                                // if the node has the matching data_ role, or the generic data role, then it matches the data_ attribute
                                predicate = s -> s.stream().anyMatch(r -> r.equals(role) || r.equals(DiscoveryNodeRole.DATA_ROLE));
                            } else {
                                // the role is not a data role, we require an exact match (e.g., ingest)
                                predicate = s -> s.contains(role);
                            }
                            final Consumer<String> mutation;
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                mutation = resolvedNodesIds::add;
                            } else {
                                mutation = resolvedNodesIds::remove;
                            }
                            for (final DiscoveryNode node : this) {
                                if (predicate.test(node.getRoles())) {
                                    mutation.accept(node.getId());
                                }
                            }
                        } else if (DiscoveryNode.COORDINATING_ONLY.equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(getCoordinatingOnlyNodes().keySet());
                            } else {
                                resolvedNodesIds.removeAll(getCoordinatingOnlyNodes().keySet());
                            }
                        } else {
                            for (DiscoveryNode node : this) {
                                for (DiscoveryNodeRole role : Sets.difference(node.getRoles(), DiscoveryNodeRole.roles())) {
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
            return resolvedNodesIds.toArray(Strings.EMPTY_ARRAY);
        }
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

        return new Delta(
            other.getMasterNode(),
            getMasterNode(),
            localNodeId,
            Collections.unmodifiableList(removed),
            Collections.unmodifiableList(added)
        );
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
        @Nullable
        private final DiscoveryNode previousMasterNode;
        @Nullable
        private final DiscoveryNode newMasterNode;
        private final List<DiscoveryNode> removed;
        private final List<DiscoveryNode> added;

        private Delta(
            @Nullable DiscoveryNode previousMasterNode,
            @Nullable DiscoveryNode newMasterNode,
            String localNodeId,
            List<DiscoveryNode> removed,
            List<DiscoveryNode> added
        ) {
            this.previousMasterNode = previousMasterNode;
            this.newMasterNode = newMasterNode;
            this.localNodeId = localNodeId;
            this.removed = removed;
            this.added = added;
        }

        public boolean hasChanges() {
            return masterNodeChanged() || removed.isEmpty() == false || added.isEmpty() == false;
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
            return removed.isEmpty() == false;
        }

        public List<DiscoveryNode> removedNodes() {
            return removed;
        }

        public boolean added() {
            return added.isEmpty() == false;
        }

        public List<DiscoveryNode> addedNodes() {
            return added;
        }

        public String shortSummary() {
            final StringBuilder summary = new StringBuilder();
            if (masterNodeChanged()) {
                summary.append("master node changed {previous [");
                if (previousMasterNode != null) {
                    previousMasterNode.appendDescriptionWithoutAttributes(summary);
                }
                summary.append("], current [");
                if (newMasterNode != null) {
                    newMasterNode.appendDescriptionWithoutAttributes(summary);
                }
                summary.append("]}");
            }
            if (removed()) {
                if (summary.length() > 0) {
                    summary.append(", ");
                }
                summary.append("removed {");
                addCommaSeparatedNodesWithoutAttributes(removedNodes().iterator(), summary);
                summary.append('}');
            }
            if (added()) {
                // ignore ourselves when reporting on nodes being added
                final Iterator<DiscoveryNode> addedNodesIterator = addedNodes().stream()
                    .filter(node -> node.getId().equals(localNodeId) == false)
                    .iterator();

                if (addedNodesIterator.hasNext()) {
                    if (summary.length() > 0) {
                        summary.append(", ");
                    }
                    summary.append("added {");
                    addCommaSeparatedNodesWithoutAttributes(addedNodesIterator, summary);
                    summary.append('}');
                }
            }
            return summary.toString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(masterNodeId);
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
            assert builder.validateAdd(node) == null
                : "building disco nodes from network doesn't pass preflight: " + builder.validateAdd(node);
            builder.putUnsafe(node);
        }
        return builder.build();
    }

    public static Diff<DiscoveryNodes> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return SimpleDiffable.readDiffFrom(in1 -> readFrom(in1, localNode), in);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(DiscoveryNodes nodes) {
        return new Builder(nodes);
    }

    public static class Builder {

        private final Map<String, DiscoveryNode> nodes;
        private String masterNodeId;
        private String localNodeId;

        public Builder() {
            nodes = new HashMap<>();
        }

        public Builder(DiscoveryNodes nodes) {
            this.masterNodeId = nodes.getMasterNodeId();
            this.localNodeId = nodes.getLocalNodeId();
            this.nodes = new HashMap<>(nodes.getNodes());
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
        @Nullable
        public DiscoveryNode get(String nodeId) {
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
            for (DiscoveryNode existingNode : nodes.values()) {
                if (node.getAddress().equals(existingNode.getAddress()) && node.getId().equals(existingNode.getId()) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode + " with same address";
                }
                if (node.getId().equals(existingNode.getId()) && node.equals(existingNode) == false) {
                    return "can't add node "
                        + node
                        + ", found existing node "
                        + existingNode
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
            for (Map.Entry<String, DiscoveryNode> nodeEntry : nodes.entrySet()) {
                String nodeId = nodeEntry.getKey();
                DiscoveryNode discoNode = nodeEntry.getValue();
                if (discoNode.canContainData()) {
                    dataNodesBuilder.put(nodeId, discoNode);
                }
                if (discoNode.isMasterNode()) {
                    masterNodesBuilder.put(nodeId, discoNode);
                }
                final Version version = discoNode.getVersion();
                if (discoNode.canContainData() || discoNode.isMasterNode()) {
                    if (minNonClientNodeVersion == null) {
                        minNonClientNodeVersion = version;
                        maxNonClientNodeVersion = version;
                    } else {
                        minNonClientNodeVersion = Version.min(minNonClientNodeVersion, version);
                        maxNonClientNodeVersion = Version.max(maxNonClientNodeVersion, version);
                    }
                }
                if (discoNode.isIngestNode()) {
                    ingestNodesBuilder.put(nodeId, discoNode);
                }
                minNodeVersion = minNodeVersion == null ? version : Version.min(minNodeVersion, version);
                maxNodeVersion = maxNodeVersion == null ? version : Version.max(maxNodeVersion, version);
            }

            return new DiscoveryNodes(
                ImmutableOpenMap.<String, DiscoveryNode>builder(nodes.size()).putAllFromMap(nodes).build(),
                dataNodesBuilder.build(),
                masterNodesBuilder.build(),
                ingestNodesBuilder.build(),
                masterNodeId,
                localNodeId,
                minNonClientNodeVersion == null ? Version.CURRENT : minNonClientNodeVersion,
                maxNodeVersion == null ? Version.CURRENT : maxNodeVersion,
                minNodeVersion == null ? Version.CURRENT : minNodeVersion
            );
        }

        public boolean isLocalNodeElectedMaster() {
            return masterNodeId != null && masterNodeId.equals(localNodeId);
        }
    }

    public static void addCommaSeparatedNodesWithoutAttributes(Iterator<DiscoveryNode> iterator, StringBuilder stringBuilder) {
        while (iterator.hasNext()) {
            iterator.next().appendDescriptionWithoutAttributes(stringBuilder);
            if (iterator.hasNext()) {
                stringBuilder.append(", ");
            }
        }
    }
}
