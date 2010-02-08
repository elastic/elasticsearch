/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.util.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class Nodes implements Iterable<Node> {

    public static Nodes EMPTY_NODES = newNodesBuilder().build();

    private final ImmutableMap<String, Node> nodes;

    private final ImmutableMap<String, Node> dataNodes;

    private final String masterNodeId;

    private final String localNodeId;

    private Nodes(ImmutableMap<String, Node> nodes, ImmutableMap<String, Node> dataNodes, String masterNodeId, String localNodeId) {
        this.nodes = nodes;
        this.dataNodes = dataNodes;
        this.masterNodeId = masterNodeId;
        this.localNodeId = localNodeId;
    }

    @Override public UnmodifiableIterator<Node> iterator() {
        return nodes.values().iterator();
    }

    /**
     * Returns <tt>true</tt> if the local node is the master node.
     */
    public boolean localNodeMaster() {
        return localNodeId.equals(masterNodeId);
    }

    public int size() {
        return nodes.size();
    }

    public ImmutableMap<String, Node> nodes() {
        return this.nodes;
    }

    public ImmutableMap<String, Node> dataNodes() {
        return this.dataNodes;
    }

    public Node get(String nodeId) {
        return nodes.get(nodeId);
    }

    public boolean nodeExists(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    public String masterNodeId() {
        return this.masterNodeId;
    }

    public String localNodeId() {
        return this.localNodeId;
    }

    public Node localNode() {
        return nodes.get(localNodeId);
    }

    public Node masterNode() {
        return nodes.get(masterNodeId);
    }

    public Nodes removeDeadMembers(Set<String> newNodes, String masterNodeId) {
        Builder builder = new Builder().masterNodeId(masterNodeId).localNodeId(localNodeId);
        for (Node node : this) {
            if (newNodes.contains(node.id())) {
                builder.put(node);
            }
        }
        return builder.build();
    }

    public Nodes newNode(Node node) {
        return new Builder().putAll(this).put(node).build();
    }

    /**
     * Returns the changes comparing this nodes to the provided nodes.
     */
    public Delta delta(Nodes other) {
        List<Node> removed = newArrayList();
        List<Node> added = newArrayList();
        for (Node node : other) {
            if (!this.nodeExists(node.id())) {
                removed.add(node);
            }
        }
        for (Node node : this) {
            if (!other.nodeExists(node.id())) {
                added.add(node);
            }
        }
        Node previousMasterNode = null;
        Node newMasterNode = null;
        if (masterNodeId != null) {
            if (other.masterNodeId == null || !other.masterNodeId.equals(masterNodeId)) {
                previousMasterNode = other.masterNode();
                newMasterNode = masterNode();
            }
        }
        return new Delta(previousMasterNode, newMasterNode, localNodeId, ImmutableList.copyOf(removed), ImmutableList.copyOf(added));
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("Nodes: \n");
        for (Node node : this) {
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
        return new Delta(null, null, localNodeId, Node.EMPTY_LIST, Node.EMPTY_LIST);
    }

    public static class Delta {

        private final String localNodeId;
        private final Node previousMasterNode;
        private final Node newMasterNode;
        private final ImmutableList<Node> removed;
        private final ImmutableList<Node> added;


        public Delta(String localNodeId, ImmutableList<Node> removed, ImmutableList<Node> added) {
            this(null, null, localNodeId, removed, added);
        }

        public Delta(@Nullable Node previousMasterNode, @Nullable Node newMasterNode, String localNodeId, ImmutableList<Node> removed, ImmutableList<Node> added) {
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

        public Node previousMasterNode() {
            return previousMasterNode;
        }

        public Node newMasterNode() {
            return newMasterNode;
        }

        public boolean removed() {
            return !removed.isEmpty();
        }

        public ImmutableList<Node> removedNodes() {
            return removed;
        }

        public boolean added() {
            return !added.isEmpty();
        }

        public ImmutableList<Node> addedNodes() {
            return added;
        }

        public String shortSummary() {
            StringBuilder sb = new StringBuilder();
            if (!removed() && masterNodeChanged()) {
                if (newMasterNode.id().equals(localNodeId)) {
                    // we are the master, no nodes we removed, we are actually the first master
                    sb.append("New Master ").append(newMasterNode());
                } else {
                    // we are not the master, so we just got this event. No nodes were removed, so its not a *new* master
                    sb.append("Detected Master ").append(newMasterNode());
                }
            } else {
                if (masterNodeChanged()) {
                    sb.append("Master {New ").append(newMasterNode());
                    if (previousMasterNode() != null) {
                        sb.append(", Previous ").append(previousMasterNode());
                    }
                    sb.append("}");
                }
                if (removed()) {
                    if (masterNodeChanged()) {
                        sb.append(", ");
                    }
                    sb.append("Removed {");
                    for (Node node : removedNodes()) {
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
                    sb.append("Added {");
                    for (Node node : addedNodes()) {
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

    public static Builder newNodesBuilder() {
        return new Builder();
    }

    public static class Builder {

        private Map<String, Node> nodes = newHashMap();

        private String masterNodeId;

        private String localNodeId;

        public Builder putAll(Nodes nodes) {
            this.masterNodeId = nodes.masterNodeId();
            this.localNodeId = nodes.localNodeId();
            for (Node node : nodes) {
                put(node);
            }
            return this;
        }

        public Builder put(Node node) {
            nodes.put(node.id(), node);
            return this;
        }

        public Builder putAll(Iterable<Node> nodes) {
            for (Node node : nodes) {
                put(node);
            }
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

        public Nodes build() {
            ImmutableMap.Builder<String, Node> dataNodesBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Node> nodeEntry : nodes.entrySet()) {
                if (nodeEntry.getValue().dataNode()) {
                    dataNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
            }
            return new Nodes(ImmutableMap.copyOf(nodes), dataNodesBuilder.build(), masterNodeId, localNodeId);
        }

        public static void writeTo(Nodes nodes, DataOutput out) throws IOException {
            out.writeUTF(nodes.masterNodeId);
            out.writeInt(nodes.size());
            for (Node node : nodes) {
                node.writeTo(out);
            }
        }

        public static Nodes readFrom(DataInput in, @Nullable Node localNode) throws IOException, ClassNotFoundException {
            Builder builder = new Builder();
            builder.masterNodeId(in.readUTF());
            if (localNode != null) {
                builder.localNodeId(localNode.id());
            }
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Node node = Node.readNode(in);
                if (localNode != null && node.id().equals(localNode.id())) {
                    // reuse the same instance of our address and local node id for faster equality
                    node = localNode;
                }
                builder.put(node);
            }
            return builder.build();
        }
    }
}
