package org.elasticsearch.cluster.node;

import java.util.List;

public class NodeGroup {
    private final List<String> nodes;

    public static NodeGroup buildFromNodeIDs(List<String> nodes) {
        return new NodeGroup(nodes);
    }

    private NodeGroup(List<String> nodes) {
        this.nodes = nodes;
    }
    
    public List<String> toList() {
        return this.nodes;
    }

    public String nodeForShard(int shard) {
        if (this.nodes.size() == 0) {
            return null;
        }
        return this.nodes.get(shard % this.nodes.size());
    }

    public boolean matchShard(String nodeId, int shard) {
        String node  = nodeForShard(shard);
        if (node == null) {
            return false;
        }
        return node.equals(nodeId);
    }

    public boolean containsNode(String nodeId) {
        return this.nodes.contains(nodeId);
    }
}
