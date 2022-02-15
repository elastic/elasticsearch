/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

public final class Explanations {

    public static final class Allocation {

        public static final String YES = """
            Elasticsearch can allocate the shard.""";

        public static final String THROTTLED = """
            Elasticsearch is currently busy with other activities, but expects to be able to allocate this shard when those activities \
            finish. Please wait.""";

        public static final String AWAITING_INFO = """
            Elasticsearch is currently retrieving information about this shard from one or more nodes, and will make an allocation \
            decision once it receives the missing information. Please wait.""";

        public static final String ALL_COPIES_INVALID = """
            Elasticsearch cannot allocate this shard because all the copies it found in the cluster are stale or corrupt. Elasticsearch \
            will allocate this shard when a node holding a good copy of its data joins the cluster. If no such node is available then you \
            should restore this index from a recent snapshot.""";

        public static final String NO_COPIES = """
            Elasticsearch cannot allocate this shard because it could not find any copies of its data in the cluster. Elasticsearch \
            will allocate this shard when a node holding a good copy of its data joins the cluster. If no such node is available then you \
            should restore this index from a recent snapshot.""";

        public static final String DELAYED_WITH_ALTERNATIVE = """
            The node holding this shard copy left the cluster and Elasticsearch is waiting for it to return. If the node does not return \
            within [%s] then Elasticsearch will be able to allocate this shard to another node in the cluster.""";

        public static final String DELAYED_WITHOUT_ALTERNATIVE = """
            The node holding this shard copy left the cluster and Elasticsearch is waiting for it to return. If the node does not return \
            within [%s] then Elasticsearch will not be able to allocate this shard to another node in the cluster.""";

        public static final String EXISTING_STORES_FORBIDDEN = """
            Elasticsearch is not permitted to allocate this shard to any of the nodes in the cluster that hold an in-sync copy of its \
            data. If you expect this shard to be allocated to one of the nodes in the cluster then you should find this node in the \
            node-by-node explanation and address the reasons which prevent Elasticsearch from allocating this shard there.""";

        public static final String ALL_NODES_FORBIDDEN = """
            Elasticsearch is not permitted to allocate this shard to any of the nodes in the cluster. If you expect this shard to be \
            allocated to one of the nodes in the cluster then you should find this node in the node-by-node explanation and address the \
            reasons which prevent Elasticsearch from allocating this shard there.""";

    }

    public static final class Rebalance {

        public static final String YES = """
            Elasticsearch can rebalance this shard onto a different node.""";

        public static final String ALREADY_BALANCED = """
            Elasticsearch cannot rebalance this shard onto a different node since there is no node to which allocation is permitted which \
            would improve the cluster balance. If you expect this shard to be rebalanced onto one of the nodes in the cluster then you \
            should find this node in the node-by-node explanation and address the reasons which prevent Elasticsearch from rebalancing \
            this shard there.""";

        public static final String AWAITING_INFO = """
            Elasticsearch is currently retrieving information about the shard from one or more nodes, and will make a rebalancing decision \
            once it receives the missing information. Please wait.""";

        public static final String CLUSTER_THROTTLE = """
            Elasticsearch is busy with other activities, but expects to be able to rebalance this shard when those activities finish. \
            Please wait.""";

        public static final String NODE_THROTTLE = """
            Elasticsearch expects to be able to rebalance this shard onto another node, but the target node is currently busy with other \
            activities. Please wait.""";

        public static final String CANNOT_REBALANCE_CAN_ALLOCATE = """
            Elasticsearch is not permitted to rebalance this shard onto any of the nodes in the cluster, even though it is permitted to \
            allocate this shard onto one of those nodes. If you expect this shard to be rebalanced onto one of the nodes in the cluster \
            then you should find this node in the node-by-node explanation and address the reasons which prevent Elasticsearch from \
            rebalancing this shard there.""";

        public static final String CANNOT_REBALANCE_CANNOT_ALLOCATE = """
            Elasticsearch is not permitted to allocate or rebalance this shard onto any of the nodes in the cluster. If you expect this \
            shard to be rebalanced onto one of the nodes in the cluster then you should find this node in the node-by-node explanation and \
            address the reasons which prevent Elasticsearch from rebalancing this shard there.""";
    }

    public static final class Move {

        public static final String YES = """
            This shard cannot remain on its current node but Elasticsearch can move it onto a different node.""";

        public static final String THROTTLED = """
            This shard may not remain on its current node. Elasticsearch expects to be able to move it onto another node, but the target \
            node is currently busy with other activities. Please wait.""";

        public static final String NO = """
            This shard may not remain on its current node, but Elasticsearch is also not permitted to move it onto another node in the \
            cluster. If you expect this shard to be allocated to one of the nodes in the cluster then you should find this node in the \
            node-by-node explanation and address the reasons which prevent Elasticsearch from allocating this shard there.""";
    }

}
