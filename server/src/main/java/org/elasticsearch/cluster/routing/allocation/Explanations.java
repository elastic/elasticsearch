/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

public final class Explanations {

    public static final class Allocation {

        public static final String YES = """
            Elasticsearch can allocate the shard.""";

        public static final String THROTTLED = """
            Elasticsearch is currently busy with other activities. It expects to be able to allocate this shard when those activities \
            finish. Please wait.""";

        public static final String AWAITING_INFO = """
            Elasticsearch is retrieving information about this shard from one or more nodes. It will make an allocation decision after it \
            receives this information. Please wait.""";

        public static final String ALL_COPIES_INVALID = """
            Elasticsearch can't allocate this shard because all the copies of its data in the cluster are stale or corrupt. Elasticsearch \
            will allocate this shard when a node containing a good copy of its data joins the cluster. If no such node is available, \
            restore this index from a recent snapshot.""";

        public static final String NO_COPIES = """
            Elasticsearch can't allocate this shard because there are no copies of its data in the cluster. Elasticsearch will allocate \
            this shard when a node holding a good copy of its data joins the cluster. If no such node is available, restore this index \
            from a recent snapshot. For more information, see\s""" + org.elasticsearch.common.ReferenceDocs.ALLOCATION_EXPLAIN_NO_COPIES;

        public static final String DELAYED_WITH_ALTERNATIVE = """
            The node containing this shard copy recently left the cluster. Elasticsearch is waiting for it to return. If the node does not \
            return within [%s] then Elasticsearch will allocate this shard to another node. Please wait.""";

        public static final String DELAYED_WITHOUT_ALTERNATIVE = """
            The node holding this shard copy recently left the cluster. Elasticsearch is waiting for it to return. If the node does not \
            return within [%s] then Elasticsearch will attempt to allocate this shard to another node, but it cannot be allocated to any \
            other node currently in the cluster. If you expect this shard to be allocated to another node, find this node in the \
            node-by-node explanation and address the reasons which prevent Elasticsearch from allocating this shard there.""";

        public static final String EXISTING_STORES_FORBIDDEN = """
            Elasticsearch isn't allowed to allocate this shard to any of the nodes in the cluster that hold an in-sync copy of its data. \
            Choose a node to which you expect this shard to be allocated, find this node in the node-by-node explanation, and address the \
            reasons which prevent Elasticsearch from allocating this shard there.""";

        public static final String ALL_NODES_FORBIDDEN = """
            Elasticsearch isn't allowed to allocate this shard to any of the nodes in the cluster. Choose a node to which you expect this \
            shard to be allocated, find this node in the node-by-node explanation, and address the reasons which prevent Elasticsearch \
            from allocating this shard there.""";

    }

    public static final class Rebalance {

        public static final String YES = """
            Elasticsearch can rebalance this shard to another node.""";

        public static final String ALREADY_BALANCED = """
            This shard is in a well-balanced location and satisfies all allocation rules so it will remain on this node. Elasticsearch \
            cannot improve the cluster balance by moving it to another node. If you expect this shard to be rebalanced to another node, \
            find the other node in the node-by-node explanation and address the reasons which prevent Elasticsearch from rebalancing this \
            shard there.""";

        public static final String AWAITING_INFO = """
            Elasticsearch is currently retrieving information about this shard from one or more nodes. It will make a rebalancing decision \
            after it receives this information. Please wait.""";

        public static final String CLUSTER_THROTTLE = """
            Elasticsearch is currently busy with other activities. It will rebalance this shard when those activities finish. Please \
            wait.""";

        public static final String NODE_THROTTLE = """
            Elasticsearch is attempting to rebalance this shard to another node, but the nodes involved are currently busy with other \
            activities. The shard will be rebalanced when those activities finish. Please wait.""";

        public static final String CANNOT_REBALANCE_CAN_ALLOCATE = """
            Elasticsearch is allowed to allocate this shard on another node, and there is at least one node to which it could move this \
            shard that would improve the overall cluster balance, but it isn't allowed to rebalance this shard there. If you expect this \
            shard to be rebalanced to another node, check the cluster-wide rebalancing decisions and address any reasons preventing \
            Elasticsearch from rebalancing shards within the cluster, and then find the expected node in the node-by-node explanation and \
            address the reasons which prevent Elasticsearch from moving this shard there.""";

        public static final String CANNOT_REBALANCE_CANNOT_ALLOCATE = """
            Elasticsearch is not allowed to allocate or rebalance this shard to another node. If you expect this shard to be rebalanced to \
            another node, find this node in the node-by-node explanation and address the reasons which prevent Elasticsearch from \
            rebalancing this shard there.""";

    }

    public static final class Move {

        public static final String YES = """
            This shard may not remain on its current node. Elasticsearch will move it to another node.""";

        public static final String THROTTLED = """
            This shard may not remain on its current node. Elasticsearch is currently busy with other activities and will move this shard \
            to another node when those activities finish. Please wait.""";

        public static final String NO = """
            This shard may not remain on its current node, but Elasticsearch isn't allowed to move it to another node. Choose a node to \
            which you expect this shard to be allocated, find this node in the node-by-node explanation, and address the reasons which \
            prevent Elasticsearch from allocating this shard there.""";

    }

}
