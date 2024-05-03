/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

public interface ShardRoutingRoleStrategy {

    /**
     * @return the role for a copy of a new empty shard, where {@code copyIndex} is the index of the copy ({@code 0} for the primary and
     * {@code 1..N} for replicas).
     */
    ShardRouting.Role newEmptyRole(int copyIndex);

    /**
     * @return the role for a new replica copy of an existing shard.
     */
    ShardRouting.Role newReplicaRole();

    /**
     * @return the role for a copy of a new shard being restored from snapshot, where {@code copyIndex} is the index of the copy ({@code 0}
     * for the primary and {@code 1..N} for replicas).
     */
    default ShardRouting.Role newRestoredRole(int copyIndex) {
        return newEmptyRole(copyIndex);
    }

    /**
     * A strategy that refuses to create any new shard copies, which is used (for instance) when reading shard copies from a remote node.
     */
    ShardRoutingRoleStrategy NO_SHARD_CREATION = new ShardRoutingRoleStrategy() {
        @Override
        public ShardRouting.Role newEmptyRole(int copyIndex) {
            return newReplicaRole();
        }

        @Override
        public ShardRouting.Role newReplicaRole() {
            assert false : "no shard creation permitted";
            throw new IllegalStateException("no shard creation permitted");
        }
    };
}
